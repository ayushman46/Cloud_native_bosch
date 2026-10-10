"""Mock end-to-end: Injector lifecycle events -> Observer -> Observation Report -> Analyzer -> API.
No Docker, no OpenMeter, no NVIDIA: a FakeOpenMeter transport and a scripted LLM stand in."""
import asyncio
import json
from datetime import datetime, timezone

import httpx
import pytest

from app.agents.analyzer import AnalyzerAgent
from app.agents.observer import ObserverAgent
from app.clients.nvidia_client import NvidiaTimeout
from app.config import Settings
from app.container import Container
from app.evaluation import metrics as ev
from app.main import create_app
from app.storage.repository import InMemoryRepository

from .fakes import FakeOpenMeter
from .helpers import FakeLLM, good_answer, ids_by_keyword
from .test_observer import event, fast_config, now

pytestmark = pytest.mark.anyio


def grounded_llm(user_prompt_unused=None):
    """A scripted 'model' that answers from the evidence ids in whatever prompt it is shown."""

    def respond(system: str, user: str) -> str:
        items = ids_by_keyword(user)
        lag = next(i for i, s in items.items() if s.startswith("processing_lag_ms") and "fault window" in s)
        health = next(i for i, s in items.items() if s.startswith("health-probe availability of sink-worker"))
        exp = json.loads(user.split("\n\nYour previous answer")[0])["experiment_id"]
        return good_answer(exp, [lag], [health])

    return respond


def make(fake, llm):
    repo = InMemoryRepository()
    observer = ObserverAgent(Settings(_env_file=None), repo, monitor_config=fast_config(), transport=fake.transport())
    container = Container(Settings(_env_file=None), repo, observer, AnalyzerAgent(llm))
    app = create_app(container)
    app.state.container = container  # lifespan is not run under ASGITransport
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://api.test")
    return container, client


async def run_restart_via_api(client, fake, exp="exp_003"):
    assert (await client.post("/api/monitoring/start", json={"experiment_id": exp})).status_code == 202
    await asyncio.sleep(0.8)
    m = (await client.get(f"/api/experiments/{exp}/metrics")).json()
    assert m["baseline_ready"] and m["current_phase"] == "baseline" and m["state"] == "running"

    t_act = now()
    fake.set_worker_down(True)
    ev_active = event_json(exp, fault_activated_at=t_act)
    assert (await client.post(f"/api/monitoring/{exp}/event", json=ev_active)).status_code == 200
    await asyncio.sleep(1.2)
    assert (await client.get(f"/api/experiments/{exp}/metrics")).json()["current_phase"] == "fault"
    fake.set_worker_down(False)
    t_rb = now()
    done = event_json(exp, fault_activated_at=t_act, rollback_started_at=t_rb, rollback_completed_at=t_rb)
    assert (await client.post(f"/api/monitoring/{exp}/event", json=done)).status_code == 200

    for _ in range(100):  # report appears by itself once recovery is confirmed
        r = await client.get(f"/api/experiments/{exp}/observations")
        if r.status_code == 200:
            return r.json()
        assert r.status_code == 409
        await asyncio.sleep(0.2)
    raise AssertionError("observation report never became ready")


def event_json(exp, **ts):
    e = event(__import__("app.schemas.injection", fromlist=["FailureType"]).FailureType.SERVICE_RESTART, **ts)
    e = e.model_copy(update={"experiment_id": exp})
    return json.loads(e.model_dump_json())


async def test_full_mock_experiment_through_the_api():
    fake = FakeOpenMeter()
    container, client = make(fake, FakeLLM(grounded_llm()))
    obs = await run_restart_via_api(client, fake)

    # Observation Report: contract blocks present with measured values
    assert obs["experiment_id"] == "exp_003"
    assert obs["during_failure"]["availability"] == 1.0
    assert obs["during_failure"]["processing_lag_ms"] > obs["baseline"]["processing_lag_ms"] * 3
    assert obs["recovery"]["functional_recovery_confirmed"] and obs["recovery"]["missing_events"] == 0
    assert obs["detection"]["detection_success"] is True
    assert "service_restart" not in json.dumps(obs)

    # Analyzer
    r = await client.post("/api/analysis", json={"experiment_id": "exp_003"})
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "completed"
    assert body["report"]["diagnosis"]["suspected_failure"] == "service_restart"
    assert body["report"]["impact"]["availability_affected"] is False
    assert body["report"]["impact"]["processing_delayed"] is True
    assert (await client.get("/api/analysis/exp_003")).json()["status"] == "completed"
    evaluation = (await client.get("/api/analysis/exp_003/evaluation")).json()
    assert evaluation["failure_correct"] and evaluation["component_correct"]
    await container.shutdown()
    await client.aclose()


async def test_nvidia_down_leaves_the_observation_intact_and_analysis_pending():
    fake = FakeOpenMeter()
    llm = FakeLLM(NvidiaTimeout("nvidia unreachable"))
    container, client = make(fake, llm)
    await run_restart_via_api(client, fake)
    r = await client.post("/api/analysis", json={"experiment_id": "exp_003"})
    assert r.status_code == 200 and r.json()["status"] == "unavailable" and r.json()["report"] is None
    assert (await client.get("/api/experiments/exp_003/observations")).status_code == 200  # unaffected
    assert (await client.get("/api/analysis/exp_003/evaluation")).status_code == 409  # nothing fabricated to score
    # NVIDIA comes back: the same observation can be analyzed later
    container.analyzer.llm = FakeLLM(grounded_llm())
    assert (await client.post("/api/analysis", json={"experiment_id": "exp_003"})).json()["status"] == "completed"
    await container.shutdown()
    await client.aclose()


async def test_api_error_paths():
    fake = FakeOpenMeter()
    container, client = make(fake, FakeLLM("x"))
    assert (await client.get("/api/experiments/nope/metrics")).status_code == 404
    assert (await client.get("/api/experiments/nope/observations")).status_code == 404
    assert (await client.post("/api/analysis", json={"experiment_id": "nope"})).status_code == 409
    assert (await client.post("/api/monitoring/nope/event", json=event_json("nope", fault_activated_at=now()))).status_code == 404
    await client.post("/api/monitoring/start", json={"experiment_id": "exp_x"})
    assert (await client.post("/api/monitoring/exp_x/event", json=event_json("other", fault_activated_at=now()))).status_code == 422
    assert (await client.get("/api/experiments/exp_x/observations")).status_code == 409  # still running
    assert (await client.post("/api/monitoring/exp_x/finalize")).status_code == 200
    await container.shutdown()
    await client.aclose()


async def test_observer_evaluation_metrics_over_a_control_and_a_fault_run():
    # healthy control
    fake = FakeOpenMeter()
    obs_h = ObserverAgent(Settings(_env_file=None), InMemoryRepository(), monitor_config=fast_config(), transport=fake.transport())
    mon_h = await obs_h.start_baseline("ctl")
    await asyncio.sleep(1.5)
    healthy_report = await obs_h.finalize("ctl")
    healthy_samples = mon_h.samples()
    await obs_h.shutdown()
    fpr = ev.false_positive_rate([(healthy_samples, healthy_report)], fast_config().metrics.sample_window_seconds)

    # faulted run
    fake2 = FakeOpenMeter()
    container, client = make(fake2, FakeLLM("x"))
    obs = await run_restart_via_api(client, fake2)
    report = container.repo.get_observation("exp_003")
    await container.shutdown()
    await client.aclose()

    assert fpr.denominator > 0 and fpr.numerator == 0, f"false positives on healthy control: {fpr}"
    assert str(ev.failure_detection_rate([report])) == "1/1 = 1.00"
    mttd, detected, missed = ev.mean_time_to_detect([report])
    assert detected == 1 and missed == 0 and mttd is not None and mttd >= 0
    assert ev.mean_monitoring_coverage([report]) >= 0.8
    assert ev.recovery_detection_accuracy([(report, True)]).value == 1.0
