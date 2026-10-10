"""Observer lifecycle tests against a FakeOpenMeter (mock transport, real asyncio timing, no Docker).

Fault shapes are simulated by what the fake does, NOT by anything in the Observer: the Observer has no
fault-type logic, so all four supported fault types share one code path.
"""
import asyncio
from datetime import datetime, timezone

import pytest

from app.agents.observer import ObserverAgent
from app.config import Settings
from app.monitoring.collector import MonitorConfig
from app.monitoring.detection import DetectionConfig
from app.monitoring.metrics import MetricsConfig
from app.schemas.injection import FailureType, InjectionEvent, InjectionTimestamps
from app.storage.repository import InMemoryRepository

from .fakes import FakeOpenMeter

pytestmark = pytest.mark.anyio


def now() -> datetime:
    return datetime.now(timezone.utc)


def fast_config() -> MonitorConfig:
    return MonitorConfig(
        health_targets={"openmeter-api": "http://om.test/api/v1/debug/metrics", "sink-worker": "http://sink.test/healthz"},
        health_interval_s=0.04, ingest_interval_s=0.03, meter_poll_interval_s=0.04,
        post_recovery_grace_s=0.3, recovery_deadline_s=6, drain_deadline_s=2,
        min_baseline_seconds=0.5, min_baseline_requests=8, recovery_confirm_probes=3,
        metrics=MetricsConfig(min_latency_samples=3, min_lag_samples=2, sample_window_seconds=0.25),
        detection=DetectionConfig(latency_floor_ms=150, lag_floor_ms=400, min_requests=3, min_health_probes=2,
                                  availability_threshold=0.8),
    )


def event(failure: FailureType, **ts) -> InjectionEvent:
    return InjectionEvent(experiment_id="exp_t", target_service="sink-worker", failure_type=failure,
                          status="active", injection_success=True, timestamps=InjectionTimestamps(**ts),
                          parameters={"observation_seconds": 1})


async def run_experiment(fake, failure=FailureType.SERVICE_RESTART, inject=lambda: None, rollback=lambda: None,
                         baseline_s=0.8, fault_s=1.0, repo=None):
    repo = repo or InMemoryRepository()
    agent = ObserverAgent(Settings(_env_file=None), repo, monitor_config=fast_config(), transport=fake.transport())
    mon = await agent.start_baseline("exp_t")
    await asyncio.sleep(baseline_s)
    assert mon.baseline_ready()
    t_act = now()
    inject()
    agent.handle_event(event(failure, fault_activated_at=t_act, dispatched_at=t_act))
    await asyncio.sleep(fault_s)
    rollback()
    t_rb = now()
    agent.handle_event(event(failure, fault_activated_at=t_act, rollback_started_at=t_rb, rollback_completed_at=t_rb))
    report = await mon.wait_finalized(timeout=20)
    await agent.shutdown()
    assert report is not None, "monitor did not auto-finalize"
    return report, repo


async def test_healthy_control_has_no_detection_and_perfect_accounting():
    fake = FakeOpenMeter()
    agent = ObserverAgent(Settings(_env_file=None), InMemoryRepository(), monitor_config=fast_config(),
                          transport=fake.transport())
    mon = await agent.start_baseline("exp_t")
    await asyncio.sleep(1.5)
    report = await agent.finalize("exp_t")
    await agent.shutdown()
    assert report.detection.anomalies == [] and report.detection.detection_success is None
    assert report.baseline.availability == 1.0 and report.baseline.error_rate == 0.0
    assert report.during_failure is None
    assert report.recovery.missing_events == 0 and report.recovery.duplicate_events == 0
    assert report.recovery.metering_accuracy == 1.0
    assert report.recovery.functional_recovery_confirmed is False  # nothing was recovered from
    assert any("No fault was activated" in n for n in report.observer_notes)


async def test_application_crash_detected_and_recovery_confirmed():
    fake = FakeOpenMeter()
    report, _ = await run_experiment(fake, FailureType.APPLICATION_CRASH,
                                     inject=lambda: setattr(fake, "api_down", True),
                                     rollback=lambda: setattr(fake, "api_down", False))
    d = report.during_failure
    assert d.availability < 0.2 and d.error_rate > 0.8
    assert d.p95_latency_ms is None  # fast refusals carry no latency signal -> null, not 0
    assert report.detection.detection_success is True and report.detection.detection_latency_ms < 1000
    assert report.recovery.functional_recovery_confirmed and report.recovery.functional_recovery_seconds < 3
    assert report.recovery.missing_events == 0 and report.recovery.metering_accuracy == 1.0


async def test_network_latency_raises_p95_and_processing_stays_correct():
    fake = FakeOpenMeter()
    report, _ = await run_experiment(fake, FailureType.NETWORK_LATENCY,
                                     inject=lambda: setattr(fake, "ingest_latency_s", 0.4),
                                     rollback=lambda: setattr(fake, "ingest_latency_s", 0.0))
    assert report.baseline.p95_latency_ms < 150
    assert report.during_failure.p95_latency_ms >= 350
    assert report.detection.detection_success is True
    assert report.detection.first_anomaly in ("HIGH_P95_LATENCY", "HIGH_PROCESSING_LAG")
    assert report.recovery.functional_recovery_confirmed
    assert report.recovery.missing_events == 0


async def test_service_restart_shows_lag_while_ingestion_stays_available():
    fake = FakeOpenMeter()
    report, _ = await run_experiment(fake, FailureType.SERVICE_RESTART,
                                     inject=lambda: fake.set_worker_down(True),
                                     rollback=lambda: fake.set_worker_down(False), fault_s=1.2)
    d = report.during_failure
    assert d.availability == 1.0 and d.error_rate == 0.0  # API stayed up
    assert d.processing_lag_ms >= 800 > (report.baseline.processing_lag_ms or 0) * 3
    assert d.service_availability["sink-worker"] < 0.2  # worker health probe saw the outage
    assert report.detection.detection_success is True
    assert report.recovery.functional_recovery_confirmed
    assert report.recovery.missing_events == 0  # backlog was eventually processed
    assert report.recovery.metering_accuracy == 1.0


async def test_traffic_spike_errors_are_recorded_without_fault_specific_code():
    fake = FakeOpenMeter()
    report, _ = await run_experiment(fake, FailureType.TRAFFIC_SPIKE,
                                     inject=lambda: setattr(fake, "api_status", 429),
                                     rollback=lambda: setattr(fake, "api_status", None))
    assert report.during_failure.error_rate > 0.8
    assert report.detection.detection_success is True
    assert report.recovery.functional_recovery_confirmed


async def test_data_loss_is_reported_as_missing_events_after_deadline():
    fake = FakeOpenMeter()
    report, _ = await run_experiment(fake, FailureType.SERVICE_RESTART,
                                     inject=lambda: setattr(fake, "drop_processing", True),
                                     rollback=lambda: setattr(fake, "drop_processing", False), fault_s=0.6)
    assert report.recovery.missing_events > 0
    assert report.recovery.metering_accuracy < 1.0


async def test_duplicate_processing_is_reported():
    fake = FakeOpenMeter()
    fake.double_count = True
    agent = ObserverAgent(Settings(_env_file=None), InMemoryRepository(), monitor_config=fast_config(), transport=fake.transport())
    await agent.start_baseline("exp_t")
    await asyncio.sleep(1.0)
    report = await agent.finalize("exp_t")
    await agent.shutdown()
    assert report.recovery.duplicate_events > 0
    assert report.recovery.metering_accuracy < 1.0


async def test_observer_survives_meter_outage_and_reports_unknown_not_zero():
    fake = FakeOpenMeter()
    fake.meter_down = True
    agent = ObserverAgent(Settings(_env_file=None), InMemoryRepository(), monitor_config=fast_config(), transport=fake.transport())
    await agent.start_baseline("exp_t")
    await asyncio.sleep(0.8)
    report = await agent.finalize("exp_t")
    await agent.shutdown()
    assert report.recovery.missing_events is None and report.recovery.metering_accuracy is None
    assert report.baseline.processing_lag_ms is None
    assert report.baseline.availability == 1.0  # HTTP monitoring unaffected
    assert any("Meter was never read" in n for n in report.observer_notes)


async def test_cancellation_stops_all_tasks_cleanly():
    fake = FakeOpenMeter()
    agent = ObserverAgent(Settings(_env_file=None), InMemoryRepository(), monitor_config=fast_config(), transport=fake.transport())
    mon = await agent.start_baseline("exp_t")
    await asyncio.sleep(0.3)
    await agent.shutdown()
    assert all(t.done() for t in mon._tasks)
    calls = fake.ingest_calls
    await asyncio.sleep(0.2)
    assert fake.ingest_calls == calls  # nothing keeps probing after cancellation
    assert len(mon.records) > 0  # data collected before cancellation is retained


async def test_future_timestamps_are_clamped_with_a_note():
    fake = FakeOpenMeter()
    agent = ObserverAgent(Settings(_env_file=None), InMemoryRepository(), monitor_config=fast_config(), transport=fake.transport())
    mon = await agent.start_baseline("exp_t")
    future = datetime(2099, 1, 1, tzinfo=timezone.utc)
    agent.handle_event(event(FailureType.SERVICE_RESTART, fault_activated_at=future))
    assert mon.lifecycle.fault_activated_at < future
    assert any("clock skew" in n for n in mon.notes)
    await agent.shutdown()


async def test_failed_injection_creates_no_fault_window():
    fake = FakeOpenMeter()
    agent = ObserverAgent(Settings(_env_file=None), InMemoryRepository(), monitor_config=fast_config(), transport=fake.transport())
    mon = await agent.start_baseline("exp_t")
    await asyncio.sleep(0.3)
    ev = InjectionEvent(experiment_id="exp_t", target_service="sink-worker", failure_type=FailureType.SERVICE_RESTART,
                        status="failed", injection_success=False,
                        timestamps=InjectionTimestamps(fault_activated_at=now()))
    agent.handle_event(ev)
    report = await agent.finalize("exp_t")
    await agent.shutdown()
    assert report.during_failure is None and report.lifecycle.fault_activated_at is None
    assert report.lifecycle.injection_success is False


async def test_observer_and_report_need_no_nvidia_credentials(monkeypatch):
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    fake = FakeOpenMeter()
    settings = Settings(_env_file=None)
    assert settings.nvidia_api_key is None
    agent = ObserverAgent(settings, InMemoryRepository(), monitor_config=fast_config(), transport=fake.transport())
    await agent.start_baseline("exp_t")
    await asyncio.sleep(0.4)
    report = await agent.finalize("exp_t")
    await agent.shutdown()
    assert report.experiment_id == "exp_t"


async def test_ground_truth_is_stored_apart_and_absent_from_report():
    fake = FakeOpenMeter()
    report, repo = await run_experiment(fake, FailureType.SERVICE_RESTART,
                                        inject=lambda: fake.set_worker_down(True),
                                        rollback=lambda: fake.set_worker_down(False), fault_s=0.5)
    gt = repo.get_ground_truth("exp_t")
    assert gt.failure_type is FailureType.SERVICE_RESTART and gt.target_service == "sink-worker"
    dumped = report.model_dump_json()
    assert "service_restart" not in dumped  # the injected fault label never enters the report
