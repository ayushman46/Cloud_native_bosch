"""Analyzer tests: all LLM traffic is mocked; no NVIDIA credits are consumed."""
import json

import httpx
import pytest

from app.agents.analyzer import SYSTEM_PROMPT, AnalyzerAgent, build_user_prompt, parse_llm_json
from app.agents.evidence import build_diagnostic_evidence
from app.clients.nvidia_client import (
    NvidiaChatClient,
    NvidiaNotConfigured,
    NvidiaRateLimited,
    NvidiaTimeout,
    NvidiaUnavailable,
)
from app.evaluation import metrics as ev
from app.schemas.analysis import AnalysisReport, AnalysisStatus, ExecutionGroundTruth
from app.schemas.injection import FailureType

from .helpers import (
    FakeLLM,
    crash_report,
    empty_report,
    good_answer,
    healthy_report,
    ids_by_keyword,
    restart_report,
)

pytestmark = pytest.mark.anyio


def evidence_ids(report):
    items = build_diagnostic_evidence(report).evidence_items
    lag = next(i.id for i in items if i.statement.startswith("processing_lag_ms: baseline") and "fault window" in i.statement)
    other = next(i.id for i in items if i.statement.startswith("health-probe availability of sink-worker"))
    return lag, other


def answer_for(report, **over):
    lag, other = evidence_ids(report)
    return good_answer(report.experiment_id, [lag], [other], **over)


async def analyze(llm, report=None):
    report = report or restart_report()
    return await AnalyzerAgent(llm).analyze(build_diagnostic_evidence(report))


# ------------------------------------------------------------ happy path & contract
async def test_valid_structured_response_is_grounded_and_matches_contract_shape():
    report = restart_report()
    res = await analyze(FakeLLM(answer_for(report)), report)
    assert res.status is AnalysisStatus.COMPLETED and res.attempts == 1
    r = res.report
    assert r.diagnosis.suspected_failure == "service_restart" and r.diagnosis.suspected_component == "sink-worker"
    assert r.grounding.grounding_rate == 1.0 and len(r.diagnosis.evidence) == len(r.diagnosis.evidence_refs) == 2
    assert r.recovery.functional_recovery_confirmed and r.recovery.functional_recovery_seconds == 8.0
    assert r.generation_latency_ms is not None
    # serialization round-trip + contract keys
    dumped = json.loads(r.model_dump_json())
    assert {"experiment_id", "diagnosis", "impact", "recommendations"} <= dumped.keys()
    assert {"suspected_component", "suspected_failure", "confidence", "evidence"} <= dumped["diagnosis"].keys()
    assert {"availability_affected", "processing_delayed", "data_loss_detected"} == dumped["impact"].keys()
    assert AnalysisReport.model_validate_json(r.model_dump_json()) == r


async def test_markdown_fenced_json_is_accepted():
    report = restart_report()
    res = await analyze(FakeLLM("```json\n" + answer_for(report) + "\n```"), report)
    assert res.status is AnalysisStatus.COMPLETED


# ------------------------------------------------------------ invalid output handling
async def test_invalid_json_then_valid_retry_succeeds_with_feedback():
    report = restart_report()
    llm = FakeLLM("this is not json", answer_for(report))
    res = await analyze(llm, report)
    assert res.status is AnalysisStatus.COMPLETED and res.attempts == 2
    assert "Your previous answer was rejected" in llm.calls[1][1]


async def test_persistently_invalid_output_returns_invalid_output_without_fabricating_a_report():
    res = await analyze(FakeLLM("nope"))
    assert res.status is AnalysisStatus.INVALID_OUTPUT and res.report is None and res.attempts == 2


async def test_missing_required_fields_are_rejected():
    res = await analyze(FakeLLM(json.dumps({"experiment_id": "exp_003", "diagnosis": {"suspected_component": "x"}})))
    assert res.status is AnalysisStatus.INVALID_OUTPUT and res.report is None


@pytest.mark.parametrize("patch", [
    {"suspected_failure": "meteor_strike"},          # not an allowed fault type
    {"suspected_component": "made-up-service"},     # not in the dependency map
])
async def test_out_of_vocabulary_values_are_rejected(patch):
    report = restart_report()
    data = json.loads(answer_for(report))
    data["diagnosis"].update(patch)
    res = await analyze(FakeLLM(json.dumps(data)), report)
    assert res.status is AnalysisStatus.INVALID_OUTPUT


async def test_wrong_experiment_id_rejected():
    report = restart_report()
    data = json.loads(answer_for(report))
    data["experiment_id"] = "someone_else"
    res = await analyze(FakeLLM(json.dumps(data)), report)
    assert res.status is AnalysisStatus.INVALID_OUTPUT


# ------------------------------------------------------------ LLM unavailability
async def test_no_llm_configured_reports_unavailable_not_a_guess():
    res = await AnalyzerAgent(None).analyze(build_diagnostic_evidence(restart_report()))
    assert res.status is AnalysisStatus.UNAVAILABLE and res.report is None and "NVIDIA_API_KEY" in res.error


@pytest.mark.parametrize("exc,kind", [
    (NvidiaTimeout("t"), "timeout"), (NvidiaRateLimited("r"), "rate_limited"),
    (NvidiaUnavailable("u"), "unavailable"), (NvidiaNotConfigured("n"), "not_configured"),
])
async def test_nvidia_errors_map_to_unavailable_with_kind(exc, kind):
    res = await analyze(FakeLLM(exc))
    assert res.status is AnalysisStatus.UNAVAILABLE and res.report is None and res.error.startswith(kind)


def _client(handler, **kw):
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    c = NvidiaChatClient("key", "meta/llama-3.3-70b-instruct", transport=httpx.MockTransport(handler),
                         sleep=fake_sleep, **kw)
    c.sleeps = sleeps
    return c


def _ok(content="{}"):
    return httpx.Response(200, json={"choices": [{"message": {"content": content}}]})


async def test_client_retries_rate_limit_then_succeeds():
    calls = []

    def handler(req):
        calls.append(req)
        return httpx.Response(429, headers={"Retry-After": "2"}) if len(calls) == 1 else _ok("hi")

    c = _client(handler, max_retries=2)
    assert await c.complete("s", "u") == "hi" and len(calls) == 2 and c.sleeps == [2.0]
    body = json.loads(calls[0].content)
    assert body["temperature"] == 0 and body["response_format"] == {"type": "json_object"}


async def test_client_persistent_rate_limit_is_bounded_and_structured():
    calls = []

    def handler(req):
        calls.append(1)
        return httpx.Response(429)

    with pytest.raises(NvidiaRateLimited) as ei:
        await _client(handler, max_retries=2).complete("s", "u")
    assert len(calls) == 3 and ei.value.attempts == 3


async def test_client_timeout_and_5xx_and_auth_failures():
    def timeout(req):
        raise httpx.ReadTimeout("slow", request=req)

    with pytest.raises(NvidiaTimeout):
        await _client(timeout, max_retries=1).complete("s", "u")
    with pytest.raises(NvidiaUnavailable):
        await _client(lambda r: httpx.Response(503), max_retries=1).complete("s", "u")
    calls = []

    def auth(req):
        calls.append(1)
        return httpx.Response(401, text="bad key")

    with pytest.raises(NvidiaUnavailable):
        await _client(auth, max_retries=3).complete("s", "u")
    assert len(calls) == 1  # 401 is never retried


async def test_client_falls_back_when_json_mode_rejected_and_checks_missing_key():
    seen = []

    def handler(req):
        body = json.loads(req.content)
        seen.append("response_format" in body)
        return httpx.Response(400, text="response_format unsupported") if "response_format" in body else _ok("{}")

    assert await _client(handler).complete("s", "u") == "{}" and seen == [True, False]
    with pytest.raises(NvidiaNotConfigured):
        await NvidiaChatClient(None, "m").complete("s", "u")


# ------------------------------------------------------------ insufficient evidence
async def test_insufficient_evidence_is_explicitly_uncertain_and_skips_the_llm():
    llm = FakeLLM("should never be called")
    res = await analyze(llm, empty_report())
    assert res.status is AnalysisStatus.INSUFFICIENT_EVIDENCE and llm.calls == []
    d = res.report.diagnosis
    assert d.suspected_failure == "uncertain" and d.confidence == 0.0 and d.evidence == []
    assert res.report.impact.availability_affected is None  # unknown, not False


async def test_unavailable_metrics_are_rendered_as_unavailable_not_zero():
    items = build_diagnostic_evidence(crash_report()).evidence_items
    p95 = next(i for i in items if i.statement.startswith("p95_latency_ms") and "fault window" in i.statement)
    assert "unavailable" in p95.statement and "0.0 ms" not in p95.statement.split("->")[1]


# ------------------------------------------------------------ grounding
async def test_claims_with_bad_refs_or_invented_numbers_are_removed():
    report = restart_report()
    lag, other = evidence_ids(report)
    data = json.loads(good_answer(report.experiment_id, [lag], [other]))
    data["diagnosis"]["evidence"] += [
        {"claim": "The worker crashed 7777 times", "refs": [lag]},          # number not in evidence
        {"claim": "Kafka brokers ran out of disk", "refs": ["E999"]},        # nonexistent evidence id
        {"claim": "Everything is probably fine", "refs": []},                # no refs
    ]
    res = await analyze(FakeLLM(json.dumps(data)), report)
    g = res.report.grounding
    assert (g.total_claims, g.supported_claims) == (5, 2) and g.grounding_rate == pytest.approx(0.4)
    assert len(g.unsupported_claims) == 3 and len(res.report.diagnosis.evidence) == 2
    assert res.report.diagnosis.confidence <= 0.6
    assert any("unsupported" in n for n in res.report.validation_notes)


async def test_fully_ungrounded_diagnosis_is_downgraded_to_uncertain():
    report = restart_report()
    data = json.loads(answer_for(report))
    for c in data["diagnosis"]["evidence"]:
        c["refs"] = ["E404"]
    res = await analyze(FakeLLM(json.dumps(data)), report)
    d = res.report.diagnosis
    assert d.suspected_failure == "uncertain" and d.confidence == 0.0 and d.evidence == []


async def test_impact_flags_are_reconciled_with_measured_facts():
    report = restart_report(missing=3)
    data = json.loads(answer_for(report))
    data["impact"] = {"availability_affected": True, "processing_delayed": False, "data_loss_detected": False}
    res = await analyze(FakeLLM(json.dumps(data)), report)
    i = res.report.impact
    assert (i.availability_affected, i.processing_delayed, i.data_loss_detected) == (False, True, True)
    assert len(res.report.validation_notes) == 3


# ------------------------------------------------------------ hidden ground truth
def test_prompt_is_a_pure_function_of_diagnostic_evidence_and_has_no_fault_label():
    report = restart_report()
    evidence = build_diagnostic_evidence(report)
    prompt = build_user_prompt(evidence)
    payload = json.loads(prompt)
    for fault in FailureType:  # the evidence itself (everything but the vocabulary list) never names a fault
        evidence_json = json.dumps({k: v for k, v in payload.items() if k not in ("allowed_failure_types", "required_output_schema")})
        assert fault.value not in evidence_json
    assert '"target_service"' not in prompt and '"actual_parameters"' not in prompt
    assert "injected" not in SYSTEM_PROMPT.lower().replace("deliberately injected", "")  # only the 'do not assume' line
    assert build_user_prompt(build_diagnostic_evidence(report)) == prompt  # deterministic


async def test_the_llm_receives_exactly_the_diagnostic_prompt():
    report = restart_report()
    llm = FakeLLM(answer_for(report))
    await analyze(llm, report)
    system, user = llm.calls[0]
    assert system == SYSTEM_PROMPT and user == build_user_prompt(build_diagnostic_evidence(report))


async def test_scoring_against_ground_truth_happens_after_diagnosis():
    report = restart_report()
    res = await analyze(FakeLLM(answer_for(report)), report)
    right = ExecutionGroundTruth(experiment_id="exp_003", failure_type=FailureType.SERVICE_RESTART, target_service="sink-worker")
    wrong = ExecutionGroundTruth(experiment_id="exp_003", failure_type=FailureType.NETWORK_LATENCY, target_service="kafka")
    assert ev.evaluate_against_truth(res, right).failure_correct and ev.evaluate_against_truth(res, right).component_correct
    e = ev.evaluate_against_truth(res, wrong)
    assert not e.failure_correct and not e.component_correct
    acc = ev.root_cause_accuracy([(res, right), (res, wrong)])
    assert (acc.numerator, acc.denominator) == (1, 2)


async def test_healthy_control_answer_no_fault_is_correct_and_a_false_alarm_is_wrong():
    report = healthy_report()
    quiet = json.dumps({
        "experiment_id": report.experiment_id,
        "diagnosis": {"suspected_component": "unknown", "suspected_failure": "no_fault_detected", "confidence": 0.8,
                      "evidence": [{"claim": "No anomalies were flagged during the whole observation",
                                    "refs": [next(i.id for i in build_diagnostic_evidence(report).evidence_items if "no anomalies" in i.statement)]}]},
        "impact": {"availability_affected": False, "processing_delayed": False, "data_loss_detected": False},
        "recommendations": ["Keep baseline monitoring in place"]})
    res = await analyze(FakeLLM(quiet), report)
    control = ExecutionGroundTruth(experiment_id=report.experiment_id, failure_type=None)
    assert res.status is AnalysisStatus.COMPLETED and ev.evaluate_against_truth(res, control).failure_correct
    alarm = json.loads(quiet)
    alarm["diagnosis"]["suspected_failure"] = "application_crash"
    res2 = await analyze(FakeLLM(json.dumps(alarm)), report)
    assert not ev.evaluate_against_truth(res2, control).failure_correct


def test_parse_llm_json_tolerates_leading_chatter():
    report = restart_report()
    out = parse_llm_json("Sure! Here is the JSON:\n" + answer_for(report))
    assert out.diagnosis.suspected_failure == "service_restart"


# ------------------------------------------------------------ evaluation helpers
async def test_analyzer_metrics_report_counts_not_just_rates():
    report = restart_report()
    ok = await analyze(FakeLLM(answer_for(report)), report)
    bad = await analyze(FakeLLM("nope"), report)
    down = await AnalyzerAgent(None).analyze(build_diagnostic_evidence(report))
    assert str(ev.structured_output_validity([ok, bad, down])) == "1/2 = 0.50"  # 'unavailable' never answered
    assert ev.evidence_grounding_rate([ok]).value == 1.0
    mean_ms, max_ms, n = ev.report_latency_ms([ok, bad])
    assert n == 1 and mean_ms == max_ms
    assert ev.recommendation_relevance_proxy(ok, "service_restart").numerator >= 2
    assert ev.Rate(0, 0).value is None
