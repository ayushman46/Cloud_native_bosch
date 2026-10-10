"""Unit tests for probes, metric calculations, ledger accounting and detection (no network, no sleeps
except where a real latency is being measured against a mock transport)."""
import asyncio
import socket
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from app.clients.openmeter_client import OpenMeterClient
from app.monitoring import metrics as M
from app.monitoring.collector import MonitorConfig, functional_recovery_time
from app.monitoring.detection import DetectionConfig, detect, flagged_windows, probe_anomalies, sample_anomalies
from app.monitoring.http_probes import build_cloudevent, run_health_probe, run_ingest_probe
from app.monitoring.meter_probes import EventLedger, poll_meter_once, sum_meter_rows
from app.monitoring.records import Outcome, ProbeRecord, classify_exception, classify_status
from app.schemas.monitoring import MonitoringSample, Phase, PhaseMetrics, SampleMetrics

T0 = datetime(2026, 10, 10, 7, 0, 0, tzinfo=timezone.utc)


def at(s: float) -> datetime:
    return T0 + timedelta(seconds=s)


_seq = 0


def rec(t: float, outcome=Outcome.OK, dur=50.0, kind="ingest", service="openmeter-api", event_id=None, started=None):
    global _seq
    _seq += 1
    if kind == "ingest" and event_id is None:
        event_id = f"e{_seq}"
    return ProbeRecord(_seq, kind, service, started if started is not None else at(t) - timedelta(milliseconds=dur),
                       at(t), dur, outcome, 204 if outcome is Outcome.OK else None, event_id)


CFG = M.MetricsConfig()


# ------------------------------------------------------------ percentile & basics
def test_percentile_nearest_rank():
    assert M.percentile_nearest_rank(range(1, 21), 95) == 19
    assert M.percentile_nearest_rank([7], 95) == 7
    assert M.percentile_nearest_rank([], 95) is None
    assert M.percentile_nearest_rank([5, 1, 3], 50) == 3


def test_p95_requires_minimum_samples():
    one = [rec(1, dur=500)]
    assert M.p95_latency_ms(one, CFG) is None  # never from a single request
    many = [rec(i, dur=d) for i, d in enumerate([10, 20, 30, 40, 1000], start=1)]
    assert M.p95_latency_ms(many, CFG) == 1000


def test_error_rate_availability_throughput_and_empty_window():
    ingest = [rec(1), rec(2), rec(3, Outcome.SERVER_ERROR), rec(4, Outcome.CONNECTION_REFUSED)]
    assert M.error_rate(ingest) == 0.5
    assert M.throughput_rps(ingest, 2.0) == 1.0  # 2 successes / 2 s
    health = [rec(1, kind="health"), rec(2, Outcome.READ_TIMEOUT, kind="health")]
    assert M.availability(health) == 0.5
    assert M.error_rate([]) is None and M.availability([]) is None and M.throughput_rps([], 5) is None
    pm = M.phase_metrics([], [], [], at(0), at(10), "openmeter-api", CFG)
    assert pm.availability is None and pm.error_rate is None and pm.p95_latency_ms is None
    assert pm.cpu_percent is None and pm.request_count == 0  # unavailable -> None, not 0


def test_unexpected_4xx_is_not_an_infrastructure_failure_but_still_fails_probe():
    bad = rec(1, Outcome.UNEXPECTED_CLIENT_ERROR)
    assert not bad.ok and not bad.is_infra_failure
    assert M.error_rate([bad, rec(2)]) == 0.0


def test_latency_percentile_excludes_fast_local_failures_but_includes_timeouts():
    ingest = [rec(i, dur=40) for i in range(1, 6)] + [rec(6, Outcome.CONNECTION_REFUSED, dur=1),
                                                       rec(7, Outcome.READ_TIMEOUT, dur=2000)]
    assert M.p95_latency_ms(ingest, CFG) == 2000
    assert M.timeout_rate(ingest) == pytest.approx(1 / 7)


# ------------------------------------------------------------ classification
def test_classify_status():
    assert classify_status(204, frozenset({204})) is Outcome.OK
    assert classify_status(500, frozenset({204})) is Outcome.SERVER_ERROR
    assert classify_status(503, frozenset({200})) is Outcome.SERVER_ERROR
    assert classify_status(429, frozenset({204})) is Outcome.RATE_LIMITED
    assert classify_status(400, frozenset({204})) is Outcome.UNEXPECTED_CLIENT_ERROR


def test_classify_exception():
    refused = httpx.ConnectError("boom")
    refused.__cause__ = ConnectionRefusedError()
    dns = httpx.ConnectError("boom")
    dns.__cause__ = socket.gaierror(-2, "Name or service not known")
    assert classify_exception(refused) is Outcome.CONNECTION_REFUSED
    assert classify_exception(dns) is Outcome.DNS_FAILURE
    assert classify_exception(httpx.ConnectTimeout("t")) is Outcome.CONNECT_TIMEOUT
    assert classify_exception(httpx.ReadTimeout("t")) is Outcome.READ_TIMEOUT
    assert classify_exception(httpx.RemoteProtocolError("reset")) is Outcome.TRANSPORT_ERROR


# ------------------------------------------------------------ probes over a mock transport
def _client(handler) -> OpenMeterClient:
    return OpenMeterClient("http://om.test", transport=httpx.MockTransport(handler), timeout_seconds=1.0)


@pytest.mark.anyio
async def test_health_probe_success_and_http_500():
    ok = await run_health_probe(_client(lambda r: httpx.Response(200)), 1, "openmeter-api", "http://om.test/h")
    assert ok.ok and ok.status_code == 200 and ok.duration_ms >= 0
    bad = await run_health_probe(_client(lambda r: httpx.Response(500)), 2, "openmeter-api", "http://om.test/h")
    assert bad.outcome is Outcome.SERVER_ERROR and bad.is_infra_failure


@pytest.mark.anyio
async def test_probe_connection_refused_and_timeout():
    def refuse(request):
        err = httpx.ConnectError("All connection attempts failed", request=request)
        err.__cause__ = ConnectionRefusedError()
        raise err

    def slow(request):
        raise httpx.ReadTimeout("timed out", request=request)

    r1 = await run_health_probe(_client(refuse), 1, "api", "http://om.test/h")
    r2 = await run_health_probe(_client(slow), 2, "api", "http://om.test/h")
    assert r1.outcome is Outcome.CONNECTION_REFUSED and not r1.has_latency_signal
    assert r2.outcome is Outcome.READ_TIMEOUT and r2.is_timeout and r2.has_latency_signal


@pytest.mark.anyio
async def test_ingest_probe_measures_high_latency_and_requires_204():
    async def slow_ok(request):
        await asyncio.sleep(0.08)
        return httpx.Response(204)

    ev = build_cloudevent(event_id="x1", subject="s", event_type="request", time=T0, data={})
    r = await run_ingest_probe(_client(slow_ok), 1, "api", ev)
    assert r.ok and r.duration_ms >= 70 and r.event_id == "x1"
    # 200 is NOT an acceptance for the ingest endpoint (spec: 204)
    r2 = await run_ingest_probe(_client(lambda req: httpx.Response(200)), 2, "api", ev)
    assert r2.outcome is Outcome.UNEXPECTED_CLIENT_ERROR and not r2.ok


def test_cloudevent_shape():
    ev = build_cloudevent(event_id="a", subject="chaos-exp", event_type="request", time=T0, data={"k": 1})
    assert ev["specversion"] == "1.0" and ev["id"] == "a" and ev["subject"] == "chaos-exp"
    assert ev["time"].endswith("Z")


# ------------------------------------------------------------ ledger / metering accounting
def _ingest_ledger(n: int, spacing=1.0) -> EventLedger:
    led = EventLedger()
    for i in range(n):
        led.record_ingest(rec(i * spacing, event_id=f"ev{i}"))
    return led


def test_lag_from_delayed_visibility():
    led = _ingest_ledger(3)  # accepted at t=0,1,2
    led.record_observation(at(0.5), 0)
    led.record_observation(at(4.0), 2)  # events 1,2 visible at t=4
    led.record_observation(at(9.0), 3)
    lags = led.lags(at(10))
    assert [round(l.lag_ms) for l in lags] == [4000, 3000, 7000]
    assert not any(l.censored for l in lags)


def test_pending_events_are_censored_lower_bound_and_missing_after_deadline():
    led = _ingest_ledger(3)
    led.record_observation(at(5), 1)
    lags = led.lags(at(10))
    assert lags[0].lag_ms == 5000 and not lags[0].censored
    assert lags[1].censored and lags[1].lag_ms == 4000  # bounded by the last meter read (t=5), not "now"
    acc = led.accounting()
    assert acc.expected_events == 3 and acc.missing_events == 2 and acc.duplicate_events == 0
    assert acc.metering_accuracy == pytest.approx(1 / 3)


def test_lag_is_unknown_not_zero_when_meter_never_read():
    led = _ingest_ledger(3)
    assert led.lags(at(10)) == []


def test_missing_and_duplicate_and_exact_accounting():
    led = _ingest_ledger(4)
    led.record_observation(at(9), 4)
    a = led.accounting()
    assert (a.missing_events, a.duplicate_events, a.metering_accuracy) == (0, 0, 1.0)
    led.record_observation(at(10), 6)
    a = led.accounting()
    assert a.duplicate_events == 2 and a.missing_events == 0 and a.metering_accuracy == 0.5


def test_uncertain_events_are_not_reported_as_duplicates():
    led = _ingest_ledger(4)
    led.record_ingest(rec(5, Outcome.READ_TIMEOUT, event_id="late"))  # may have been enqueued
    led.record_observation(at(9), 5)
    a = led.accounting()
    assert a.uncertain_events == 1 and a.duplicate_events == 0


def test_accounting_none_when_meter_never_read_and_empty_ledger():
    assert EventLedger().accounting() is None
    led = EventLedger()
    led.record_observation(at(1), 0)
    a = led.accounting()
    assert a.expected_events == 0 and a.metering_accuracy is None  # undefined, not 1.0


def test_sum_meter_rows_validation():
    assert sum_meter_rows({"data": [{"value": 2}, {"value": 3.5}]}) == 5.5
    assert sum_meter_rows({"data": []}) == 0.0
    assert sum_meter_rows({"nope": 1}) is None
    assert sum_meter_rows({"data": [{"value": "x"}]}) is None


@pytest.mark.anyio
async def test_poll_meter_handles_failure_and_invalid_body():
    led = EventLedger()
    r1 = await poll_meter_once(_client(lambda r: httpx.Response(503)), led, "m", "s", T0)
    r2 = await poll_meter_once(_client(lambda r: httpx.Response(200, json={"weird": True})), led, "m", "s", T0)
    r3 = await poll_meter_once(_client(lambda r: httpx.Response(200, json={"data": [{"value": 2}]})), led, "m", "s", T0)
    assert r1.outcome is Outcome.SERVER_ERROR and r2.outcome is Outcome.INVALID_RESPONSE
    assert led.query_failures == 2 and len(led.observations) == 1 and led.observations[0].count == 2


# ------------------------------------------------------------ lifecycle & phases
def test_lifecycle_phases_use_injector_timestamps():
    lc = M.Lifecycle(T0, fault_activated_at=at(10), rollback_completed_at=at(20))
    assert lc.phase_at(at(9.9)) is Phase.BASELINE
    assert lc.phase_at(at(10)) is Phase.FAULT
    assert lc.phase_at(at(19.9)) is Phase.FAULT
    assert lc.phase_at(at(20)) is Phase.RECOVERY
    w = lc.windows(at(30))
    assert w[Phase.BASELINE] == (T0, at(10)) and w[Phase.FAULT] == (at(10), at(20)) and w[Phase.RECOVERY] == (at(20), at(30))
    assert M.Lifecycle(T0).windows(at(5))[Phase.FAULT] is None


def test_samples_cover_windows_and_tag_phase():
    recs = [rec(t + 0.1, kind="health") for t in range(0, 8)] + [rec(t + 0.2) for t in range(0, 8)]
    lc = M.Lifecycle(T0, fault_activated_at=at(4), rollback_completed_at=at(6))
    s = M.build_samples("exp", "svc", recs, [], [], lc, at(8), "openmeter-api", M.MetricsConfig(sample_window_seconds=2.0))
    assert [x.phase for x in s] == [Phase.BASELINE, Phase.BASELINE, Phase.FAULT, Phase.RECOVERY]
    assert s[0].metrics.availability == 1.0 and s[0].request_count == 2
    assert s[0].metrics.processing_lag_ms is None and s[0].metrics.cpu_percent is None


# ------------------------------------------------------------ detection
def _sample(t, **kw):
    n = kw.pop("n", 10)
    return MonitoringSample(experiment_id="e", timestamp=at(t), phase=Phase.FAULT, metrics=SampleMetrics(**kw),
                            request_count=n, health_probe_count=kw.get("hn", 4))


BASE = PhaseMetrics(availability=1.0, error_rate=0.0, p95_latency_ms=85, processing_lag_ms=250)


def test_consecutive_failures_fire_once_on_third_failure():
    recs = [rec(1), rec(2, Outcome.CONNECTION_REFUSED), rec(3, Outcome.CONNECTION_REFUSED),
            rec(4, Outcome.CONNECTION_REFUSED), rec(5, Outcome.CONNECTION_REFUSED)]
    a = probe_anomalies(recs, "openmeter-api", DetectionConfig())
    assert len(a) == 1 and a[0].detected_at == at(4)


def test_two_failures_then_success_is_not_an_anomaly():
    recs = [rec(1, Outcome.READ_TIMEOUT), rec(2, Outcome.READ_TIMEOUT), rec(3), rec(4, Outcome.READ_TIMEOUT)]
    assert probe_anomalies(recs, "openmeter-api", DetectionConfig()) == []


def test_error_rate_needs_minimum_requests():
    assert sample_anomalies([_sample(2, error_rate=1.0, n=2)], BASE, DetectionConfig()) == []
    out = sample_anomalies([_sample(2, error_rate=0.5, n=10)], BASE, DetectionConfig())
    assert [a.kind for a in out] == ["HIGH_ERROR_RATE"]


def test_latency_threshold_is_baseline_aware():
    cfg = DetectionConfig()
    slow_baseline = PhaseMetrics(p95_latency_ms=2000)  # threshold = max(1000, 3*2000) = 6000
    assert sample_anomalies([_sample(2, p95_latency_ms=4000)], slow_baseline, cfg) == []
    assert sample_anomalies([_sample(2, p95_latency_ms=4000)], BASE, cfg)[0].kind == "HIGH_P95_LATENCY"


def test_processing_lag_detection_and_none_is_skipped():
    cfg = DetectionConfig()
    assert sample_anomalies([_sample(2, processing_lag_ms=2500)], BASE, cfg) == []  # below floor 3000
    assert sample_anomalies([_sample(2, processing_lag_ms=3500)], BASE, cfg)[0].kind == "HIGH_PROCESSING_LAG"
    assert sample_anomalies([_sample(2)], BASE, cfg) == []  # all None -> nothing flagged


def test_healthy_control_has_no_anomalies_and_no_detection_target():
    recs = [rec(t) for t in range(10)] + [rec(t + 0.5, kind="health") for t in range(10)]
    lc = M.Lifecycle(T0)
    samples = M.build_samples("e", None, recs, [], [], lc, at(10), "openmeter-api", M.MetricsConfig())
    res = detect(recs, samples, PhaseMetrics(availability=1.0), lc, "openmeter-api")
    assert res.anomalies == [] and res.detection_success is None and res.pre_fault_anomalies == 0


def test_detection_latency_from_fault_activation_and_pre_fault_false_positive():
    recs = [rec(t) for t in range(0, 8)] + [rec(t, Outcome.SERVER_ERROR) for t in (9, 10, 11)]
    lc = M.Lifecycle(T0, fault_activated_at=at(8.5), rollback_completed_at=at(20))
    res = detect(recs, [], BASE, lc, "openmeter-api")
    assert res.detection_success is True and res.detection_latency_ms == pytest.approx(2500)
    assert res.first_anomaly == "CONSECUTIVE_PROBE_FAILURES" and res.pre_fault_anomalies == 0
    # same anomaly before activation counts as a pre-fault (false positive)
    lc2 = M.Lifecycle(T0, fault_activated_at=at(15), rollback_completed_at=at(20))
    res2 = detect(recs, [], BASE, lc2, "openmeter-api")
    assert res2.pre_fault_anomalies == 1 and res2.detection_success is False


def test_flagged_windows_counts_windows_with_anomalies():
    samples = [_sample(2, error_rate=0.0), _sample(4, error_rate=0.9), _sample(6, error_rate=0.0)]
    anomalies = sample_anomalies(samples, BASE, DetectionConfig())
    assert flagged_windows(samples, anomalies, 2.0) == 1


# ------------------------------------------------------------ functional recovery
def _recovery_records(ref_s, ok_from_s, n=6):
    out = []
    for i in range(n):
        t = ref_s + i
        outcome = Outcome.OK if t >= ok_from_s else Outcome.CONNECTION_REFUSED
        out.append(rec(t + 0.5, outcome, kind="health"))
        out.append(rec(t + 0.6, outcome))
    return out


CFGM = MonitorConfig(health_targets={"openmeter-api": "http://x"}, recovery_confirm_probes=3)


def test_recovery_needs_consecutive_ok_probes_and_pipeline_catch_up():
    ref = at(10)
    recs = _recovery_records(10, ok_from_s=12)
    led = EventLedger()
    for r in recs:
        led.record_ingest(r)
    # API probes healthy from t=12, but nothing processed yet -> NOT recovered
    assert functional_recovery_time(recs, led, ref, CFGM) is None
    # meter becomes consistent at t=16
    led.record_observation(at(16), len(led.accepted))
    t = functional_recovery_time(recs, led, ref, CFGM)
    assert t == at(16)  # limited by pipeline catch-up, not by the API probes


def test_probes_started_before_reference_do_not_count_toward_recovery():
    ref = at(10)
    early = rec(10.2, kind="health", started=at(9.0))  # started before ref
    assert functional_recovery_time([early], EventLedger(), ref, CFGM, meter_enabled=False) is None
