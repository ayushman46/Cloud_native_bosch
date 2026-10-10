"""Hand-built ObservationReports (illustrative numbers, used ONLY in tests) and a scriptable fake LLM."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from app.schemas.monitoring import (
    Anomaly,
    DetectionResult,
    LifecycleTimes,
    ObservationReport,
    PhaseMetrics,
    RecoverySummary,
)

T0 = datetime(2026, 10, 10, 7, 0, 0, tzinfo=timezone.utc)


def at(s: float) -> datetime:
    return T0 + timedelta(seconds=s)


def _lifecycle(fault=True) -> LifecycleTimes:
    return LifecycleTimes(
        monitoring_started_at=T0,
        fault_activated_at=at(20) if fault else None,
        rollback_completed_at=at(40) if fault else None,
        monitoring_finished_at=at(70),
        injection_success=True if fault else None,
    )


def restart_report(exp="exp_003", missing=0, duplicates=0) -> ObservationReport:
    """Looks like a sink-worker outage: API fine, processing lag up, worker health down."""
    base = PhaseMetrics(availability=1.0, error_rate=0.0, p95_latency_ms=85, throughput_rps=10,
                        processing_lag_ms=250, request_count=200, health_probe_count=40, duration_seconds=20,
                        service_availability={"sink-worker": 1.0})
    fault = PhaseMetrics(availability=1.0, error_rate=0.0, p95_latency_ms=90, throughput_rps=10,
                         processing_lag_ms=3500, request_count=200, health_probe_count=40, duration_seconds=20,
                         service_availability={"sink-worker": 0.4})
    rec = PhaseMetrics(availability=1.0, error_rate=0.0, p95_latency_ms=88, throughput_rps=10, processing_lag_ms=400,
                       request_count=100, health_probe_count=20, duration_seconds=10,
                       service_availability={"sink-worker": 1.0})
    return ObservationReport(
        experiment_id=exp, baseline=base, during_failure=fault,
        recovery=RecoverySummary(functional_recovery_seconds=8.0, functional_recovery_confirmed=True,
                                 missing_events=missing, duplicate_events=duplicates, metering_accuracy=1.0,
                                 accepted_events=500, uncertain_events=0, metrics=rec),
        detection=DetectionResult(
            detection_success=True, detection_latency_ms=800, first_anomaly="CONSECUTIVE_PROBE_FAILURES",
            anomalies=[
                Anomaly(kind="CONSECUTIVE_PROBE_FAILURES", detected_at=at(20.8), metric="health:sink-worker_probe_consecutive_failures",
                        observed=3, threshold=3, evidence_ref="probe#12"),
                Anomaly(kind="HIGH_PROCESSING_LAG", detected_at=at(24), metric="processing_lag_ms",
                        observed=3500, threshold=3000, baseline=250, evidence_ref="sample@x"),
            ]),
        lifecycle=_lifecycle(), monitoring_coverage=1.0,
        observer_notes=[],
    )


def crash_report(exp="exp_001") -> ObservationReport:
    base = PhaseMetrics(availability=1.0, error_rate=0.0, p95_latency_ms=85, throughput_rps=10, processing_lag_ms=250,
                        request_count=200, health_probe_count=40, duration_seconds=20)
    fault = PhaseMetrics(availability=0.0, error_rate=1.0, p95_latency_ms=None, throughput_rps=0.0,
                         processing_lag_ms=None, request_count=100, health_probe_count=40, duration_seconds=20)
    return ObservationReport(
        experiment_id=exp, baseline=base, during_failure=fault,
        recovery=RecoverySummary(functional_recovery_seconds=12.0, functional_recovery_confirmed=True,
                                 missing_events=0, duplicate_events=0, metering_accuracy=1.0, accepted_events=300,
                                 uncertain_events=0),
        detection=DetectionResult(detection_success=True, detection_latency_ms=600, first_anomaly="CONSECUTIVE_PROBE_FAILURES",
                                  anomalies=[Anomaly(kind="CONSECUTIVE_PROBE_FAILURES", detected_at=at(20.6),
                                                     metric="ingest_probe_consecutive_failures", observed=3, threshold=3,
                                                     evidence_ref="probe#9")]),
        lifecycle=_lifecycle(), monitoring_coverage=0.6)


def healthy_report(exp="exp_h1") -> ObservationReport:
    base = PhaseMetrics(availability=1.0, error_rate=0.0, p95_latency_ms=80, throughput_rps=10, processing_lag_ms=240,
                        request_count=300, health_probe_count=60, duration_seconds=30)
    return ObservationReport(
        experiment_id=exp, baseline=base, during_failure=None,
        recovery=RecoverySummary(missing_events=0, duplicate_events=0, metering_accuracy=1.0, accepted_events=300,
                                 uncertain_events=0),
        detection=DetectionResult(), lifecycle=_lifecycle(fault=False), monitoring_coverage=1.0)


def empty_report(exp="exp_e") -> ObservationReport:
    return ObservationReport(experiment_id=exp, baseline=PhaseMetrics(), during_failure=None,
                             recovery=RecoverySummary(), detection=DetectionResult(), lifecycle=_lifecycle(fault=False))


class FakeLLM:
    """Returns scripted responses (str or exception) in order; records every prompt it receives."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls: list[tuple[str, str]] = []

    async def complete(self, system: str, user: str) -> str:
        self.calls.append((system, user))
        r = self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]
        if isinstance(r, BaseException):
            raise r
        return r(system, user) if callable(r) else r


def ids_by_keyword(user_prompt: str) -> dict[str, str]:
    """Map a keyword -> evidence id by reading the prompt (what a grounded model would do)."""
    payload = json.loads(user_prompt.split("\n\nYour previous answer")[0])
    return {i["id"]: i["statement"] for i in payload["evidence_items"]}


def good_answer(exp: str, refs_lag: list[str], refs_other: list[str], **over) -> str:
    out = {
        "experiment_id": exp,
        "diagnosis": {
            "suspected_component": "sink-worker", "suspected_failure": "service_restart", "confidence": 0.9,
            "evidence": [
                {"claim": "Processing lag rose from 250.0 ms to 3500.0 ms during the fault window", "refs": refs_lag},
                {"claim": "Inference: the sink-worker stopped consuming and then caught up", "refs": refs_other},
            ],
        },
        "impact": {"availability_affected": False, "processing_delayed": True, "data_loss_detected": False},
        "recovery_assessment": "Recovery was confirmed after the worker returned.",
        "recommendations": ["Monitor Kafka consumer lag", "Alert on prolonged processing delays",
                            "Verify worker restart and catch-up behavior"],
    }
    out.update(over)
    return json.dumps(out)
