"""Evaluation metrics for the Observer and the Analyzer.

Every function returns the counts it was computed from (`n`), because a rate without its sample size is
not evidence. With the handful of runs a hackathon allows, report the counts, not just the percentage.
"""
from __future__ import annotations

from dataclasses import dataclass
from statistics import mean

from ..monitoring.detection import flagged_windows
from ..schemas.analysis import (
    NO_FAULT,
    UNCERTAIN,
    AnalysisResult,
    AnalysisStatus,
    ExecutionGroundTruth,
    GroundTruthEvaluation,
)
from ..schemas.monitoring import MonitoringSample, ObservationReport


@dataclass(frozen=True)
class Rate:
    numerator: int
    denominator: int

    @property
    def value(self) -> float | None:
        return self.numerator / self.denominator if self.denominator else None

    def __str__(self) -> str:
        return f"{self.numerator}/{self.denominator}" + (f" = {self.value:.2f}" if self.denominator else " (n/a)")


# ------------------------------------------------------------------ Observer
def failure_detection_rate(reports: list[ObservationReport]) -> Rate:
    """Detected / evaluated injected failures (reports where a fault was actually activated)."""
    evaluated = [r for r in reports if r.detection.detection_success is not None]
    return Rate(sum(bool(r.detection.detection_success) for r in evaluated), len(evaluated))


def mean_time_to_detect(reports: list[ObservationReport]) -> tuple[float | None, int, int]:
    """(mean detection latency ms over detected faults, #detected, #missed). Misses are reported, not dropped."""
    evaluated = [r for r in reports if r.detection.detection_success is not None]
    lat = [r.detection.detection_latency_ms for r in evaluated
           if r.detection.detection_success and r.detection.detection_latency_ms is not None]
    return (mean(lat) if lat else None), len(lat), sum(1 for r in evaluated if not r.detection.detection_success)


def false_positive_rate(healthy: list[tuple[list[MonitoringSample], ObservationReport]], window_seconds: float) -> Rate:
    """Healthy windows incorrectly flagged / all healthy windows, over healthy-control experiments."""
    flagged = total = 0
    for samples, report in healthy:
        flagged += flagged_windows(samples, report.detection.anomalies, window_seconds)
        total += len(samples)
    return Rate(flagged, total)


def mean_monitoring_coverage(reports: list[ObservationReport]) -> float | None:
    vals = [r.monitoring_coverage for r in reports if r.monitoring_coverage is not None]
    return mean(vals) if vals else None


def recovery_detection_accuracy(pairs: list[tuple[ObservationReport, bool]]) -> Rate:
    """Observer's `functional_recovery_confirmed` vs an independent oracle (`truly_recovered`), per run."""
    return Rate(sum(r.recovery.functional_recovery_confirmed == truth for r, truth in pairs), len(pairs))


# ------------------------------------------------------------------ Analyzer
def evaluate_against_truth(result: AnalysisResult, truth: ExecutionGroundTruth) -> GroundTruthEvaluation | None:
    """Score a finished diagnosis against the hidden ground truth. Only meaningful after `analyze()`.
    For a healthy control (failure_type None) the correct answer is `no_fault_detected` (or `uncertain`)."""
    if result.report is None:
        return None
    d = result.report.diagnosis
    if truth.failure_type is None:
        failure_ok = d.suspected_failure in (NO_FAULT, UNCERTAIN)
        component_ok = True
    else:
        failure_ok = d.suspected_failure == truth.failure_type.value
        component_ok = d.suspected_component == truth.target_service
    return GroundTruthEvaluation(experiment_id=truth.experiment_id, failure_correct=failure_ok,
                                 component_correct=component_ok, predicted_failure=d.suspected_failure,
                                 predicted_component=d.suspected_component)


def root_cause_accuracy(cases: list[tuple[AnalysisResult, ExecutionGroundTruth]]) -> Rate:
    """Correct failure-type diagnoses / evaluated cases. Cases with no report (LLM unavailable) count as
    not evaluated, and are visible through `structured_output_validity`/availability counts."""
    scored = [e for res, gt in cases if (e := evaluate_against_truth(res, gt)) is not None]
    return Rate(sum(e.failure_correct for e in scored), len(scored))


def evidence_grounding_rate(results: list[AnalysisResult]) -> Rate:
    """Supported diagnostic claims / all diagnostic claims the model produced."""
    reps = [r.report for r in results if r.report is not None and r.status is AnalysisStatus.COMPLETED]
    return Rate(sum(r.grounding.supported_claims for r in reps), sum(r.grounding.total_claims for r in reps))


def structured_output_validity(results: list[AnalysisResult]) -> Rate:
    """Valid reports / generation attempts that reached the model (unavailable = model never answered)."""
    answered = [r for r in results if r.status is not AnalysisStatus.UNAVAILABLE
                and r.status is not AnalysisStatus.INSUFFICIENT_EVIDENCE]
    return Rate(sum(r.status is AnalysisStatus.COMPLETED for r in answered), len(answered))


def report_latency_ms(results: list[AnalysisResult]) -> tuple[float | None, float | None, int]:
    """(mean, max, n) of generation+validation latency for completed reports."""
    lat = [r.report.generation_latency_ms for r in results
           if r.report is not None and r.report.generation_latency_ms is not None and r.status is AnalysisStatus.COMPLETED]
    return (mean(lat) if lat else None), (max(lat) if lat else None), len(lat)


# Recommendation relevance needs human judgement. This keyword rubric is only a rough PROXY to triage
# reports; do not present its output as a measured relevance score without manual review.
RELEVANCE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "application_crash": ("health", "retry", "restart", "redundan", "replica", "failover", "circuit", "availability"),
    "network_latency": ("timeout", "retry", "backpressure", "latency", "circuit", "buffer", "backoff"),
    "service_restart": ("lag", "consumer", "catch-up", "catch up", "restart", "readiness", "backlog", "idempot"),
    "traffic_spike": ("rate limit", "capacity", "scal", "backpressure", "queue", "autoscal", "throttl"),
}


def recommendation_relevance_proxy(result: AnalysisResult, true_failure: str) -> Rate:
    """Recommendations containing at least one keyword for the true failure / all recommendations."""
    if result.report is None:
        return Rate(0, 0)
    words = RELEVANCE_KEYWORDS.get(true_failure, ())
    recs = result.report.recommendations
    return Rate(sum(any(w in r.lower() for w in words) for r in recs), len(recs))
