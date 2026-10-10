"""Monitoring contracts: samples, observation report, detection result.

Conventions (team contract): UTC ISO 8601, `_ms` = milliseconds, `_rate` = fraction 0..1,
unavailable metrics are `None` (never 0).
"""
from __future__ import annotations

from enum import Enum

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class Phase(str, Enum):
    BASELINE = "baseline"
    FAULT = "fault"
    RECOVERY = "recovery"


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SampleMetrics(_Model):
    availability: float | None = Field(default=None, ge=0, le=1)
    error_rate: float | None = Field(default=None, ge=0, le=1)
    p95_latency_ms: float | None = Field(default=None, ge=0)
    throughput_rps: float | None = Field(default=None, ge=0)
    processing_lag_ms: float | None = Field(default=None, ge=0)
    cpu_percent: float | None = Field(default=None, ge=0)
    memory_mb: float | None = Field(default=None, ge=0)


class MonitoringSample(_Model):
    """One aggregation window (`window_seconds`) of real probe results."""

    experiment_id: str
    timestamp: AwareDatetime  # window end
    phase: Phase
    target_service: str | None = None
    metrics: SampleMetrics
    request_count: int = Field(default=0, ge=0)  # ingest requests attempted in the window
    health_probe_count: int = Field(default=0, ge=0)


class PhaseMetrics(_Model):
    """Aggregate of a phase, computed over raw probe results (not an average of sample p95s)."""

    availability: float | None = Field(default=None, ge=0, le=1)
    error_rate: float | None = Field(default=None, ge=0, le=1)
    timeout_rate: float | None = Field(default=None, ge=0, le=1)
    p95_latency_ms: float | None = Field(default=None, ge=0)
    throughput_rps: float | None = Field(default=None, ge=0)
    processing_lag_ms: float | None = Field(default=None, ge=0)
    cpu_percent: float | None = Field(default=None, ge=0)
    memory_mb: float | None = Field(default=None, ge=0)
    request_count: int = Field(default=0, ge=0)
    health_probe_count: int = Field(default=0, ge=0)
    duration_seconds: float | None = Field(default=None, ge=0)
    # worker/service health probes by service name (availability 0..1); None = never probed
    service_availability: dict[str, float | None] = Field(default_factory=dict)


class RecoverySummary(_Model):
    functional_recovery_seconds: float | None = Field(default=None, ge=0)
    functional_recovery_confirmed: bool = False
    missing_events: int | None = Field(default=None, ge=0)
    duplicate_events: int | None = Field(default=None, ge=0)
    metering_accuracy: float | None = Field(default=None, ge=0, le=1)
    # additions (not in the base contract): supporting counts and post-fault metrics
    accepted_events: int | None = Field(default=None, ge=0)
    uncertain_events: int | None = Field(default=None, ge=0)
    metrics: PhaseMetrics | None = None


class Anomaly(_Model):
    kind: str  # CONSECUTIVE_PROBE_FAILURES | HIGH_ERROR_RATE | LOW_AVAILABILITY | HIGH_P95_LATENCY | HIGH_PROCESSING_LAG
    detected_at: AwareDatetime
    metric: str
    observed: float
    threshold: float
    baseline: float | None = None
    evidence_ref: str  # e.g. "sample@2026-10-10T07:00:05Z" or "probe#42"


class DetectionResult(_Model):
    detection_success: bool | None = None  # None: no fault was activated, nothing to detect
    detection_latency_ms: float | None = Field(default=None, ge=0)
    first_anomaly: str | None = None
    anomalies: list[Anomaly] = Field(default_factory=list)
    pre_fault_anomalies: int = Field(default=0, ge=0)  # anomalies flagged during baseline (false positives)


class LifecycleTimes(_Model):
    monitoring_started_at: AwareDatetime
    fault_activated_at: AwareDatetime | None = None
    rollback_started_at: AwareDatetime | None = None
    rollback_completed_at: AwareDatetime | None = None
    monitoring_finished_at: AwareDatetime | None = None
    injection_success: bool | None = None


class ObservationReport(_Model):
    """Consumed by the Analyzer. Field names from the contract plus clearly-marked additions."""

    experiment_id: str
    baseline: PhaseMetrics
    during_failure: PhaseMetrics | None = None
    recovery: RecoverySummary
    detection: DetectionResult
    # additions
    lifecycle: LifecycleTimes
    monitoring_coverage: float | None = Field(default=None, ge=0, le=1)
    observer_notes: list[str] = Field(default_factory=list)


class MonitoringStartRequest(_Model):
    experiment_id: str = Field(min_length=1)
    target_service: str | None = None  # optional; also learned from the injection event


class MetricsResponse(_Model):
    experiment_id: str
    state: str  # created | running | finalizing | finalized
    current_phase: Phase
    baseline_ready: bool
    phases: dict[str, PhaseMetrics | None]
    samples: list[MonitoringSample]
    notes: list[str] = Field(default_factory=list)
    report: ObservationReport | None = None  # present once finalized
