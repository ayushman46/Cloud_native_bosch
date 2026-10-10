"""Shared, versioned data contracts for the Hacker Agent Observer.

Use this module from the Observer Agent, Orchestrator, Safety Controller,
and Analyzer. Keep field names stable; version breaking changes.

Install: pip install 'pydantic>=2.5'
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from pydantic import BaseModel, ConfigDict, Field

SCHEMA_VERSION = "1.0.0"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", use_enum_values=True)


class FaultType(str, Enum):
    CONTAINER_CRASH = "container_crash"
    CONTAINER_RESTART = "container_restart"
    NETWORK_DELAY = "network_delay"
    NETWORK_FAILURE = "network_failure"
    HEAVY_LOAD = "heavy_load"
    STORAGE_LIMIT = "storage_limit"
    DATABASE_UNAVAILABLE = "database_unavailable"
    UNKNOWN = "unknown"


class ExperimentPhase(str, Enum):
    BASELINE = "baseline"
    INJECTION = "injection"
    RECOVERY = "recovery"
    COMPLETE = "complete"
    ABORTED = "aborted"


class HealthState(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class EventSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class RecoveryStatus(str, Enum):
    NOT_STARTED = "not_started"
    RECOVERING = "recovering"
    VERIFIED = "verified"
    FAILED = "failed"
    NOT_REQUIRED = "not_required"


class ExperimentStatus(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    ABORTED = "aborted"
    COMPLETED = "completed"
    FAILED = "failed"


class ThresholdPolicy(ContractModel):
    """Per-target policy. Do not rely on these example limits as production defaults."""
    max_duration_seconds: int = Field(default=30, ge=1, le=300)
    max_http_5xx_rate_pct: float | None = Field(default=None, ge=0, le=100)
    max_p95_latency_ms: float | None = Field(default=None, gt=0)
    max_cpu_pct: float | None = Field(default=None, ge=0, le=100)
    max_memory_pct: float | None = Field(default=None, ge=0, le=100)
    stop_on_target_unhealthy: bool = True
    stop_on_database_unhealthy: bool = True


class ExperimentContext(ContractModel):
    """Created by Orchestrator/Planner and shared with every component."""
    schema_version: str = SCHEMA_VERSION
    experiment_id: str
    run_id: str
    target_app: str  # e.g. "cal_diy" or "bosch_sunrise"
    target_profile: str  # versioned config profile name
    sandbox_id: str
    target_services: list[str] = Field(default_factory=list)
    target_service: str | None = None
    fault_type: FaultType
    phase: ExperimentPhase = ExperimentPhase.BASELINE
    started_at: datetime = Field(default_factory=utc_now)
    fault_started_at: datetime | None = None
    thresholds: ThresholdPolicy
    metadata: dict[str, Any] = Field(default_factory=dict)


class MetricSample(ContractModel):
    """One read-only observation from a probe, Docker, database, or metrics backend.

    Use None for unavailable data; do not convert missing metrics to zero.
    Rate fields are percentages from 0 to 100.
    """
    schema_version: str = SCHEMA_VERSION
    experiment_id: str
    run_id: str
    timestamp: datetime = Field(default_factory=utc_now)
    service: str
    source: str  # http_probe | docker | postgres | app_log | prometheus
    health: HealthState = HealthState.UNKNOWN
    request_count: int | None = Field(default=None, ge=0)
    failed_request_count: int | None = Field(default=None, ge=0)
    http_5xx_rate_pct: float | None = Field(default=None, ge=0, le=100)
    success_rate_pct: float | None = Field(default=None, ge=0, le=100)
    p50_latency_ms: float | None = Field(default=None, ge=0)
    p95_latency_ms: float | None = Field(default=None, ge=0)
    p99_latency_ms: float | None = Field(default=None, ge=0)
    cpu_pct: float | None = Field(default=None, ge=0, le=100)
    memory_pct: float | None = Field(default=None, ge=0, le=100)
    memory_mb: float | None = Field(default=None, ge=0)
    container_state: str | None = None  # running | exited | restarting | unhealthy | unknown
    database_healthy: bool | None = None
    database_active_connections: int | None = Field(default=None, ge=0)
    database_max_connections: int | None = Field(default=None, ge=1)
    notes: list[str] = Field(default_factory=list)


class ObserverEvent(ContractModel):
    schema_version: str = SCHEMA_VERSION
    event_id: str
    experiment_id: str
    run_id: str
    timestamp: datetime = Field(default_factory=utc_now)
    event_type: str  # health_changed | threshold_breached | fault_detected | recovery_verified | ...
    severity: EventSeverity
    service: str | None = None
    summary: str
    metric_name: str | None = None
    observed_value: float | str | bool | None = None
    threshold_value: float | str | bool | None = None
    evidence_refs: list[str] = Field(default_factory=list)  # log path, trace ID, sample ID, etc.
    safety_critical: bool = False


class MetricSummary(ContractModel):
    """Aggregate metrics for one phase; missing metrics remain null."""
    phase: ExperimentPhase
    sample_count: int = Field(ge=0)
    error_rate_pct: float | None = Field(default=None, ge=0, le=100)
    success_rate_pct: float | None = Field(default=None, ge=0, le=100)
    p95_latency_ms: float | None = Field(default=None, ge=0)
    peak_cpu_pct: float | None = Field(default=None, ge=0, le=100)
    peak_memory_pct: float | None = Field(default=None, ge=0, le=100)
    target_unhealthy_samples: int = Field(default=0, ge=0)
    database_unhealthy_samples: int = Field(default=0, ge=0)


class ObserverReport(ContractModel):
    """Evidence package passed to the Analyzer/Recommender Agent."""
    schema_version: str = SCHEMA_VERSION
    experiment_id: str
    run_id: str
    target_app: str
    target_profile: str
    fault_type: FaultType
    status: ExperimentStatus
    started_at: datetime
    finished_at: datetime | None = None
    baseline: MetricSummary
    during_fault: MetricSummary | None = None
    recovery: MetricSummary | None = None
    detection_latency_ms: float | None = Field(default=None, ge=0)
    recovery_time_ms: float | None = Field(default=None, ge=0)
    thresholds_breached: list[str] = Field(default_factory=list)
    recovery_status: RecoveryStatus = RecoveryStatus.NOT_STARTED
    recovery_verified: bool = False
    events: list[ObserverEvent] = Field(default_factory=list)
    evidence_refs: list[str] = Field(default_factory=list)
    observer_notes: list[str] = Field(default_factory=list)


class ObserverCommand(ContractModel):
    """Orchestrator -> Observer command; Observer remains read-only."""
    experiment_id: str
    run_id: str
    action: str  # start | mark_injection | mark_recovery | finalize | get_snapshot
    timestamp: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)
