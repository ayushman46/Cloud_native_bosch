from datetime import datetime, timezone
from enum import StrEnum
from typing import Any
from uuid import uuid4
from pydantic import BaseModel, ConfigDict, Field, model_validator


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class FaultType(StrEnum):
    application_crash = 'application_crash'
    network_latency = 'network_latency'
    service_restart = 'service_restart'
    traffic_spike = 'traffic_spike'


class Status(StrEnum):
    created = 'CREATED'
    planning = 'PLANNING'
    planned = 'PLANNED'
    validating = 'VALIDATING'
    approved = 'APPROVED'
    baselining = 'BASELINING'
    armed = 'ARMED'
    injecting = 'INJECTING'
    active = 'ACTIVE'
    rolling_back = 'ROLLING_BACK'
    verifying_recovery = 'VERIFYING_RECOVERY'
    completed = 'COMPLETED'
    rejected = 'REJECTED'
    failed = 'FAILED'
    aborted = 'ABORTED'
    recovery_failed = 'RECOVERY_FAILED'


TERMINAL = {Status.completed, Status.rejected, Status.failed, Status.aborted, Status.recovery_failed}


class PlanRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    goal: str = Field(min_length=8, max_length=1000)
    preferred_fault: FaultType | None = None
    preferred_target: str | None = None


class Parameters(BaseModel):
    model_config = ConfigDict(extra='forbid')
    duration_seconds: int = Field(default=10, ge=1)
    latency_ms: int | None = Field(default=None, ge=1)
    requests_per_second: int | None = Field(default=None, ge=1)
    observation_seconds: int = Field(default=10, ge=1)


class Plan(BaseModel):
    model_config = ConfigDict(extra='forbid')
    experiment_id: str = Field(default_factory=lambda: f'exp_{uuid4().hex}')
    application: str = 'openmeter'
    target_service: str
    failure_type: FaultType
    parameters: Parameters
    reason: str = Field(min_length=5)
    expected_impact: str = Field(min_length=5)
    required_observations: list[str] = Field(min_length=1)
    safety_requirements: list[str] = Field(default_factory=lambda: ['remote watchdog rollback', 'target allowlist'])

    @model_validator(mode='after')
    def compatible(self):
        if self.application != 'openmeter':
            raise ValueError('Only openmeter is supported')
        if self.failure_type == FaultType.network_latency and self.parameters.latency_ms is None:
            raise ValueError('network_latency requires latency_ms')
        if self.failure_type == FaultType.traffic_spike and self.parameters.requests_per_second is None:
            raise ValueError('traffic_spike requires requests_per_second')
        if self.failure_type != FaultType.network_latency and self.parameters.latency_ms is not None:
            raise ValueError('latency_ms only applies to network_latency')
        if self.failure_type != FaultType.traffic_spike and self.parameters.requests_per_second is not None:
            raise ValueError('requests_per_second only applies to traffic_spike')
        return self


class FaultEvidence(BaseModel):
    model_config = ConfigDict(extra='forbid')
    evidence_type: str = Field(min_length=3)
    target_service: str = Field(min_length=1)
    observed_at: datetime
    details: dict[str, Any] = Field(default_factory=dict)
    simulated: bool = False

    @model_validator(mode='after')
    def timestamp_is_utc(self):
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() != timezone.utc.utcoffset(self.observed_at):
            raise ValueError('Evidence timestamp must be timezone-aware UTC')
        return self


class RunRequest(PlanRequest):
    approved_plan: Plan | None = None


class ControllerStatus(BaseModel):
    experiment_id: str
    status: str
    watchdog_armed: bool = False
    injection_verified: bool = False
    rollback_verified: bool = False
    activation_evidence: FaultEvidence | None = None
    rollback_evidence: FaultEvidence | None = None
    fault_activated_at: datetime | None = None
    rollback_started_at: datetime | None = None
    rollback_completed_at: datetime | None = None
    message: str = ''

    @model_validator(mode='after')
    def verified_requires_evidence(self):
        if self.injection_verified and (self.activation_evidence is None or self.fault_activated_at is None):
            raise ValueError('Verified activation requires timestamp and evidence')
        if self.rollback_verified and (self.rollback_evidence is None or self.rollback_completed_at is None):
            raise ValueError('Verified rollback requires timestamp and evidence')
        for value in (self.fault_activated_at, self.rollback_started_at, self.rollback_completed_at):
            if value is not None and (value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value)):
                raise ValueError('Controller timestamps must be timezone-aware UTC')
        return self


class Probe(BaseModel):
    at: datetime = Field(default_factory=utcnow)
    phase: str
    endpoint: str
    reachable: bool
    status_code: int | None = None
    latency_ms: float | None = None
    error: str | None = None
    simulated: bool = False


class SyntheticEvent(BaseModel):
    at: datetime = Field(default_factory=utcnow)
    event_id: str
    phase: str
    accepted: bool
    status_code: int | None = None
    latency_ms: float | None = None
    error: str | None = None
    simulated: bool = False


class Report(BaseModel):
    diagnosis: str
    evidence: list[str]
    recommendations: list[str]
    metrics: dict[str, Any]
    analysis_source: str
    limitations: list[str] = []


class Experiment(BaseModel):
    experiment_id: str
    goal: str
    status: Status = Status.created
    plan: Plan | None = None
    controller: ControllerStatus | None = None
    probes: list[Probe] = []
    events: list[SyntheticEvent] = []
    metered_count: float | None = None
    meter_samples: list[dict[str, Any]] = Field(default_factory=list)
    report: Report | None = None
    error: str | None = None
    timestamps: dict[str, datetime] = {}
    history: list[dict[str, Any]] = Field(default_factory=list)
    simulated: bool = False
