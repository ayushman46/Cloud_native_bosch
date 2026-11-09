"""Analyzer contracts: diagnostic evidence in, AnalysisReport out; ground truth kept apart."""
from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from .injection import FailureType
from .monitoring import ObservationReport

UNCERTAIN = "uncertain"
NO_FAULT = "no_fault_detected"  # extension: the right answer for a healthy control
ALLOWED_FAILURES = [f.value for f in FailureType] + [UNCERTAIN, NO_FAULT]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


# ---------------------------------------------------------------- inputs
class ExecutionGroundTruth(_Model):
    """What was really injected. NEVER sent to the Analyzer; used only for scoring afterwards."""

    experiment_id: str
    failure_type: FailureType | None  # None = healthy control experiment (no fault injected)
    target_service: str | None = None
    fault_activated_at: AwareDatetime | None = None
    actual_parameters: dict = Field(default_factory=dict)


class EvidenceItem(_Model):
    id: str  # E1, E2, ... the only identifiers the LLM may cite
    kind: Literal["metric", "anomaly", "health", "recovery", "accounting", "log", "note"]
    statement: str  # fully rendered, computed by Python from the ObservationReport


class LogEntry(_Model):
    timestamp: AwareDatetime | None = None
    service: str | None = None
    message: str


class DependencyEdge(_Model):
    source: str
    target: str
    relation: str  # e.g. "produces_to", "consumes_from", "writes_to", "dedupes_via"


class DiagnosticEvidence(_Model):
    """The only object handed to the Analyzer prompt. Contains no injected-fault label."""

    experiment_id: str
    observation: ObservationReport
    evidence_items: list[EvidenceItem]
    logs: list[LogEntry] = Field(default_factory=list)
    dependency_map: list[DependencyEdge] = Field(default_factory=list)
    known_services: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------- LLM raw output
class LLMClaim(_Model):
    claim: str = Field(min_length=3)
    refs: list[str] = Field(default_factory=list)  # evidence item ids


class LLMDiagnosis(_Model):
    suspected_component: str
    suspected_failure: str  # validated against FailureType | "uncertain" by the Analyzer
    confidence: float = Field(ge=0, le=1)
    evidence: list[LLMClaim]
    observed_vs_inferred_note: str | None = None


class LLMImpact(_Model):
    availability_affected: bool
    processing_delayed: bool
    data_loss_detected: bool
    explanation: str | None = None


class LLMOutput(_Model):
    experiment_id: str
    diagnosis: LLMDiagnosis
    impact: LLMImpact
    recovery_assessment: str | None = None
    recommendations: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------- final report
class Diagnosis(_Model):
    suspected_component: str
    suspected_failure: str  # one of FailureType values or "uncertain"
    confidence: float = Field(ge=0, le=1)  # model estimate, NOT a calibrated probability
    evidence: list[str]
    evidence_refs: list[list[str]] = Field(default_factory=list)  # parallel to `evidence`

    @field_validator("suspected_failure")
    @classmethod
    def _known_failure(cls, v: str) -> str:
        if v not in ALLOWED_FAILURES:
            raise ValueError(f"suspected_failure must be one of {ALLOWED_FAILURES}, got {v!r}")
        return v


class Impact(_Model):
    availability_affected: bool | None
    processing_delayed: bool | None
    data_loss_detected: bool | None


class RecoveryAssessment(_Model):
    functional_recovery_confirmed: bool
    functional_recovery_seconds: float | None = None
    assessment: str | None = None


class Grounding(_Model):
    total_claims: int = Field(ge=0)
    supported_claims: int = Field(ge=0)
    grounding_rate: float | None = Field(default=None, ge=0, le=1)
    unsupported_claims: list[str] = Field(default_factory=list)


class AnalysisReport(_Model):
    experiment_id: str
    diagnosis: Diagnosis
    impact: Impact
    recovery: RecoveryAssessment
    recommendations: list[str]
    # additions
    grounding: Grounding
    validation_notes: list[str] = Field(default_factory=list)
    generation_latency_ms: float | None = Field(default=None, ge=0)


class AnalysisStatus(str, Enum):
    COMPLETED = "completed"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    UNAVAILABLE = "unavailable"  # LLM not configured / unreachable after bounded retries
    INVALID_OUTPUT = "invalid_output"


class AnalysisResult(_Model):
    """Envelope stored and returned by the API; `report` is None unless a real analysis exists
    (an `insufficient_evidence` result carries an explicit `uncertain` report, never a guess)."""

    experiment_id: str
    status: AnalysisStatus
    report: AnalysisReport | None = None
    error: str | None = None
    attempts: int = 0


class AnalysisRequest(_Model):
    experiment_id: str
    observation_report: ObservationReport | None = None  # defaults to the stored report


class GroundTruthEvaluation(_Model):
    experiment_id: str
    failure_correct: bool
    component_correct: bool
    predicted_failure: str
    predicted_component: str
