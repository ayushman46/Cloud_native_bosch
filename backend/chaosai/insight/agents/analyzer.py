"""Analyzer Agent: evidence-grounded diagnosis through NVIDIA's hosted LLM.

Guarantees (each covered by tests):
* The prompt is a pure function of `DiagnosticEvidence` - no ground truth can reach it.
* The LLM never computes numbers: every figure it may cite is pre-rendered as an evidence item `E<n>`.
* Output is validated with Pydantic; claims without valid evidence refs (or quoting numbers that appear
  nowhere in the evidence) are dropped and counted, never passed through.
* Impact booleans are reconciled with hard facts from the Observation Report.
* If the LLM is missing/unreachable/invalid, the result says so (`unavailable` / `invalid_output`); no
  diagnosis is invented. If there is too little evidence, the report is an explicit `uncertain`.
* Nothing here can touch Docker or trigger rollback.
"""
from __future__ import annotations

import json
import re
import time

from pydantic import ValidationError

from ..clients.nvidia_client import LLMBackend, NvidiaError
from ..schemas.analysis import (
    ALLOWED_FAILURES,
    NO_FAULT,
    UNCERTAIN,
    AnalysisReport,
    AnalysisResult,
    AnalysisStatus,
    Diagnosis,
    DiagnosticEvidence,
    Grounding,
    Impact,
    LLMOutput,
    RecoveryAssessment,
)
from ..schemas.monitoring import ObservationReport
from .evidence import phase_has_data

SYSTEM_PROMPT = """You are an expert Site Reliability Engineer and distributed systems failure analyst.

You are analyzing telemetry collected from an isolated OpenMeter chaos engineering experiment.

Your task is to diagnose the most likely failure using only the provided observations, metrics, logs, and service dependency map.

Do not assume you know which failure was deliberately injected.

Distinguish observed facts from inferred explanations.

Do not fabricate measurements, log messages, timestamps, or recovery outcomes.

If evidence is insufficient, explicitly state uncertainty.

Identify the most likely affected service and failure category.

Explain which metrics changed relative to baseline.

Determine whether application availability, event processing, or metering correctness was affected.

Assess whether functional recovery was confirmed.

Recommend practical engineering improvements relevant to the observed failure.

Return a JSON object matching the required schema.

Rules for this task:
- Cite evidence ONLY by the ids in `evidence_items` (E1, E2, ...). Every claim must list at least one id in `refs`.
- Quote numbers only as they appear in the cited items. Never invent a metric, log line, or timestamp.
- `suspected_failure` must be exactly one of the allowed values; use "uncertain" when the evidence does not
  discriminate, and "no_fault_detected" when nothing abnormal was observed.
- `suspected_component` must be one of `known_services`, or "unknown".
- Treat "unavailable" metrics as unknown, not as zero or healthy.
- Prefix inferred explanations with "Inference:" in the claim text; observed facts need no prefix.
- Output a single JSON object, no markdown, no commentary."""

OUTPUT_SCHEMA_HINT = {
    "experiment_id": "<same as input>",
    "diagnosis": {
        "suspected_component": "<one of known_services or 'unknown'>",
        "suspected_failure": f"<one of {ALLOWED_FAILURES}>",
        "confidence": "<number 0..1, your estimate - not a calibrated probability>",
        "evidence": [{"claim": "<sentence>", "refs": ["E3", "E7"]}],
        "observed_vs_inferred_note": "<optional>",
    },
    "impact": {
        "availability_affected": "<bool>",
        "processing_delayed": "<bool>",
        "data_loss_detected": "<bool>",
        "explanation": "<optional>",
    },
    "recovery_assessment": "<optional sentence>",
    "recommendations": ["<practical, evidence-linked engineering improvements>"],
}


def build_user_prompt(ev: DiagnosticEvidence, feedback: str | None = None) -> str:
    payload = {
        "experiment_id": ev.experiment_id,
        "evidence_items": [i.model_dump() for i in ev.evidence_items],
        "observation_report": json.loads(ev.observation.model_dump_json()),
        "dependency_map": [e.model_dump() for e in ev.dependency_map],
        "known_services": ev.known_services,
        "allowed_failure_types": ALLOWED_FAILURES,
        "required_output_schema": OUTPUT_SCHEMA_HINT,
    }
    text = json.dumps(payload, indent=1, default=str)
    if feedback:
        text += f"\n\nYour previous answer was rejected: {feedback}\nReturn a corrected JSON object."
    return text


# ----------------------------------------------------------------- parsing & grounding
_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.I)
_NUM = re.compile(r"(?<![\w.])\d[\d,]*\.?\d*")


def parse_llm_json(text: str) -> LLMOutput:
    cleaned = _FENCE.sub("", text.strip())
    if not cleaned.startswith("{"):  # tolerate leading chatter: take the outermost object
        a, b = cleaned.find("{"), cleaned.rfind("}")
        if a == -1 or b <= a:
            raise ValueError("no JSON object found in model output")
        cleaned = cleaned[a : b + 1]
    return LLMOutput.model_validate_json(cleaned)


def _numbers(text: str) -> set[str]:
    out = set()
    for m in _NUM.findall(text):
        m = m.replace(",", "").rstrip(".")
        if m:
            out.add(m)
            try:
                out.add(f"{float(m):g}")
            except ValueError:
                pass
    return out


def check_grounding(out: LLMOutput, ev: DiagnosticEvidence) -> tuple[list[str], list[list[str]], Grounding]:
    by_id = {i.id: i.statement for i in ev.evidence_items}
    all_numbers = set().union(*(_numbers(s) for s in by_id.values())) if by_id else set()
    kept, refs, unsupported = [], [], []
    for c in out.diagnosis.evidence:
        valid = [r for r in c.refs if r in by_id]
        problem = None
        if not valid:
            problem = "no valid evidence reference"
        else:
            cited_numbers = set().union(*(_numbers(by_id[r]) for r in valid))
            # multi-digit numbers must exist in the cited items (or at least in the evidence at all)
            stray = [n for n in _numbers(c.claim) if len(n.replace(".", "")) >= 2 and n not in cited_numbers
                     and n not in all_numbers]
            if stray:
                problem = f"quotes numbers not present in the evidence: {sorted(stray)}"
        if problem:
            unsupported.append(f"{c.claim} [{problem}]")
        else:
            kept.append(c.claim)
            refs.append(valid)
    total = len(out.diagnosis.evidence)
    grounding = Grounding(total_claims=total, supported_claims=len(kept),
                          grounding_rate=(len(kept) / total) if total else None, unsupported_claims=unsupported)
    return kept, refs, grounding


# ----------------------------------------------------------------- hard facts
def derive_facts(r: ObservationReport) -> dict[str, bool | None]:
    """Facts computed by Python from the report; used to correct contradictory LLM impact flags."""
    f = r.during_failure
    availability = None
    if f is not None and (f.availability is not None or f.error_rate is not None):
        availability = bool((f.availability is not None and f.availability < 0.95)
                            or (f.error_rate is not None and f.error_rate > 0.20))
    lag_kinds = {a.kind for a in r.detection.anomalies}
    delayed = None
    if "HIGH_PROCESSING_LAG" in lag_kinds:
        delayed = True
    elif f is not None and f.processing_lag_ms is not None:
        delayed = False
    loss = None
    rc = r.recovery
    if rc.missing_events is not None and rc.duplicate_events is not None:
        loss = rc.missing_events > 0 or rc.duplicate_events > 0
    return {"availability_affected": availability, "processing_delayed": delayed, "data_loss_detected": loss}


def reconcile_impact(llm_impact, facts: dict[str, bool | None]) -> tuple[Impact, list[str]]:
    notes: list[str] = []
    values: dict[str, bool | None] = {}
    for key in ("availability_affected", "processing_delayed", "data_loss_detected"):
        claimed, fact = getattr(llm_impact, key), facts[key]
        if fact is not None and claimed != fact:
            notes.append(f"impact.{key}: model said {claimed}, measured facts say {fact}; using measured value")
            values[key] = fact
        else:
            values[key] = claimed if fact is None else fact
    return Impact(**values), notes


def recovery_from_report(r: ObservationReport, text: str | None) -> RecoveryAssessment:
    return RecoveryAssessment(
        functional_recovery_confirmed=r.recovery.functional_recovery_confirmed,
        functional_recovery_seconds=r.recovery.functional_recovery_seconds,
        assessment=text,
    )


def insufficient_evidence_reason(r: ObservationReport) -> str | None:
    if not phase_has_data(r.baseline):
        return "no usable baseline metrics were collected"
    if r.during_failure is None and r.detection.detection_success is None and not r.detection.anomalies:
        # A healthy control is analysable (answer: no fault); but without any fault window AND without
        # any recovery/health data there is nothing to compare.
        if r.baseline.request_count == 0 and r.baseline.health_probe_count == 0:
            return "no observations in any phase"
    if r.during_failure is not None and not phase_has_data(r.during_failure):
        return "the fault window contains no usable metrics"
    return None


def uncertain_report(ev: DiagnosticEvidence, reason: str, latency_ms: float | None = None) -> AnalysisReport:
    r = ev.observation
    return AnalysisReport(
        experiment_id=ev.experiment_id,
        diagnosis=Diagnosis(suspected_component="unknown", suspected_failure=UNCERTAIN, confidence=0.0,
                            evidence=[], evidence_refs=[]),
        impact=Impact(availability_affected=None, processing_delayed=None, data_loss_detected=None),
        recovery=recovery_from_report(r, None),
        recommendations=[f"Collect more observations before diagnosing: {reason}."],
        grounding=Grounding(total_claims=0, supported_claims=0, grounding_rate=None),
        validation_notes=[f"insufficient evidence: {reason}"],
        generation_latency_ms=latency_ms,
    )


# ----------------------------------------------------------------- agent
class AnalyzerAgent:
    def __init__(self, llm: LLMBackend | None, max_validation_retries: int = 1) -> None:
        self.llm = llm
        self.max_validation_retries = max_validation_retries

    async def analyze(self, ev: DiagnosticEvidence) -> AnalysisResult:
        t0 = time.perf_counter()
        ms = lambda: (time.perf_counter() - t0) * 1000  # noqa: E731

        reason = insufficient_evidence_reason(ev.observation)
        if reason:
            return AnalysisResult(experiment_id=ev.experiment_id, status=AnalysisStatus.INSUFFICIENT_EVIDENCE,
                                  report=uncertain_report(ev, reason, ms()), attempts=0)
        if self.llm is None:
            return AnalysisResult(experiment_id=ev.experiment_id, status=AnalysisStatus.UNAVAILABLE,
                                  error="no LLM configured (set NVIDIA_API_KEY); analysis pending", attempts=0)

        feedback: str | None = None
        last_error = "unknown"
        for attempt in range(1, self.max_validation_retries + 2):
            try:
                raw = await self.llm.complete(SYSTEM_PROMPT, build_user_prompt(ev, feedback))
            except NvidiaError as exc:
                return AnalysisResult(experiment_id=ev.experiment_id, status=AnalysisStatus.UNAVAILABLE,
                                      error=f"{exc.kind}: {exc}", attempts=attempt)
            try:
                report = self._validate(ev, raw, ms)
            except (ValueError, ValidationError) as exc:  # includes JSON errors and schema violations
                last_error = str(exc).splitlines()[0][:300] if str(exc) else type(exc).__name__
                feedback = last_error
                continue
            return AnalysisResult(experiment_id=ev.experiment_id, status=AnalysisStatus.COMPLETED,
                                  report=report, attempts=attempt)
        return AnalysisResult(experiment_id=ev.experiment_id, status=AnalysisStatus.INVALID_OUTPUT,
                              error=f"model output failed validation: {last_error}",
                              attempts=self.max_validation_retries + 1)

    def _validate(self, ev: DiagnosticEvidence, raw: str, ms) -> AnalysisReport:
        out = parse_llm_json(raw)
        if out.experiment_id != ev.experiment_id:
            raise ValueError(f"experiment_id mismatch: {out.experiment_id!r} != {ev.experiment_id!r}")
        d = out.diagnosis
        if d.suspected_failure not in ALLOWED_FAILURES:
            raise ValueError(f"suspected_failure {d.suspected_failure!r} not in {ALLOWED_FAILURES}")
        if d.suspected_component != "unknown" and d.suspected_component not in ev.known_services:
            raise ValueError(f"suspected_component {d.suspected_component!r} not in known_services")
        if not out.recommendations:
            raise ValueError("recommendations must not be empty")

        notes: list[str] = []
        kept, refs, grounding = check_grounding(out, ev)
        failure, component, confidence = d.suspected_failure, d.suspected_component, d.confidence
        if not kept and failure not in (UNCERTAIN, NO_FAULT):
            notes.append("diagnosis downgraded to 'uncertain': none of the model's claims were grounded in evidence")
            failure, component, confidence = UNCERTAIN, "unknown", 0.0
        elif grounding.unsupported_claims:
            notes.append(f"{len(grounding.unsupported_claims)} unsupported claim(s) removed")
            confidence = min(confidence, 0.6)

        impact, impact_notes = reconcile_impact(out.impact, derive_facts(ev.observation))
        notes += impact_notes
        recs = list(dict.fromkeys(r.strip() for r in out.recommendations if r.strip()))[:8]
        return AnalysisReport(
            experiment_id=ev.experiment_id,
            diagnosis=Diagnosis(suspected_component=component, suspected_failure=failure, confidence=confidence,
                                evidence=kept, evidence_refs=refs),
            impact=impact,
            recovery=recovery_from_report(ev.observation, out.recovery_assessment),
            recommendations=recs,
            grounding=grounding,
            validation_notes=notes,
            generation_latency_ms=ms(),
        )
