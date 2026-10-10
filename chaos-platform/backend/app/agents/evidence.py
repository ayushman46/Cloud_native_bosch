"""Build the Analyzer's input from an ObservationReport. All numbers are rendered here by Python;
the LLM only cites them by id. This is the ONLY path into the diagnostic prompt, and it takes no
ground-truth object, so the injected fault label cannot leak in."""
from __future__ import annotations

from ..monitoring.dependency_map import KNOWN_SERVICES, OPENMETER_DEPENDENCY_MAP
from ..schemas.analysis import DependencyEdge, DiagnosticEvidence, EvidenceItem, LogEntry
from ..schemas.monitoring import ObservationReport, PhaseMetrics

_METRICS = [
    ("availability", "", 3),
    ("error_rate", "", 3),
    ("timeout_rate", "", 3),
    ("p95_latency_ms", " ms", 1),
    ("throughput_rps", " rps", 2),
    ("processing_lag_ms", " ms", 1),
    ("cpu_percent", " %", 1),
    ("memory_mb", " MB", 1),
]


def _fmt(v: float | None, unit: str, nd: int) -> str:
    return "unavailable" if v is None else f"{v:.{nd}f}{unit}"


def _compare(name: str, unit: str, nd: int, base: float | None, other: float | None, label: str) -> str:
    s = f"{name}: baseline {_fmt(base, unit, nd)} -> {label} {_fmt(other, unit, nd)}"
    if base not in (None, 0) and other is not None and name.endswith(("_ms", "_rps")):
        s += f" ({other / base:.1f}x)"
    return s


def build_evidence_items(r: ObservationReport) -> list[EvidenceItem]:
    items: list[EvidenceItem] = []

    def add(kind: str, statement: str) -> None:
        items.append(EvidenceItem(id=f"E{len(items) + 1}", kind=kind, statement=statement))  # type: ignore[arg-type]

    fault, rec_m = r.during_failure, r.recovery.metrics
    for name, unit, nd in _METRICS:
        base = getattr(r.baseline, name)
        if fault is not None:
            add("metric", _compare(name, unit, nd, base, getattr(fault, name), "fault window"))
        if rec_m is not None:
            add("metric", _compare(name, unit, nd, base, getattr(rec_m, name), "recovery window"))
        if fault is None and rec_m is None:
            add("metric", f"{name}: baseline {_fmt(base, unit, nd)}; no fault or recovery window was observed")
    services = sorted({*r.baseline.service_availability, *(fault.service_availability if fault else {}),
                       *(rec_m.service_availability if rec_m else {})})
    for svc in services:
        b = r.baseline.service_availability.get(svc)
        f = fault.service_availability.get(svc) if fault else None
        rc = rec_m.service_availability.get(svc) if rec_m else None
        add("health", f"health-probe availability of {svc}: baseline {_fmt(b, '', 3)}, fault window "
                      f"{_fmt(f, '', 3)}, recovery window {_fmt(rc, '', 3)}")
    fa = r.lifecycle.fault_activated_at
    for a in r.detection.anomalies[:12]:
        offset = f" (t{(a.detected_at - fa).total_seconds():+.2f}s from fault window start)" if fa else ""
        add("anomaly", f"{a.kind} on {a.metric} at {a.detected_at.isoformat()}{offset}: observed {a.observed:.3f}, "
                       f"threshold {a.threshold:.3f}, baseline {_fmt(a.baseline, '', 3)}")
    if len(r.detection.anomalies) > 12:
        add("note", f"{len(r.detection.anomalies) - 12} further anomalies omitted")
    if not r.detection.anomalies:
        add("anomaly", "no anomalies were flagged during the whole observation")
    d = r.detection
    if d.detection_success is None:
        add("anomaly", "fault detection is not applicable: no fault window was observed")
    else:
        add("anomaly", f"fault detection_success={d.detection_success}, detection_latency_ms={_fmt(d.detection_latency_ms, '', 1)}")
    rc = r.recovery
    if rc.functional_recovery_confirmed:
        add("recovery", f"functional recovery confirmed {rc.functional_recovery_seconds:.2f} s after fault removal")
    else:
        add("recovery", "functional recovery was NOT confirmed (or could not be measured)")
    add("accounting", f"events accepted={_fmt(rc.accepted_events, '', 0)}, missing_events={_fmt(rc.missing_events, '', 0)}, "
                      f"duplicate_events={_fmt(rc.duplicate_events, '', 0)}, metering_accuracy={_fmt(rc.metering_accuracy, '', 3)}, "
                      f"ambiguous-ingest events={_fmt(rc.uncertain_events, '', 0)}")
    for n in r.observer_notes:
        add("note", f"observer note: {n}")
    return items


def build_diagnostic_evidence(
    report: ObservationReport,
    logs: list[LogEntry] | None = None,
    dependency_map: list[DependencyEdge] | None = None,
    known_services: list[str] | None = None,
) -> DiagnosticEvidence:
    items = build_evidence_items(report)
    for entry in logs or []:
        items.append(EvidenceItem(id=f"E{len(items) + 1}", kind="log",
                                  statement=f"log[{entry.service or '?'}]: {entry.message}"))
    return DiagnosticEvidence(
        experiment_id=report.experiment_id,
        observation=report,
        evidence_items=items,
        logs=logs or [],
        dependency_map=dependency_map if dependency_map is not None else OPENMETER_DEPENDENCY_MAP,
        known_services=known_services if known_services is not None else KNOWN_SERVICES,
    )


def phase_has_data(m: PhaseMetrics | None) -> bool:
    return m is not None and any(
        getattr(m, n) is not None for n in ("availability", "error_rate", "p95_latency_ms", "throughput_rps", "processing_lag_ms")
    )
