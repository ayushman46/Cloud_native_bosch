"""Deterministic, baseline-aware anomaly detection. The Observer never asks an LLM whether something broke.

Rules (all thresholds configurable via `DetectionConfig`):

* CONSECUTIVE_PROBE_FAILURES  N back-to-back infrastructure failures of the same probe stream
                              (health or ingest). Fastest signal for crashes.
* HIGH_ERROR_RATE             window error_rate > threshold, only with >= min_requests in the window.
* LOW_AVAILABILITY            window availability < threshold, only with >= min_health_probes.
* HIGH_P95_LATENCY            window p95 > max(floor, factor x baseline p95)   (baseline-aware)
* HIGH_PROCESSING_LAG         window lag  > max(floor, factor x baseline lag)  (baseline-aware)

Metrics that are None (no data) are skipped - never treated as zero or as healthy.
`detected_at` is the moment the rule could first have fired (completion of the N-th failed probe, or the
end of the sample window), so detection latency is not understated.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from ..schemas.monitoring import Anomaly, DetectionResult, MonitoringSample, PhaseMetrics
from .metrics import Lifecycle
from .records import ProbeRecord


@dataclass(frozen=True)
class DetectionConfig:
    consecutive_failures: int = 3
    error_rate_threshold: float = 0.20
    min_requests: int = 5
    availability_threshold: float = 0.80
    min_health_probes: int = 2
    latency_factor: float = 3.0
    latency_floor_ms: float = 1000.0
    lag_factor: float = 3.0
    lag_floor_ms: float = 3000.0
    detection_grace_seconds: float = 30.0  # anomalies may surface shortly after the fault window ends


def _dyn(floor: float, factor: float, baseline: float | None) -> float:
    return max(floor, factor * baseline) if baseline is not None else floor


def probe_anomalies(records: list[ProbeRecord], primary_service: str, cfg: DetectionConfig) -> list[Anomaly]:
    out: list[Anomaly] = []
    streams = {"ingest": [r for r in records if r.kind == "ingest"]}
    for svc in sorted({r.service for r in records if r.kind == "health"} | {primary_service}):
        streams[f"health:{svc}"] = [r for r in records if r.kind == "health" and r.service == svc]
    for name, recs in streams.items():
        run, fired = 0, False
        for r in sorted(recs, key=lambda r: r.completed_at):
            if r.is_infra_failure:
                run += 1
                if run >= cfg.consecutive_failures and not fired:
                    fired = True
                    out.append(
                        Anomaly(
                            kind="CONSECUTIVE_PROBE_FAILURES",
                            detected_at=r.completed_at,
                            metric=f"{name}_probe_consecutive_failures",
                            observed=float(run),
                            threshold=float(cfg.consecutive_failures),
                            evidence_ref=f"probe#{r.seq}",
                        )
                    )
            elif r.ok:
                run, fired = 0, False
    return out


def sample_anomalies(samples: list[MonitoringSample], baseline: PhaseMetrics, cfg: DetectionConfig) -> list[Anomaly]:
    out: list[Anomaly] = []
    lat_thr = _dyn(cfg.latency_floor_ms, cfg.latency_factor, baseline.p95_latency_ms)
    lag_thr = _dyn(cfg.lag_floor_ms, cfg.lag_factor, baseline.processing_lag_ms)
    for s in samples:
        m, ref = s.metrics, f"sample@{s.timestamp.isoformat()}"
        if m.error_rate is not None and s.request_count >= cfg.min_requests and m.error_rate > cfg.error_rate_threshold:
            out.append(Anomaly(kind="HIGH_ERROR_RATE", detected_at=s.timestamp, metric="error_rate",
                               observed=m.error_rate, threshold=cfg.error_rate_threshold,
                               baseline=baseline.error_rate, evidence_ref=ref))
        if (m.availability is not None and s.health_probe_count >= cfg.min_health_probes
                and m.availability < cfg.availability_threshold):
            out.append(Anomaly(kind="LOW_AVAILABILITY", detected_at=s.timestamp, metric="availability",
                               observed=m.availability, threshold=cfg.availability_threshold,
                               baseline=baseline.availability, evidence_ref=ref))
        if m.p95_latency_ms is not None and m.p95_latency_ms > lat_thr:
            out.append(Anomaly(kind="HIGH_P95_LATENCY", detected_at=s.timestamp, metric="p95_latency_ms",
                               observed=m.p95_latency_ms, threshold=lat_thr,
                               baseline=baseline.p95_latency_ms, evidence_ref=ref))
        if m.processing_lag_ms is not None and m.processing_lag_ms > lag_thr:
            out.append(Anomaly(kind="HIGH_PROCESSING_LAG", detected_at=s.timestamp, metric="processing_lag_ms",
                               observed=m.processing_lag_ms, threshold=lag_thr,
                               baseline=baseline.processing_lag_ms, evidence_ref=ref))
    return out


def detect(
    records: list[ProbeRecord],
    samples: list[MonitoringSample],
    baseline: PhaseMetrics,
    lifecycle: Lifecycle,
    primary_service: str,
    cfg: DetectionConfig | None = None,
) -> DetectionResult:
    cfg = cfg or DetectionConfig()
    anomalies = sorted(
        probe_anomalies(records, primary_service, cfg) + sample_anomalies(samples, baseline, cfg),
        key=lambda a: a.detected_at,
    )
    fa = lifecycle.fault_activated_at
    if fa is None:  # healthy control or injection never activated: everything flagged is a false positive
        return DetectionResult(detection_success=None, anomalies=anomalies, pre_fault_anomalies=len(anomalies))
    pre = [a for a in anomalies if a.detected_at < fa]
    horizon: datetime | None = None
    if lifecycle.rollback_completed_at is not None:
        horizon = lifecycle.rollback_completed_at + timedelta(seconds=cfg.detection_grace_seconds)
    in_fault = [a for a in anomalies if a.detected_at >= fa and (horizon is None or a.detected_at <= horizon)]
    if in_fault:
        first = in_fault[0]
        return DetectionResult(
            detection_success=True,
            detection_latency_ms=(first.detected_at - fa).total_seconds() * 1000,
            first_anomaly=first.kind,
            anomalies=anomalies,
            pre_fault_anomalies=len(pre),
        )
    return DetectionResult(detection_success=False, anomalies=anomalies, pre_fault_anomalies=len(pre))


def flagged_windows(samples: list[MonitoringSample], anomalies: list[Anomaly], window_seconds: float) -> int:
    """Number of sample windows containing at least one anomaly (used for the false-positive rate)."""
    flagged = 0
    for s in samples:
        start = s.timestamp - timedelta(seconds=window_seconds)
        if any(start < a.detected_at <= s.timestamp for a in anomalies):
            flagged += 1
    return flagged
