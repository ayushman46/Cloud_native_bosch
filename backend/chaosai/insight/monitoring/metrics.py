"""Deterministic metric calculations. No LLM, no I/O, no randomness.

Definitions
-----------
availability   successful health probes / scheduled health probes for the primary service.
error_rate     infrastructure-failed ingest requests / attempted ingest requests (see records.py for what
               counts as a failure; unexpected 4xx are excluded).
timeout_rate   timed-out ingest requests / attempted ingest requests.
p95_latency_ms nearest-rank 95th percentile of ingest request durations over requests that got a response
               or timed out (timeouts enter at their elapsed time, i.e. right-censored). Fast local
               failures (refused / DNS / reset) are excluded. Needs >= `min_latency_samples` requests,
               otherwise None - never computed from one request.
throughput_rps successful ingest requests in the window / window length in seconds.
processing_lag_ms  p95 (phase) or max (sample window) of per-event lag between acceptance and meter
               visibility; still-invisible events contribute their current age (lower bound).
cpu/memory     peak over the window from the optional resource probe; None when no probe exists.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

from ..schemas.monitoring import MonitoringSample, Phase, PhaseMetrics, SampleMetrics
from .meter_probes import EventLag
from .records import ProbeRecord


@dataclass(frozen=True)
class MetricsConfig:
    min_latency_samples: int = 5
    min_lag_samples: int = 3
    sample_window_seconds: float = 2.0
    percentile: float = 95.0


@dataclass(frozen=True)
class ResourceSample:
    at: datetime
    cpu_percent: float | None
    memory_mb: float | None


@dataclass
class Lifecycle:
    """Injection timeline as reported by the Injector (not guessed)."""

    monitoring_started_at: datetime
    fault_activated_at: datetime | None = None
    rollback_started_at: datetime | None = None
    rollback_completed_at: datetime | None = None
    injection_success: bool | None = None
    finished_at: datetime | None = None

    def phase_at(self, ts: datetime) -> Phase:
        if self.fault_activated_at is None or ts < self.fault_activated_at:
            return Phase.BASELINE
        if self.rollback_completed_at is None or ts < self.rollback_completed_at:
            return Phase.FAULT
        return Phase.RECOVERY

    def windows(self, as_of: datetime) -> dict[Phase, tuple[datetime, datetime] | None]:
        end = self.finished_at or as_of
        fa, rc = self.fault_activated_at, self.rollback_completed_at
        base_end = min(fa, end) if fa else end
        out: dict[Phase, tuple[datetime, datetime] | None] = {
            Phase.BASELINE: (self.monitoring_started_at, base_end),
            Phase.FAULT: None,
            Phase.RECOVERY: None,
        }
        if fa and fa < end:
            out[Phase.FAULT] = (fa, min(rc, end) if rc else end)
        if rc and rc < end:
            out[Phase.RECOVERY] = (rc, end)
        return out


# ------------------------------------------------------------- primitives
def percentile_nearest_rank(values: Iterable[float], p: float = 95.0) -> float | None:
    """Nearest-rank percentile: the ceil(p/100 * n)-th smallest value (1-based). None for empty input."""
    xs = sorted(values)
    if not xs:
        return None
    rank = max(1, math.ceil(p / 100.0 * len(xs)))
    return xs[rank - 1]


def in_window(ts: datetime, start: datetime, end: datetime) -> bool:
    return start <= ts < end


def select(records: Iterable[ProbeRecord], start: datetime, end: datetime, *, kind: str | None = None,
           service: str | None = None) -> list[ProbeRecord]:
    return [
        r for r in records
        if in_window(r.completed_at, start, end)
        and (kind is None or r.kind == kind)
        and (service is None or r.service == service)
    ]


def availability(health: list[ProbeRecord]) -> float | None:
    return sum(r.ok for r in health) / len(health) if health else None


def error_rate(ingest: list[ProbeRecord]) -> float | None:
    return sum(r.is_infra_failure for r in ingest) / len(ingest) if ingest else None


def timeout_rate(ingest: list[ProbeRecord]) -> float | None:
    return sum(r.is_timeout for r in ingest) / len(ingest) if ingest else None


def p95_latency_ms(ingest: list[ProbeRecord], cfg: MetricsConfig) -> float | None:
    durations = [r.duration_ms for r in ingest if r.has_latency_signal]
    if len(durations) < cfg.min_latency_samples:
        return None
    return percentile_nearest_rank(durations, cfg.percentile)


def throughput_rps(ingest: list[ProbeRecord], seconds: float) -> float | None:
    if seconds <= 0 or not ingest:
        return None
    return sum(r.ok for r in ingest) / seconds


def peak(values: Iterable[float | None]) -> float | None:
    vals = [v for v in values if v is not None]
    return max(vals) if vals else None


# --------------------------------------------------------------- phases
def phase_metrics(
    records: list[ProbeRecord],
    lags: list[EventLag],
    resources: list[ResourceSample],
    start: datetime,
    end: datetime,
    primary_service: str,
    cfg: MetricsConfig,
) -> PhaseMetrics:
    seconds = max(0.0, (end - start).total_seconds())
    primary_health = select(records, start, end, kind="health", service=primary_service)
    ingest = select(records, start, end, kind="ingest")
    other_services = sorted({r.service for r in records if r.kind == "health" and r.service != primary_service})
    service_av = {
        s: availability(select(records, start, end, kind="health", service=s)) for s in other_services
    }
    phase_lags = [l.lag_ms for l in lags if in_window(l.event.accepted_at, start, end)]
    lag = percentile_nearest_rank(phase_lags, cfg.percentile) if len(phase_lags) >= cfg.min_lag_samples else None
    res = [r for r in resources if in_window(r.at, start, end)]
    return PhaseMetrics(
        availability=availability(primary_health),
        error_rate=error_rate(ingest),
        timeout_rate=timeout_rate(ingest),
        p95_latency_ms=p95_latency_ms(ingest, cfg),
        throughput_rps=throughput_rps(ingest, seconds),
        processing_lag_ms=lag,
        cpu_percent=peak(r.cpu_percent for r in res),
        memory_mb=peak(r.memory_mb for r in res),
        request_count=len(ingest),
        health_probe_count=len(primary_health),
        duration_seconds=seconds,
        service_availability=service_av,
    )


def build_samples(
    experiment_id: str,
    target_service: str | None,
    records: list[ProbeRecord],
    lags: list[EventLag],
    resources: list[ResourceSample],
    lifecycle: Lifecycle,
    as_of: datetime,
    primary_service: str,
    cfg: MetricsConfig,
) -> list[MonitoringSample]:
    """Fixed windows of `sample_window_seconds` from monitoring start; each window is tagged with the
    phase at its END using the Injector's timestamps. Metrics with no data stay None."""
    end_all = lifecycle.finished_at or as_of
    step = timedelta(seconds=cfg.sample_window_seconds)
    samples: list[MonitoringSample] = []
    ws = lifecycle.monitoring_started_at
    while ws + step <= end_all:
        we = ws + step
        health = select(records, ws, we, kind="health", service=primary_service)
        ingest = select(records, ws, we, kind="ingest")
        # lag: events that became visible in the window, or were still pending at its end
        window_lags = []
        for l in lags:
            if l.event.accepted_at >= we:
                continue
            if l.visible_at is not None and l.visible_at < we:
                if l.visible_at >= ws:
                    window_lags.append(l.lag_ms)  # became visible inside this window
            elif l.visible_at is not None or (l.observed_until is not None and l.observed_until >= we):
                # certainly still pending at the window end (a later meter read lacked it or showed it)
                window_lags.append((we - l.event.accepted_at).total_seconds() * 1000)
        res = [r for r in resources if in_window(r.at, ws, we)]
        samples.append(
            MonitoringSample(
                experiment_id=experiment_id,
                timestamp=we,
                phase=lifecycle.phase_at(we - timedelta(microseconds=1)),
                target_service=target_service,
                metrics=SampleMetrics(
                    availability=availability(health),
                    error_rate=error_rate(ingest),
                    p95_latency_ms=p95_latency_ms(ingest, cfg),
                    throughput_rps=throughput_rps(ingest, cfg.sample_window_seconds),
                    processing_lag_ms=max(window_lags) if window_lags else None,
                    cpu_percent=peak(r.cpu_percent for r in res),
                    memory_mb=peak(r.memory_mb for r in res),
                ),
                request_count=len(ingest),
                health_probe_count=len(health),
            )
        )
        ws = we
    return samples


def monitoring_coverage(m: PhaseMetrics | None, required: tuple[str, ...]) -> float | None:
    """Observed required metrics / configured required metrics."""
    if m is None or not required:
        return None
    return sum(getattr(m, name) is not None for name in required) / len(required)
