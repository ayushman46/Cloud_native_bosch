"""Continuous experiment monitor: baseline -> fault -> recovery, driven by the Injector's timestamps.

The monitor keeps probing for the whole experiment (health, ingestion, meter query, optional resource
sampler). Phases are NOT decided by when we happen to look: every record is stored with its own
timestamp and phases are derived from `fault_activated_at` / `rollback_completed_at` supplied by the
Injector. No LLM and no network access other than OpenMeter is involved, so it keeps working when
NVIDIA is down.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Awaitable, Callable

from ..clients.openmeter_client import OpenMeterClient
from ..schemas.injection import InjectionEvent
from ..schemas.monitoring import (
    LifecycleTimes,
    MonitoringSample,
    ObservationReport,
    Phase,
    PhaseMetrics,
    RecoverySummary,
)
from .detection import DetectionConfig, detect
from .http_probes import build_cloudevent, new_event_id, run_health_probe, run_ingest_probe
from .meter_probes import EventLedger, poll_meter_once
from .metrics import (
    Lifecycle,
    MetricsConfig,
    ResourceSample,
    build_samples,
    monitoring_coverage,
    phase_metrics,
)
from .records import Clock, Outcome, ProbeRecord, SystemClock

log = logging.getLogger(__name__)

ResourceProbe = Callable[[], Awaitable[tuple[float | None, float | None]]]  # -> (cpu_percent, memory_mb)

REQUIRED_METRICS = ("availability", "error_rate", "p95_latency_ms", "throughput_rps", "processing_lag_ms")


@dataclass
class MonitorConfig:
    primary_service: str = "openmeter-api"
    health_targets: dict[str, str] = field(default_factory=dict)  # name -> full URL; must include primary
    health_interval_s: float = 0.5
    ingest_interval_s: float = 0.2
    meter_poll_interval_s: float = 0.5
    meter_slug: str = "api_requests_total"
    event_type: str = "request"
    subject_prefix: str = "chaos-"
    value_per_event: float = 1.0
    recovery_confirm_probes: int = 3
    post_recovery_grace_s: float = 5.0
    recovery_deadline_s: float = 60.0
    drain_deadline_s: float = 30.0
    min_baseline_seconds: float = 10.0
    min_baseline_requests: int = 10
    max_runtime_s: float = 900.0
    clock_skew_tolerance_s: float = 1.0
    max_inflight: int = 200
    metrics: MetricsConfig = field(default_factory=MetricsConfig)
    detection: DetectionConfig = field(default_factory=DetectionConfig)


def _kth_ok_completion(recs: list[ProbeRecord], after: datetime, k: int) -> datetime | None:
    """Completion time of the k-th consecutive OK probe among probes that STARTED at/after `after`."""
    run = 0
    for r in sorted((r for r in recs if r.started_at >= after), key=lambda r: r.completed_at):
        run = run + 1 if r.ok else 0
        if run >= k:
            return r.completed_at
    return None


def functional_recovery_time(
    records: list[ProbeRecord], ledger: EventLedger, ref: datetime, cfg: MonitorConfig, meter_enabled: bool = True
) -> datetime | None:
    """Moment functionality is confirmed after `ref` (fault removal / restart completion), or None.

    Requires ALL of: k consecutive OK probes of every configured health target; k consecutive accepted
    ingest requests; and (if metering is monitored) an event accepted after `ref` is visible in the meter,
    i.e. the processing backlog drained. A container merely being 'up' is never enough."""
    k = cfg.recovery_confirm_probes
    times: list[datetime] = []
    for name in sorted({r.service for r in records if r.kind == "health"} | {cfg.primary_service}):
        t = _kth_ok_completion([r for r in records if r.kind == "health" and r.service == name], ref, k)
        if t is None:
            return None
        times.append(t)
    t_ingest = _kth_ok_completion([r for r in records if r.kind == "ingest"], ref, k)
    if t_ingest is None:
        return None
    times.append(t_ingest)
    if meter_enabled:
        t_meter = ledger.caught_up_after(ref)
        if t_meter is None:
            return None
        times.append(t_meter)
    return max(times)


class ExperimentMonitor:
    def __init__(
        self,
        experiment_id: str,
        client: OpenMeterClient,
        config: MonitorConfig,
        clock: Clock | None = None,
        resource_probe: ResourceProbe | None = None,
        target_service: str | None = None,
    ) -> None:
        self.experiment_id = experiment_id
        self.client = client
        self.cfg = config
        self.clock = clock or SystemClock()
        self.resource_probe = resource_probe
        self.target_service = target_service
        if config.primary_service not in config.health_targets:
            raise ValueError("health_targets must include the primary service")
        self.subject = f"{config.subject_prefix}{experiment_id}"
        self.ledger = EventLedger(config.value_per_event)
        self.records: list[ProbeRecord] = []
        self.resources: list[ResourceSample] = []
        self.notes: list[str] = []
        self.lifecycle: Lifecycle | None = None
        self.state = "created"  # created | running | finalizing | finalized
        self.report: ObservationReport | None = None
        self._seq = 0
        self._tasks: list[asyncio.Task] = []
        self._inflight: set[asyncio.Task] = set()
        self._sending = asyncio.Event()
        self._done = asyncio.Event()
        self._finalize_lock = asyncio.Lock()
        self._skipped_ticks = 0

    # ------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        if self.state != "created":
            return
        now = self.clock.now()
        self.lifecycle = Lifecycle(monitoring_started_at=now)
        self.state = "running"
        self._sending.set()
        loops = [self._health_loop(), self._ingest_loop(), self._meter_loop(), self._supervisor()]
        if self.resource_probe:
            loops.append(self._resource_loop())
        self._tasks = [asyncio.create_task(c) for c in loops]

    def apply_event(self, event: InjectionEvent) -> None:
        """Merge an Injector lifecycle event. Idempotent; timestamps are taken as given (clamped if they
        are in the future beyond the skew tolerance)."""
        lc = self.lifecycle
        if lc is None:
            raise RuntimeError("monitor not started")
        if event.target_service:
            self.target_service = event.target_service
        if event.injection_success is False:
            lc.injection_success = False
            self._note("Injector reported injection_success=false; no fault window is assumed.")
        elif event.injection_success is True and lc.injection_success is None:
            lc.injection_success = True
        now = self.clock.now()
        ts = event.timestamps
        for name in ("fault_activated_at", "rollback_started_at", "rollback_completed_at"):
            value = getattr(ts, name)
            if value is None or getattr(lc, name) is not None:
                continue
            if value > now + timedelta(seconds=self.cfg.clock_skew_tolerance_s):
                self._note(f"{name} {value.isoformat()} is in the future vs observer clock; clamped to receipt time (clock skew?).")
                value = now
            if name == "fault_activated_at":
                if event.injection_success is False:
                    continue
                if value < lc.monitoring_started_at:
                    self._note("fault_activated_at precedes monitoring start: no baseline window exists.")
            elif lc.fault_activated_at is not None and value < lc.fault_activated_at:
                self._note(f"{name} earlier than fault_activated_at; ignored.")
                continue
            setattr(lc, name, value)

    async def finalize(self) -> ObservationReport:
        async with self._finalize_lock:
            if self.report is not None:
                return self.report
            if self.state == "created":
                raise RuntimeError("monitor not started")
            self.state = "finalizing"
            self._sending.clear()  # stop new probes; in-flight ones finish below
            if self._inflight:
                await asyncio.wait(self._inflight, timeout=self.client.timeout_seconds + 1)
            assert self.lifecycle is not None
            self.lifecycle.finished_at = self.clock.now()
            await self._drain_meter()
            await self._cancel_tasks()
            self.report = self.build_report(self.clock.now())
            self.state = "finalized"
            self._done.set()
            return self.report

    async def wait_finalized(self, timeout: float | None = None) -> ObservationReport | None:
        try:
            await asyncio.wait_for(self._done.wait(), timeout)
        except asyncio.TimeoutError:
            return None
        return self.report

    async def stop(self) -> None:
        """Cancel everything without producing a report (cleanup / task cancellation path)."""
        self._sending.clear()
        await self._cancel_tasks()

    # --------------------------------------------------------------- loops
    async def _every(self, interval: float, tick: Callable[[], Awaitable[None]], only_while_sending: bool = True):
        loop = asyncio.get_running_loop()
        nxt = loop.time()
        try:
            while True:
                if only_while_sending and not self._sending.is_set():
                    return
                try:
                    await tick()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # the monitor must outlive any single failure
                    log.exception("monitor tick failed")
                    self._note(f"internal monitor error: {type(exc).__name__}: {exc}")
                nxt += interval
                await asyncio.sleep(max(0.0, nxt - loop.time()))
        except asyncio.CancelledError:
            raise

    def _spawn(self, coro) -> None:
        if len(self._inflight) >= self.cfg.max_inflight:
            self._skipped_ticks += 1
            coro.close()
            return
        t = asyncio.create_task(coro)
        self._inflight.add(t)
        t.add_done_callback(self._inflight.discard)

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    async def _health_loop(self) -> None:
        async def tick() -> None:
            for name, url in self.cfg.health_targets.items():
                self._spawn(self._health_once(name, url))

        await self._every(self.cfg.health_interval_s, tick)

    async def _health_once(self, name: str, url: str) -> None:
        rec = await run_health_probe(self.client, self._next_seq(), name, url)
        self.records.append(rec)

    async def _ingest_loop(self) -> None:
        async def tick() -> None:
            self._spawn(self._ingest_once())

        await self._every(self.cfg.ingest_interval_s, tick)

    async def _ingest_once(self) -> None:
        seq = self._next_seq()
        event = build_cloudevent(
            event_id=new_event_id(self.experiment_id),
            subject=self.subject,
            event_type=self.cfg.event_type,
            time=self.clock.now(),
            data={"method": "GET", "route": "/chaos/probe", "duration_ms": self.cfg.value_per_event},
        )
        rec = await run_ingest_probe(self.client, seq, self.cfg.primary_service, event)
        self.records.append(rec)
        self.ledger.record_ingest(rec)

    async def _meter_loop(self) -> None:
        async def tick() -> None:
            assert self.lifecycle is not None
            await poll_meter_once(
                self.client, self.ledger, self.cfg.meter_slug, self.subject,
                self.lifecycle.monitoring_started_at - timedelta(seconds=1),
            )

        # keeps polling while draining too (only stopped by _cancel_tasks)
        await self._every(self.cfg.meter_poll_interval_s, tick, only_while_sending=False)

    async def _resource_loop(self) -> None:
        async def tick() -> None:
            assert self.resource_probe is not None
            cpu, mem = await self.resource_probe()
            self.resources.append(ResourceSample(self.clock.now(), cpu, mem))

        await self._every(1.0, tick)

    async def _supervisor(self) -> None:
        async def tick() -> None:
            if self._should_finalize():
                asyncio.create_task(self.finalize())
                raise asyncio.CancelledError  # supervisor's job is done

        try:
            await self._every(0.25, tick, only_while_sending=False)
        except asyncio.CancelledError:
            pass

    def _should_finalize(self) -> bool:
        lc, now = self.lifecycle, self.clock.now()
        if lc is None or self.state != "running":
            return False
        if (now - lc.monitoring_started_at).total_seconds() >= self.cfg.max_runtime_s:
            self._note(f"max_runtime_s={self.cfg.max_runtime_s} reached; finalizing.")
            return True
        rc = lc.rollback_completed_at
        if rc is None:
            return False
        t = functional_recovery_time(self.records, self.ledger, rc, self.cfg)
        if t is not None and now >= t + timedelta(seconds=self.cfg.post_recovery_grace_s):
            return True
        if now >= rc + timedelta(seconds=self.cfg.recovery_deadline_s):
            self._note("recovery deadline passed without confirmed functional recovery; finalizing.")
            return True
        return False

    async def _drain_meter(self) -> None:
        """After sending stops, keep reading the meter until every accepted event is visible or the drain
        deadline passes. Events still invisible then are 'missing'."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.cfg.drain_deadline_s
        while loop.time() < deadline and self.ledger.accepted and not self.ledger.all_visible():
            await asyncio.sleep(self.cfg.meter_poll_interval_s)
        # one last read so accounting reflects the end state
        assert self.lifecycle is not None
        await poll_meter_once(self.client, self.ledger, self.cfg.meter_slug, self.subject,
                              self.lifecycle.monitoring_started_at - timedelta(seconds=1))

    async def _cancel_tasks(self) -> None:
        tasks = [t for t in self._tasks if t is not asyncio.current_task()]
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for t in list(self._inflight):
            t.cancel()
        await asyncio.gather(*list(self._inflight), return_exceptions=True)

    def _note(self, msg: str) -> None:
        if msg not in self.notes:
            self.notes.append(msg)

    # ------------------------------------------------------------- results
    def samples(self, as_of: datetime | None = None) -> list[MonitoringSample]:
        assert self.lifecycle is not None
        as_of = as_of or self.clock.now()
        lags = self.ledger.lags(as_of)
        return build_samples(self.experiment_id, self.target_service, list(self.records), lags,
                             list(self.resources), self.lifecycle, as_of, self.cfg.primary_service, self.cfg.metrics)

    def phase_summary(self, as_of: datetime | None = None) -> dict[Phase, PhaseMetrics | None]:
        assert self.lifecycle is not None
        as_of = as_of or self.clock.now()
        lags = self.ledger.lags(as_of)
        recs = list(self.records)
        out: dict[Phase, PhaseMetrics | None] = {}
        for phase, win in self.lifecycle.windows(as_of).items():
            out[phase] = None if win is None else phase_metrics(
                recs, lags, self.resources, win[0], win[1], self.cfg.primary_service, self.cfg.metrics)
        return out

    def baseline_ready(self) -> bool:
        assert self.lifecycle is not None
        base = self.phase_summary()[Phase.BASELINE]
        return (
            base is not None
            and (base.duration_seconds or 0) >= self.cfg.min_baseline_seconds
            and base.request_count >= self.cfg.min_baseline_requests
            and base.availability is not None
        )

    def build_report(self, as_of: datetime) -> ObservationReport:
        lc = self.lifecycle
        assert lc is not None
        phases = self.phase_summary(as_of)
        baseline = phases[Phase.BASELINE] or PhaseMetrics()
        samples = self.samples(as_of)
        detection = detect(list(self.records), samples, baseline, lc, self.cfg.primary_service, self.cfg.detection)
        notes = list(self.notes)

        acct = self.ledger.accounting()
        rec_time = None
        if lc.rollback_completed_at is not None and lc.fault_activated_at is not None:
            rec_time = functional_recovery_time(self.records, self.ledger, lc.rollback_completed_at, self.cfg)
        if lc.fault_activated_at is None:
            notes.append("No fault was activated (healthy control or injection did not activate).")
        elif lc.rollback_completed_at is None:
            notes.append("No rollback_completed_at received: functional recovery cannot be measured.")
        elif rec_time is None:
            notes.append("Functional recovery not confirmed before the recovery deadline.")
        if acct is None:
            notes.append("Meter was never read successfully: missing/duplicate events and accuracy are unknown (null).")
        if self.ledger.uncertain:
            notes.append(f"{len(self.ledger.uncertain)} ingest requests failed ambiguously (may or may not be enqueued); "
                         "they are excluded from duplicate counts.")
        if self.ledger.rejected_client_errors:
            notes.append(f"{self.ledger.rejected_client_errors} ingest probes got an unexpected 4xx - check probe payload/auth; "
                         "not counted in error_rate.")
        if self.ledger.query_failures:
            notes.append(f"{self.ledger.query_failures} meter queries failed or returned an unusable body.")
        if self._skipped_ticks:
            notes.append(f"{self._skipped_ticks} probe ticks skipped because max_inflight was reached.")
        if baseline.request_count < self.cfg.min_baseline_requests:
            notes.append("Baseline has few requests; baseline-relative thresholds fall back to absolute floors.")
        during = phases[Phase.FAULT]
        cov_src = during or baseline
        recovery = RecoverySummary(
            functional_recovery_seconds=(rec_time - lc.rollback_completed_at).total_seconds() if rec_time and lc.rollback_completed_at else None,
            functional_recovery_confirmed=rec_time is not None,
            missing_events=acct.missing_events if acct else None,
            duplicate_events=acct.duplicate_events if acct else None,
            metering_accuracy=acct.metering_accuracy if acct else None,
            accepted_events=len(self.ledger.accepted),
            uncertain_events=len(self.ledger.uncertain),
            metrics=phases[Phase.RECOVERY],
        )
        return ObservationReport(
            experiment_id=self.experiment_id,
            baseline=baseline,
            during_failure=during,
            recovery=recovery,
            detection=detection,
            lifecycle=LifecycleTimes(
                monitoring_started_at=lc.monitoring_started_at,
                fault_activated_at=lc.fault_activated_at,
                rollback_started_at=lc.rollback_started_at,
                rollback_completed_at=lc.rollback_completed_at,
                monitoring_finished_at=lc.finished_at,
                injection_success=lc.injection_success,
            ),
            monitoring_coverage=monitoring_coverage(cov_src, REQUIRED_METRICS),
            observer_notes=notes,
        )

    def snapshot(self) -> dict:
        """Live view for GET /metrics."""
        now = self.clock.now()
        assert self.lifecycle is not None
        phases = self.phase_summary(now)
        current = self.lifecycle.phase_at(now)
        return {
            "experiment_id": self.experiment_id,
            "state": self.state,
            "current_phase": current,
            "baseline_ready": self.baseline_ready(),
            "phases": {p.value: m for p, m in phases.items()},
            "samples": self.samples(now)[-60:],
            "notes": list(self.notes),
        }
