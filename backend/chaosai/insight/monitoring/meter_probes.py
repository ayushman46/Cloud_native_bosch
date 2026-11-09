"""Metering probes: event ledger, processing-lag and event accounting.

OpenMeter ingestion is asynchronous (API -> Kafka -> sink-worker -> ClickHouse). A 204 from
POST /api/v1/events only means "accepted". We therefore keep a ledger of events we sent and compare it
with what the meter reports for a dedicated synthetic subject.

Approximations (documented, deliberate):

* A meter query returns an AGGREGATE (COUNT/SUM), not per-event status. The n-th accepted event is
  treated as "visible" at the first meter observation whose count >= n (FIFO assumption). Processing lag
  is therefore an estimate with resolution = meter poll interval.
* Events whose POST failed ambiguously (read timeout, reset, 5xx) are "uncertain": they may or may not
  have been enqueued. They can explain meter counts above the accepted count, so they are never
  reported as duplicates (duplicates are only counted beyond accepted + uncertain).
* The meter must be COUNT, or SUM over a property we set to `value_per_event` in every event.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ..clients.openmeter_client import HttpResult, OpenMeterClient
from .records import Outcome, ProbeRecord

_EPS = 1e-6
_UNCERTAIN_OUTCOMES = {Outcome.READ_TIMEOUT, Outcome.TRANSPORT_ERROR, Outcome.SERVER_ERROR, Outcome.INVALID_RESPONSE}


@dataclass(frozen=True)
class AcceptedEvent:
    event_id: str
    accepted_at: datetime
    rank: int  # 1-based order of acceptance


@dataclass(frozen=True)
class MeterObservation:
    at: datetime
    count: float  # meter total converted to events (value / value_per_event)


@dataclass(frozen=True)
class EventLag:
    event: AcceptedEvent
    visible_at: datetime | None
    lag_ms: float  # if not visible: lower bound measured to the last meter read that still lacked it
    censored: bool
    observed_until: datetime | None = None  # for invisible events: time of the last meter read (event absent then)


@dataclass(frozen=True)
class Accounting:
    expected_events: int
    observed_count: float
    missing_events: int
    duplicate_events: int
    uncertain_events: int
    metering_accuracy: float | None


class EventLedger:
    def __init__(self, value_per_event: float = 1.0) -> None:
        self.value_per_event = value_per_event
        self.accepted: list[AcceptedEvent] = []
        self.uncertain: list[tuple[str, datetime]] = []
        self.observations: list[MeterObservation] = []
        self.query_failures = 0
        self.rejected_client_errors = 0

    # ------------------------------------------------------------- input
    def record_ingest(self, rec: ProbeRecord) -> None:
        if rec.kind != "ingest" or rec.event_id is None:
            return
        if rec.ok:
            self.accepted.append(AcceptedEvent(rec.event_id, rec.completed_at, len(self.accepted) + 1))
        elif rec.outcome in _UNCERTAIN_OUTCOMES:
            self.uncertain.append((rec.event_id, rec.started_at))
        elif rec.outcome is Outcome.UNEXPECTED_CLIENT_ERROR:
            self.rejected_client_errors += 1

    def record_observation(self, at: datetime, meter_total: float) -> None:
        self.observations.append(MeterObservation(at, meter_total / self.value_per_event))

    # ------------------------------------------------------- derived data
    def visibility_times(self) -> list[datetime | None]:
        """visible_at for each accepted event (same order); None = never reached by the last observation."""
        obs = sorted(self.observations, key=lambda o: o.at)
        out: list[datetime | None] = []
        running_max = -1.0
        visible_at: datetime | None = None
        pending_idx = 0
        # advance through observations once; ranks are increasing so a single sweep suffices
        for ev in self.accepted:
            while pending_idx < len(obs) and running_max + _EPS < ev.rank:
                running_max = max(running_max, obs[pending_idx].count)
                visible_at = obs[pending_idx].at
                pending_idx += 1
            out.append(visible_at if running_max + _EPS >= ev.rank else None)
        return out

    def lags(self, as_of: datetime) -> list[EventLag]:
        """Per-event lag. An event still invisible contributes a LOWER BOUND measured to the last successful
        meter read (when we know it was absent). Events accepted after the last read, or any event when the
        meter was never read, are omitted: their lag is unknown, not zero."""
        if not self.observations:
            return []
        last_obs = max(o.at for o in self.observations)
        result = []
        for ev, vis in zip(self.accepted, self.visibility_times()):
            if vis is not None:
                result.append(EventLag(ev, vis, max(0.0, (vis - ev.accepted_at).total_seconds() * 1000), False))
            elif ev.accepted_at < last_obs:
                end = min(as_of, last_obs)
                result.append(EventLag(ev, None, max(0.0, (end - ev.accepted_at).total_seconds() * 1000), True, last_obs))
        return result

    def latest_count(self) -> float | None:
        if not self.observations:
            return None
        return max(self.observations, key=lambda o: o.at).count

    def accounting(self) -> Accounting | None:
        """Final accounting from the latest meter observation. None if the meter was never read."""
        count = self.latest_count()
        if count is None:
            return None
        expected = len(self.accepted)
        n_uncertain = len(self.uncertain)
        observed = int(round(count))
        missing = max(0, expected - observed)
        excess = max(0, observed - expected)
        duplicates = max(0, excess - n_uncertain)
        accuracy = None
        if expected > 0:
            accuracy = max(0.0, 1.0 - (missing + duplicates) / expected)
        return Accounting(expected, count, missing, duplicates, n_uncertain, accuracy)

    def all_visible(self) -> bool:
        return bool(self.accepted) and all(v is not None for v in self.visibility_times())

    def caught_up_after(self, t: datetime) -> datetime | None:
        """Time at which the pipeline is proven working after `t`: the first visibility of an event
        accepted at/after `t`. Under the FIFO assumption its visibility implies every earlier accepted
        event was already counted, i.e. the backlog drained. None if no such event is visible yet."""
        post = [v for ev, v in zip(self.accepted, self.visibility_times()) if ev.accepted_at >= t and v is not None]
        return max(min(post), t) if post else None


# --------------------------------------------------------------- polling
def sum_meter_rows(body: Any) -> float | None:
    """Total across all returned rows (no groupBy -> one row). None if the shape is unexpected."""
    if not isinstance(body, dict) or not isinstance(body.get("data"), list):
        return None
    total = 0.0
    for row in body["data"]:
        if not isinstance(row, dict) or not isinstance(row.get("value"), (int, float)):
            return None
        total += float(row["value"])
    return total


async def poll_meter_once(
    client: OpenMeterClient,
    ledger: EventLedger,
    meter_slug: str,
    subject: str,
    from_: datetime,
) -> HttpResult:
    res = await client.query_meter(meter_slug, subject, from_)
    if res.outcome is Outcome.OK:
        total = sum_meter_rows(res.json)
        if total is None:
            ledger.query_failures += 1
            res.outcome = Outcome.INVALID_RESPONSE
        else:
            ledger.record_observation(res.completed_at, total)
    else:
        ledger.query_failures += 1
    return res
