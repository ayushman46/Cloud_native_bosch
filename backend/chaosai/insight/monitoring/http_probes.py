"""HTTP probe runners producing `ProbeRecord`s (health/liveness probes and ingestion probes)."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from ..clients.openmeter_client import HttpResult, OpenMeterClient
from .records import ProbeRecord


def record_from_result(seq: int, kind: str, service: str, res: HttpResult, event_id: str | None = None) -> ProbeRecord:
    return ProbeRecord(
        seq=seq,
        kind=kind,
        service=service,
        started_at=res.started_at,
        completed_at=res.completed_at,
        duration_ms=res.duration_ms,
        outcome=res.outcome,
        status_code=res.status_code,
        event_id=event_id,
    )


async def run_health_probe(
    client: OpenMeterClient, seq: int, service: str, url: str, expected: frozenset[int] = frozenset({200})
) -> ProbeRecord:
    return record_from_result(seq, "health", service, await client.probe(url, expected))


def build_cloudevent(
    *,
    event_id: str,
    subject: str,
    event_type: str,
    time: datetime,
    data: dict[str, Any],
    source: str = "chaos-observer",
) -> dict[str, Any]:
    """CloudEvents 1.0 payload. `id`+`source` is OpenMeter's dedupe key, so ids are unique per event."""
    return {
        "specversion": "1.0",
        "id": event_id,
        "source": source,
        "type": event_type,
        "subject": subject,
        "time": time.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "datacontenttype": "application/json",
        "data": data,
    }


def new_event_id(experiment_id: str) -> str:
    return f"{experiment_id}-{uuid.uuid4().hex[:16]}"


async def run_ingest_probe(
    client: OpenMeterClient, seq: int, service: str, event: dict[str, Any]
) -> ProbeRecord:
    """POST one event. Success (204) = ACCEPTED only; processing is verified by the meter probe."""
    return record_from_result(seq, "ingest", service, await client.ingest_event(event), event_id=event["id"])
