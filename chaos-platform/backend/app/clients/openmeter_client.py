"""Thin async HTTP client for the OpenMeter endpoints the Observer uses.

Verified against openmeter `api/openapi.yaml`:
  POST /api/v1/events                 -> 204 on acceptance (Content-Type application/cloudevents+json)
  GET  /api/v1/meters/{slug}/query    -> {"data":[{"value","windowStart","windowEnd","subject",...}]}
  GET  /api/v1/debug/metrics          -> used by the official compose healthcheck of the API
  GET  <worker>:10000/healthz         -> official compose healthcheck of the workers

A 204 only means the event was ACCEPTED by the API. Whether it was processed (Kafka -> sink-worker ->
ClickHouse) is observed separately through the meter query.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import httpx

from ..monitoring.records import (
    Clock,
    Outcome,
    SystemClock,
    classify_exception,
    classify_status,
)


@dataclass
class HttpResult:
    started_at: datetime
    completed_at: datetime
    duration_ms: float
    outcome: Outcome
    status_code: int | None = None
    json: Any = None
    error: str | None = None
    headers: dict[str, str] = field(default_factory=dict)


class OpenMeterClient:
    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        timeout_seconds: float = 2.0,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.clock = clock or SystemClock()
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._http = httpx.AsyncClient(
            headers=headers,
            timeout=httpx.Timeout(timeout_seconds),
            transport=transport,
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    # ------------------------------------------------------------------ core
    async def request(
        self,
        method: str,
        url: str,
        *,
        expected: frozenset[int] = frozenset({200}),
        json: Any = None,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
        params: list[tuple[str, str]] | None = None,
        parse_json: bool = False,
    ) -> HttpResult:
        if not url.startswith("http"):
            url = f"{self.base_url}{url}"
        started = self.clock.now()
        t0 = time.perf_counter()
        try:
            resp = await self._http.request(
                method, url, json=json, content=content, headers=headers, params=params
            )
        except httpx.HTTPError as exc:
            return HttpResult(
                started_at=started,
                completed_at=self.clock.now(),
                duration_ms=(time.perf_counter() - t0) * 1000,
                outcome=classify_exception(exc),
                error=f"{type(exc).__name__}: {exc}",
            )
        duration_ms = (time.perf_counter() - t0) * 1000
        outcome = classify_status(resp.status_code, expected)
        body = None
        error = None
        if parse_json and outcome is Outcome.OK:
            try:
                body = resp.json()
            except ValueError as exc:
                outcome, error = Outcome.INVALID_RESPONSE, f"invalid JSON: {exc}"
        elif outcome is not Outcome.OK:
            error = f"HTTP {resp.status_code}"
        return HttpResult(
            started_at=started,
            completed_at=self.clock.now(),
            duration_ms=duration_ms,
            outcome=outcome,
            status_code=resp.status_code,
            json=body,
            error=error,
        )

    # ------------------------------------------------------------- endpoints
    async def ingest_event(self, event: dict[str, Any]) -> HttpResult:
        return await self.request(
            "POST",
            "/api/v1/events",
            expected=frozenset({204}),
            content=_json_bytes(event),
            headers={"Content-Type": "application/cloudevents+json"},
        )

    async def query_meter(
        self, meter_slug: str, subject: str, from_: datetime, to: datetime | None = None
    ) -> HttpResult:
        params = [("subject", subject), ("from", _rfc3339(from_))]
        if to is not None:
            params.append(("to", _rfc3339(to)))
        return await self.request(
            "GET",
            f"/api/v1/meters/{meter_slug}/query",
            expected=frozenset({200}),
            params=params,
            parse_json=True,
        )

    async def probe(self, url: str, expected: frozenset[int] = frozenset({200})) -> HttpResult:
        return await self.request("GET", url, expected=expected)


def _json_bytes(obj: Any) -> bytes:
    import json

    return json.dumps(obj, separators=(",", ":")).encode()


def _rfc3339(dt: datetime) -> str:
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
