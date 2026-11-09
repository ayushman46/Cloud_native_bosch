"""Fake OpenMeter used by tests: ingestion, async processing with a pausable sink worker, meter query."""
from __future__ import annotations

import asyncio
import json
import time

import httpx


class FakeOpenMeter:
    def __init__(self, processing_delay_s: float = 0.05) -> None:
        self.processing_delay_s = processing_delay_s
        self.api_down = False
        self.api_status: int | None = None  # force e.g. 500
        self.ingest_latency_s = 0.0
        self.worker_down = False  # sink-worker stopped: events accepted but not processed
        self.worker_health_down = False
        self.drop_processing = False  # events accepted but never processed (data loss)
        self.double_count = False  # meter over-counts (duplicate processing)
        self.meter_down = False
        self.events: dict[str, float | None] = {}  # id -> process_at (monotonic) or None while pending
        self.ingest_calls = 0

    def now(self) -> float:
        return time.monotonic()

    def set_worker_down(self, down: bool) -> None:
        self.worker_down = down
        if not down:
            for k, v in self.events.items():
                if v is None:
                    self.events[k] = self.now() + self.processing_delay_s

    def count(self) -> int:
        now = self.now()
        n = sum(1 for v in self.events.values() if v is not None and v <= now)
        return n * 2 if self.double_count else n

    async def handler(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "healthz" in url:
            if self.worker_health_down or self.worker_down:
                return httpx.Response(503)
            return httpx.Response(200)
        if self.api_down:
            err = httpx.ConnectError("All connection attempts failed", request=request)
            err.__cause__ = ConnectionRefusedError()
            raise err
        if request.url.path == "/api/v1/debug/metrics":
            return httpx.Response(self.api_status or 200)
        if request.method == "POST" and request.url.path == "/api/v1/events":
            self.ingest_calls += 1
            if self.ingest_latency_s:
                await asyncio.sleep(self.ingest_latency_s)
            if self.api_status:
                return httpx.Response(self.api_status)
            ev = json.loads(request.content)
            if ev["id"] not in self.events:  # OpenMeter dedupes on source+id
                if self.drop_processing:
                    self.events[ev["id"]] = float("inf")
                elif self.worker_down:
                    self.events[ev["id"]] = None
                else:
                    self.events[ev["id"]] = self.now() + self.processing_delay_s
            return httpx.Response(204)
        if request.method == "GET" and "/query" in request.url.path:
            if self.meter_down:
                return httpx.Response(503)
            return httpx.Response(200, json={"data": [{"value": self.count(), "windowStart": "2026-01-01T00:00:00Z",
                                                       "windowEnd": "2026-01-01T00:00:01Z", "subject": "x", "groupBy": {}}]})
        return httpx.Response(404)

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)
