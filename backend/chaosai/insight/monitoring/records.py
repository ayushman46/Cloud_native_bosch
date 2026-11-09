"""Raw probe records and failure classification.

Failure policy (documented, configurable by choosing `expected_statuses` per probe):

* OK                      - response status is in the probe's expected set.
* SERVER_ERROR            - HTTP 5xx.                              -> infrastructure failure
* RATE_LIMITED            - HTTP 429 / 408.                        -> infrastructure failure
* CONNECTION_REFUSED      - TCP refused.                           -> infrastructure failure
* DNS_FAILURE             - name resolution failed.                -> infrastructure failure
* CONNECT_TIMEOUT / READ_TIMEOUT - no connection / no response in time. -> infrastructure failure
* TRANSPORT_ERROR         - reset, protocol error, other transport. -> infrastructure failure
* INVALID_RESPONSE        - 2xx but body unusable for the probe.   -> infrastructure failure
* UNEXPECTED_CLIENT_ERROR - other 4xx / unexpected 3xx. Means OUR probe is wrong (bad payload,
                            wrong path, missing auth), not that the service is down. It is NOT counted
                            in error_rate; it still fails the probe (so availability cannot be claimed)
                            and is surfaced in observer notes.
"""
from __future__ import annotations

import socket
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Protocol

import httpx


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class Outcome(str, Enum):
    OK = "ok"
    SERVER_ERROR = "server_error"
    RATE_LIMITED = "rate_limited"
    UNEXPECTED_CLIENT_ERROR = "unexpected_client_error"
    CONNECTION_REFUSED = "connection_refused"
    DNS_FAILURE = "dns_failure"
    CONNECT_TIMEOUT = "connect_timeout"
    READ_TIMEOUT = "read_timeout"
    TRANSPORT_ERROR = "transport_error"
    INVALID_RESPONSE = "invalid_response"


_TIMEOUTS = {Outcome.CONNECT_TIMEOUT, Outcome.READ_TIMEOUT}
# Outcomes where a request was served or timed out, i.e. the duration says something about the service.
# Fast local failures (refused / DNS / reset) are excluded from latency percentiles.
_LATENCY_SIGNAL = {
    Outcome.OK,
    Outcome.SERVER_ERROR,
    Outcome.RATE_LIMITED,
    Outcome.UNEXPECTED_CLIENT_ERROR,
    Outcome.INVALID_RESPONSE,
    Outcome.CONNECT_TIMEOUT,
    Outcome.READ_TIMEOUT,
}


@dataclass(frozen=True)
class ProbeRecord:
    seq: int
    kind: str  # "health" | "ingest"
    service: str
    started_at: datetime
    completed_at: datetime
    duration_ms: float
    outcome: Outcome
    status_code: int | None = None
    event_id: str | None = None

    @property
    def ok(self) -> bool:
        return self.outcome is Outcome.OK

    @property
    def is_infra_failure(self) -> bool:
        return self.outcome not in (Outcome.OK, Outcome.UNEXPECTED_CLIENT_ERROR)

    @property
    def is_timeout(self) -> bool:
        return self.outcome in _TIMEOUTS

    @property
    def has_latency_signal(self) -> bool:
        return self.outcome in _LATENCY_SIGNAL


def classify_status(status: int, expected: frozenset[int]) -> Outcome:
    if status in expected:
        return Outcome.OK
    if status >= 500:
        return Outcome.SERVER_ERROR
    if status in (408, 429):
        return Outcome.RATE_LIMITED
    return Outcome.UNEXPECTED_CLIENT_ERROR


def _chain(exc: BaseException):
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        yield exc
        exc = exc.__cause__ or exc.__context__


def classify_exception(exc: BaseException) -> Outcome:
    if isinstance(exc, httpx.ConnectTimeout) or isinstance(exc, httpx.PoolTimeout):
        return Outcome.CONNECT_TIMEOUT
    if isinstance(exc, httpx.TimeoutException):  # read / write timeouts
        return Outcome.READ_TIMEOUT
    chain = list(_chain(exc))
    if any(isinstance(e, socket.gaierror) for e in chain):
        return Outcome.DNS_FAILURE
    if any(isinstance(e, ConnectionRefusedError) for e in chain):
        return Outcome.CONNECTION_REFUSED
    text = " ".join(str(e).lower() for e in chain)
    if "getaddrinfo" in text or "name or service not known" in text or "name resolution" in text:
        return Outcome.DNS_FAILURE
    if "refused" in text or "10061" in text:
        return Outcome.CONNECTION_REFUSED
    return Outcome.TRANSPORT_ERROR
