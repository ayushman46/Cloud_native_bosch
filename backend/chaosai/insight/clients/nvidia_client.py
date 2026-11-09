"""NVIDIA hosted-LLM client (OpenAI-compatible /chat/completions) with bounded retries.

Uses plain httpx so the Analyzer needs no extra dependency and is trivially mockable. If the Planner
teammate already has a shared NVIDIA client, point `AnalyzerAgent(llm=...)` at theirs: the Analyzer only
needs an object with `async complete(system: str, user: str) -> str`.

Errors are structured (`NvidiaError.kind`) and never swallowed into a fake answer.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Protocol

import httpx

log = logging.getLogger(__name__)


class LLMBackend(Protocol):
    async def complete(self, system: str, user: str) -> str: ...


class NvidiaError(Exception):
    kind = "error"

    def __init__(self, message: str, attempts: int = 0) -> None:
        super().__init__(message)
        self.attempts = attempts


class NvidiaNotConfigured(NvidiaError):
    kind = "not_configured"


class NvidiaTimeout(NvidiaError):
    kind = "timeout"


class NvidiaRateLimited(NvidiaError):
    kind = "rate_limited"


class NvidiaUnavailable(NvidiaError):
    kind = "unavailable"  # 5xx, transport failure, or model not found / not permitted


class NvidiaBadResponse(NvidiaError):
    kind = "bad_response"  # 200 but no usable message content


_RETRYABLE_STATUS = {500, 502, 503, 504}


class NvidiaChatClient:
    def __init__(
        self,
        api_key: str | None,
        model: str,
        base_url: str = "https://integrate.api.nvidia.com/v1",
        timeout_seconds: float = 30.0,
        max_retries: int = 2,
        json_mode: bool = True,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        backoff_base_s: float = 0.5,
    ) -> None:
        self.api_key, self.model = api_key, model
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.json_mode = json_mode
        self.backoff_base_s = backoff_base_s
        self._sleep = sleep
        self.last_attempts = 0
        self._http = httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds), transport=transport)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def complete(self, system: str, user: str) -> str:
        if not self.api_key:
            raise NvidiaNotConfigured("NVIDIA_API_KEY is not set")
        payload: dict = {
            "model": self.model,
            "temperature": 0,
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        }
        if self.json_mode:
            payload["response_format"] = {"type": "json_object"}
        headers = {"Authorization": f"Bearer {self.api_key}", "Accept": "application/json"}
        attempts, last_err = 0, None
        while attempts <= self.max_retries:
            attempts += 1
            self.last_attempts = attempts
            try:
                resp = await self._http.post(f"{self.base_url}/chat/completions", json=payload, headers=headers)
            except httpx.TimeoutException as exc:
                last_err = NvidiaTimeout(f"timeout after attempt {attempts}: {exc}", attempts)
            except httpx.HTTPError as exc:
                last_err = NvidiaUnavailable(f"transport error: {type(exc).__name__}: {exc}", attempts)
            else:
                if resp.status_code == 200:
                    return self._content(resp, attempts)
                if resp.status_code == 429:
                    last_err = NvidiaRateLimited("rate limited (429)", attempts)
                    await self._sleep(_retry_after(resp, self.backoff_base_s * attempts))
                    continue
                if resp.status_code == 400 and "response_format" in payload:
                    # the model/account may not support JSON mode: retry once without it
                    del payload["response_format"]
                    self.json_mode = False
                    attempts -= 1
                    last_err = NvidiaUnavailable("JSON mode rejected (400); retrying without it", attempts)
                    continue
                if resp.status_code in _RETRYABLE_STATUS:
                    last_err = NvidiaUnavailable(f"HTTP {resp.status_code}", attempts)
                else:  # 400/401/403/404...: retrying cannot help
                    raise NvidiaUnavailable(f"HTTP {resp.status_code}: {resp.text[:200]}", attempts)
            if attempts <= self.max_retries:
                await self._sleep(self.backoff_base_s * (2 ** (attempts - 1)))
        assert last_err is not None
        last_err.attempts = attempts
        raise last_err

    @staticmethod
    def _content(resp: httpx.Response, attempts: int) -> str:
        try:
            content = resp.json()["choices"][0]["message"]["content"]
        except (ValueError, KeyError, IndexError, TypeError) as exc:
            raise NvidiaBadResponse(f"unexpected response shape: {exc}", attempts) from exc
        if not isinstance(content, str) or not content.strip():
            raise NvidiaBadResponse("empty message content", attempts)
        return content


def _retry_after(resp: httpx.Response, default: float) -> float:
    try:
        return min(10.0, float(resp.headers.get("Retry-After", default)))
    except ValueError:
        return default
