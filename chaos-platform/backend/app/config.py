"""Environment-driven settings (shared file: add fields, don't rename)."""
from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # OpenMeter sandbox. Defaults match openmeter/quickstart/docker-compose.yaml (API 48888, workers 40000-40003).
    openmeter_base_url: str = "http://127.0.0.1:48888"
    openmeter_api_key: str | None = None
    openmeter_health_path: str = "/api/v1/debug/metrics"
    openmeter_meter_slug: str = "api_requests_total"
    openmeter_event_type: str = "request"
    sink_worker_health_url: str | None = "http://127.0.0.1:40000/healthz"
    balance_worker_health_url: str | None = "http://127.0.0.1:40001/healthz"
    probe_timeout_seconds: float = 2.0

    # NVIDIA hosted LLM (Analyzer). Leave the key unset to run with the Analyzer reporting "unavailable".
    nvidia_api_key: str | None = None
    nvidia_model: str = "meta/llama-3.3-70b-instruct"
    nvidia_base_url: str = "https://integrate.api.nvidia.com/v1"
    nvidia_timeout_seconds: float = 30.0
    nvidia_max_retries: int = Field(default=2, ge=0, le=5)
    nvidia_json_mode: bool = True
