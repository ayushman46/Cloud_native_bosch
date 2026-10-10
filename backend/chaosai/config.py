from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')
    mode: str = 'mock'
    openmeter_base_url: str = ''
    openmeter_health_path: str = '/api/v1/debug/metrics'
    enable_synthetic_events: bool = False
    openmeter_meter_slug: str = 'api_requests_total'
    openmeter_ingest_path: str = ''
    openmeter_query_path: str = ''
    chaos_control_url: str = ''
    chaos_control_api_key: str = ''
    chaosai_api_key: str = ''
    real_injections_enabled: bool = False
    nvidia_api_key: str = ''
    nvidia_model: str = ''
    nvidia_base_url: str = 'https://integrate.api.nvidia.com/v1'
    database_path: str = './chaosai.sqlite3'
    allowed_targets: str = 'openmeter,sink-worker'
    baseline_seconds: float = Field(default=3, gt=0, le=120)
    recovery_seconds: float = Field(default=10, gt=0, le=300)
    poll_seconds: float = Field(default=1, ge=0.01, le=10)
    max_duration_seconds: int = Field(default=30, ge=1, le=300)
    max_latency_ms: int = Field(default=2000, ge=1, le=10000)
    max_rps: int = Field(default=20, ge=1, le=100)
    activation_timeout_seconds: float = Field(default=10, gt=0, le=60)
    rollback_timeout_seconds: float = Field(default=10, gt=0, le=60)
    frontend_origin: str = 'http://localhost:5173'

    @property
    def targets(self) -> list[str]:
        return [x.strip() for x in self.allowed_targets.split(',') if x.strip()]
