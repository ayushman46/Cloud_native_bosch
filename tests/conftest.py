import pytest
from chaosai.config import Settings


@pytest.fixture
def cfg(tmp_path):
    return Settings(_env_file=None, mode='mock', database_path=str(tmp_path/'test.db'), baseline_seconds=.03, recovery_seconds=.12, poll_seconds=.02, max_duration_seconds=1, activation_timeout_seconds=.5, rollback_timeout_seconds=.8, enable_synthetic_events=True, late_metering_seconds=.1)
