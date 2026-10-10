#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHONPATH="backend:." .venv/bin/python -m compileall -q backend tests
PYTHONPATH="backend:." .venv/bin/pytest -q --junitxml=verification/pytest.xml
(cd chaos-platform/backend && ../../.venv/bin/python -m pytest tests -q)
npm --prefix frontend run build
.venv/bin/pip check
.venv/bin/python - <<'PY'
import yaml
with open('docker-compose.yml') as source:
    config = yaml.safe_load(source)
assert set(config['services']) == {'backend', 'dashboard'}
print('Compose YAML: valid')
PY
if [[ "${1:-}" == "--http" ]]; then
  .venv/bin/python scripts/http_smoke.py
fi
