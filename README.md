# ChaosAI for OpenMeter

ChaosAI is a separate FastAPI and React application for bounded chaos experiments against an OpenMeter deployment on another laptop. The ChaosAI container never accesses the remote Docker daemon. Its Injector calls a restricted, authenticated Chaos Control API on the OpenMeter host. The control API and its independent watchdog must be deployed by the infrastructure teammate; this repository provides a contract and an in-process mock, not remote Docker control.
The repository also contains a teammate's separately developed Observer and Analyzer service at `chaos-platform/backend`. Its 76 tests run independently; the Docker Compose entrypoint in this README runs the complete root-level ChaosAI application, while the teammate service remains available for follow-on integration.

## Run the complete mock application

```bash
cp .env.example .env
docker compose up --build
```

Open <http://localhost:5173>. `MODE=mock` clearly labels simulated plans, faults, probes, and reports. It does not inject faults or measure OpenMeter. The API is at <http://localhost:8000/docs>.
The dashboard and API publish only on this laptop's loopback interface. SQLite data survives container restarts in the `chaos_data` volume.
If either host port is occupied, set `CHAOSAI_HOST_API_PORT` or `CHAOSAI_HOST_DASHBOARD_PORT` in `.env`; the container ports and dashboard proxy remain unchanged.

## Connect to the remote OpenMeter laptop

Set these in `.env` before `docker compose up --build`:

```env
MODE=real
REAL_INJECTIONS_ENABLED=false
OPENMETER_BASE_URL=http://PRIVATE_HOST_IP:OPENMETER_PORT
OPENMETER_HEALTH_PATH=/api/v1/debug/metrics
ENABLE_SYNTHETIC_EVENTS=true
OPENMETER_METER_SLUG=api_requests_total
CHAOS_CONTROL_URL=http://PRIVATE_HOST_IP:CONTROL_PORT
CHAOS_CONTROL_API_KEY=replace-with-controller-secret
CHAOSAI_API_KEY=replace-with-dashboard-secret
NVIDIA_API_KEY=replace-with-key
NVIDIA_MODEL=replace-with-verified-model-name
ALLOWED_TARGETS=openmeter,sink-worker
```

`PRIVATE_HOST_IP`, ports, health paths, and service identifiers must come from the running OpenMeter Compose deployment. The supplied project brief describes them as unverified. Enter `CHAOSAI_API_KEY` in the dashboard key field before connecting. Real-mode `/api/*` endpoints require this bearer key. The dashboard's connectivity panel and `GET /api/health` report checks from inside the ChaosAI container. Configure an authenticated private network or tunnel between laptops; keep the control API off public interfaces. NVIDIA model availability and JSON response support must be verified with the selected account.
Real mode can check connectivity and preview plans with `REAL_INJECTIONS_ENABLED=false`. Set it to `true` only after the OpenMeter host owner authorizes the bounded fault run and its controller watchdog has been verified. The actual fault is initiated by `POST /api/experiments`.

The observer samples configured GET endpoints for HTTP reachability and latency. With `ENABLE_SYNTHETIC_EVENTS=true`, it also sends one uniquely identified CloudEvent in each experiment phase to `/api/v1/events` and queries the isolated subject on `/api/v1/meters/{OPENMETER_METER_SLUG}/query`. Recovery succeeds only when the metered count equals the number of accepted events. This assumes the running deployment contains the quickstart `request` meter and permits these endpoints. Set the flag to false for other configurations. HTTP probes alone do not establish event correctness or Kafka consumer lag.

## Remote Chaos Control API contract

The remote host must expose the following JSON endpoints and accept `Authorization: Bearer <CHAOS_CONTROL_API_KEY>`:

- `GET /api/health` — reachable/authenticated check.
- `GET /api/capabilities` — `targets`, `fault_types`, `watchdog_supported`, `network_latency_ready`, `traffic_spike_ready`.
- `POST /api/experiments` — accepts `experiment_id`, `application`, `target_service`, `failure_type`, `parameters` and `Idempotency-Key` equal to the ID. Response must include `experiment_id`, `status`, `watchdog_armed`. The controller must arm its independent rollback watchdog **before** activating a fault or accepting it as armed.
- `GET /api/experiments/{id}` — returns `experiment_id`, `status`, `watchdog_armed`, `injection_verified`, `rollback_verified`, optional `fault_activated_at`, `rollback_completed_at`, and `message`. A verified activation or rollback must also include its corresponding nonempty `activation_evidence` or `rollback_evidence` object, with `evidence_type`, `observed_at`, and `details`.
- `POST /api/experiments/{id}/abort` — returns the same status shape and initiates local rollback.

`status` values used by ChaosAI are `armed`, `active`, `rolling_back`, `rolled_back`, `failed`, `rejected`, and `aborted`. The controller must allowlist container IDs, fault operations and parameters; perform actual activation and rollback verification; reject concurrent faults; and keep the watchdog working if ChaosAI disconnects. A successful POST alone is never treated as proof of injection. Network latency requires verified Toxiproxy wiring; traffic spike requires an approved bounded load generator. This service is a separate teammate deliverable and is not included here because the OpenMeter host configuration is unavailable.

Supported faults: `application_crash`, `network_latency`, `service_restart`, `traffic_spike`. Local caps default to 30 seconds, 2000 ms latency, and 20 requests/sec. The remote controller must enforce its own limits as well.

## Workflow and endpoints

The LangGraph flow is `plan → safety/preflight → baseline → inject/verify → observe → rollback/verify → functional recovery → analyze`. Experiment records persist in SQLite. Only one local experiment runs at a time. `POST /api/experiments/plan` previews a plan; `POST /api/experiments` starts a run; `GET /api/experiments`, `GET /api/experiments/{id}`, `GET /api/experiments/{id}/metrics`, and `POST /api/experiments/{id}/abort` support the dashboard. `GET /api/experiments/{id}/report` provides the detailed ChaosAI report. `GET /api/experiments/{id}/observer-report` converts an experiment to the repository's shared Observer contract, with unavailable telemetry left null. A report appears only after verified controller rollback and recovery evaluation. Real-mode planner errors fail closed. Real-mode NVIDIA analyzer failures produce a labeled deterministic summary.

## Development and tests

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r backend/requirements.txt
PYTHONPATH=backend:. pytest -q
cd frontend && npm install && npm run build
```

For the full local checks, run `scripts/test_all.sh`. With the mock API already running at port 8000, `scripts/test_all.sh --http` also exercises all four fault workflows and writes `verification/api-smoke.json`. No real fault is triggered by tests. The mock controller uses the same API data shapes and simulates watchdog, activation, rollback, and abort. The shared Observer schema under `hacker_agent_observer_contract/` comes from the team's existing GitHub repository; the adapter maps ChaosAI faults, timestamps, and percentage units to it.

## Verified upstream assumptions

The current official [OpenMeter quickstart](https://github.com/openmeterio/openmeter/blob/main/quickstart/README.md) documents port 48888, CloudEvents ingestion at `/api/v1/events`, and the `api_requests_total` meter query. Its [Compose file](https://github.com/openmeterio/openmeter/blob/main/quickstart/docker-compose.yaml) names `openmeter` and `sink-worker` and uses `/api/v1/debug/metrics` for the API health check. The published port binds to `127.0.0.1` on the OpenMeter laptop, so cross-laptop access needs an approved private tunnel or a deliberate host binding change. Verify the teammate's checked-out version and `docker compose ps` before using real mode.
