# Chaos Control API contract and notes

Detail moved out of the README. The README is the quickstart.

## Remote Chaos Control API contract

The remote host must expose the following JSON endpoints and accept `Authorization: Bearer <CHAOS_CONTROL_API_KEY>`:

- `GET /api/health` — reachable/authenticated check.
- `GET /api/capabilities` — `targets`, `fault_types`, `watchdog_supported`, `network_latency_ready`, `traffic_spike_ready`.
- `POST /api/experiments` — accepts `experiment_id`, `application`, `target_service`, `failure_type`, `parameters` and `Idempotency-Key` equal to the ID. Response must include `experiment_id`, `status`, `watchdog_armed`. The controller must arm its independent rollback watchdog **before** activating a fault or accepting it as armed.
- `GET /api/experiments/{id}` — returns `experiment_id`, `status`, `watchdog_armed`, `injection_verified`, `rollback_verified`, optional `fault_activated_at`, `rollback_completed_at`, and `message`. A verified activation or rollback must also include its corresponding nonempty `activation_evidence` or `rollback_evidence` object, with `evidence_type`, `observed_at`, and `details`.
- `POST /api/experiments/{id}/abort` — returns the same status shape and initiates local rollback.

`status` values used by ChaosAI are `armed`, `active`, `rolling_back`, `rolled_back`, `failed`, `rejected`, and `aborted`. The controller must allowlist container IDs, fault operations and parameters; perform actual activation and rollback verification; reject concurrent faults; and keep the watchdog working if ChaosAI disconnects. A successful POST alone is never treated as proof of injection. Network latency requires verified Toxiproxy wiring; traffic spike requires an approved bounded load generator. The reference implementation is `controller/` in this repository (Docker SDK for crash/restart, Toxiproxy for latency, a bounded load generator for traffic spikes, and its own watchdog). `MODE=mock` uses an in-process mock with the same API shape.

Supported faults: `application_crash`, `network_latency`, `service_restart`, `traffic_spike`. Local caps default to 30 seconds, 2000 ms latency, and 20 requests/sec. The remote controller must enforce its own limits as well.

## Workflow and endpoints

The LangGraph flow is `plan → safety/preflight → baseline → inject/verify → observe → rollback/verify → functional recovery → analyze`. Experiment records persist in SQLite. Only one local experiment runs at a time. `POST /api/experiments/plan` previews a plan; `POST /api/experiments` starts a run; `GET /api/experiments`, `GET /api/experiments/{id}`, `GET /api/experiments/{id}/metrics`, and `POST /api/experiments/{id}/abort` support the dashboard. `GET /api/experiments/{id}/report` provides the detailed ChaosAI report. `GET /api/experiments/{id}/observer-report` converts an experiment to the repository's shared Observer contract, with unavailable telemetry left null. A report appears only after verified controller rollback and recovery evaluation. Real-mode planner errors fail closed. Real-mode NVIDIA analyzer failures produce a labeled deterministic summary.

