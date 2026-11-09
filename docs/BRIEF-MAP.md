# Brief to deliverable map

Every requirement from the Bosch brief, where it is satisfied, and the evidence. "Verified" means it ran on the real stack on 2026-10-10.

| Brief requirement | Where it is satisfied | Status |
|---|---|---|
| Sample app is open source, free, public repo | OpenMeter (Apache-2.0), pulled from its published images. Our repo is MIT (`LICENSE`) | Repo must be made public at submission |
| Cloud-native: containerized, multi-service, Docker Compose | `docker-compose.real.yml`: OpenMeter API, sink and balance workers, Kafka, ClickHouse, Postgres, Redis, Svix | Verified, 12 containers healthy |
| Runs entirely in our own sandbox | All ports bound to `127.0.0.1`; faults only on the allowlist | Verified |
| Document repo link and deployment steps | `README.md` quickstart. Repo link: _add after pushing_ | Link pending |
| 1. Select and understand the app, map services, dependencies, data flows | `GET /api/dependency-map`, dashboard "What depends on what". Documented flows plus `depends_on` read from the running containers | Verified |
| 2. Plan experiments intelligently from the dependency map and observed behavior | Planner prompt carries the map and past results (`planning_context` in `orchestration.py`); NVIDIA LLM | Verified live: LLM chose the API from the map |
| 3. Inject controlled failures with safety limits | `controller/`: crash, restart, latency (Toxiproxy), traffic spike, failed dependency. Allowlist, hard caps, watchdog armed first, evidence required | Verified: latency, crash, restart, Kafka down, traffic spike |
| 3. Stop automatically on error rate, latency or blast radius | `_threshold_guard` in `orchestration.py`. Error rate and latency thresholds are settings. Blast radius is shown on the map, not enforced | Verified for error rate. Latency threshold covered by tests only |
| 4. Observe errors, slowdowns, recovery time | Probes per phase, detection latency, functional recovery time, usage-metering check | Verified |
| 5. Identify weaknesses, recommend fixes | Analyzer and LLM diagnosis, evidence-cited and grounded | Verified |
| 5. Ranked resilience score | `resilience_score` in `metrics.py`; dashboard ranks experiments weakest first | Verified |
| Examples: service or container crash | `application_crash` on OpenMeter or any allowlisted service | Verified |
| Examples: slow or lossy network | `network_latency` through Toxiproxy (slow; no packet loss) | Verified (slow only) |
| Examples: full disk | Not implemented | Gap |
| Examples: overloaded database | Traffic spike on the API, and stopping Postgres or ClickHouse. No database-specific load fault | Partial |
| Examples: failed dependency | Crash Kafka, ClickHouse, Redis or Postgres | Verified with Kafka |
| Examples: sudden traffic spike | `traffic_spike`, bounded to 20 requests per second | Verified: 15 usage events per second for 10 s sent to OpenMeter's ingest endpoint, 150 of 150 accepted and metered, p95 latency 11.6 to 27 ms |
| Out of scope: production or other participants' systems | README rules section; allowlist; loopback only | Satisfied |
| Day-1: app running, dependency map, one safe failure, working kill switch | All of it | Verified: kill switch aborted a live fault in about 4 s with verified rollback |
| Day-1: monitoring baseline under steady load | Baseline probes plus steady synthetic events | Verified |
| Day-1: safety control tested at least once | Automatic stop fired in a real crash run, and unit tests cover both thresholds | Verified |
