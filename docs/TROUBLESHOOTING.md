# Reference and troubleshooting guide

Keep this open while you present or demo. Every command runs from the repository folder. On Windows use Git Bash or PowerShell; `docker` must be on the PATH (Docker Desktop adds it).

## 1. The commands you will use

| Goal | Command |
|---|---|
| Start simulation only (no OpenMeter, no real faults) | `docker compose up --build -d` |
| Start the real stack | `docker compose -f docker-compose.yml -f docker-compose.real.yml up --build -d` |
| See what is running | `docker compose -f docker-compose.yml -f docker-compose.real.yml ps` |
| Stop everything, keep data | `docker compose -f docker-compose.yml -f docker-compose.real.yml down` |
| Stop everything and wipe data (clean experiment list, fresh databases) | `docker compose -f docker-compose.yml -f docker-compose.real.yml down -v` |
| Backend logs | `docker logs chaosai-backend-1 --tail 50` |
| Controller logs | `docker logs chaosai-controller-1 --tail 50` |
| OpenMeter logs | `docker logs chaosai-openmeter-1 --tail 50` |
| Restart one service | `docker compose -f docker-compose.yml -f docker-compose.real.yml restart backend` |
| Rebuild after a code change | `docker compose -f docker-compose.yml -f docker-compose.real.yml up -d --build backend` (or `controller`, `dashboard`) |
| Run the tests | `pytest -q` (needs `pip install -r backend/requirements.txt`) |

Use `-f docker-compose.yml` alone for simulation; add `-f docker-compose.real.yml` for the real stack. Mixing them without the second file leaves the real containers running.

## 2. Where things live

| What | Where |
|---|---|
| Dashboard | <http://localhost:5173> |
| ChaosAI API and docs | <http://localhost:8000> and <http://localhost:8000/docs> |
| OpenMeter API | <http://localhost:48888> (only on this machine) |
| Secrets and settings | `.env` (never committed). Keys: `CHAOSAI_API_KEY`, `CHAOS_CONTROL_API_KEY`, `NVIDIA_API_KEY`, `NVIDIA_MODEL`, `REAL_INJECTIONS_ENABLED` |
| Experiment history | SQLite in the `chaosai_chaos_data` Docker volume. `down -v` deletes it |
| Controller code | `controller/chaos_controller/main.py` |
| Orchestration, safety, score | `backend/chaosai/orchestration.py`, `safety.py`, `metrics.py` |
| Observer and Analyzer | `backend/chaosai/insight/` |
| Sample app config | `docker-compose.real.yml`, `sample-app/openmeter-config.yaml` |

## 3. What each secret is

- `CHAOSAI_API_KEY`: a password you made up. It protects the ChaosAI API in real mode because that API can break containers. Paste it into the dashboard's key box.
- `CHAOS_CONTROL_API_KEY`: a password between ChaosAI and the controller. You never type it.
- `NVIDIA_API_KEY`: from build.nvidia.com. Used for planning and diagnosis.
- Change all three to new random values before you submit. Edit `.env`, then run the `up -d` command again.

## 3b. How to read the logs

Open Docker Desktop, Containers, the **chaosai** project, **Logs**. Use the search box (magnifier icon) and type one of the keywords below to see only the story.

**ChaosAI writes these lines on purpose. Search for them:**

| Search for | Container | Meaning |
|---|---|---|
| `EXPERIMENT` | backend | Stage changes: PLANNING, VALIDATING, BASELINING, INJECTING, ARMED, ACTIVE, ROLLING_BACK, VERIFYING_RECOVERY, COMPLETED or RECOVERY_FAILED |
| `WATCHDOG ARMED` | controller | The independent safety timer is set. Always appears before the fault |
| `FAULT ACTIVATED` | controller | The fault is really on, with evidence (`container_exited`, `toxic_active`, `container_restarted`, `load_generator_running`) |
| `LOAD` | controller | **Traffic spike only.** One line per second: `t=3s sent=60 ok=60 errors=0 (target 20/s)` |
| `SAFETY STOP` | backend | The automatic stop fired and says why (error rate or latency) |
| `ROLLING BACK` / `ROLLBACK VERIFIED` | controller | The fix, and proof that it worked (`healthy: True`, toxic removed, load stopped) |
| `REJECTED` | controller | The controller refused a plan, with the reason |

**What the victim and its neighbours print during a fault:**

| Fault | Where the error shows | What you will see |
|---|---|---|
| Crash OpenMeter or Kafka | The container in Docker Desktop turns red (Exited). Neighbours log it | `Connection refused`, `Failed to resolve 'kafka:9092'` in `openmeter` and `sink-worker` logs |
| Latency | Nothing in container logs. It is a network delay | Dashboard chart: latency jumps. controller: `toxic_active` |
| Traffic spike | controller `LOAD` lines | Per-second progress and the dashboard's "Load generated" box. Kafka and the sink worker stay quiet because they do not log individual messages |
| Restart a service | `container_restarted` in controller, startup lines in that container | The container restarts and turns healthy |

**Lines that look scary but are normal. Ignore them:**
- kafka: `In the last 60000 ms period, ... controller events were completed` appears once a minute, always.
- toxiproxy: `Source terminated ... use of closed network connection` appears when a proxied connection closes.
- sink-worker: `failed to subscribe to namespaces ... Timed out` repeats in the background even without a fault. Events are still processed.
- The `GET /healthz` and dashboard polling lines are now hidden from the backend and dashboard logs.

**Command-line version of the story view (PowerShell):**
```powershell
docker compose -f docker-compose.yml -f docker-compose.real.yml logs -f controller backend | Select-String "EXPERIMENT|WATCHDOG|FAULT|LOAD|SAFETY|ROLL|REJECT"
```

## 4. Symptoms and fixes

| Symptom | Likely cause | Fix |
|---|---|---|
| Dashboard says "Connecting" forever or "Unavailable" | Backend not up, or port 8000 busy | `docker compose ... ps`; `docker logs chaosai-backend-1`; free the port or set `CHAOSAI_HOST_API_PORT` in `.env` |
| Dashboard shows 401 or empty data in real mode | API key missing or wrong | Paste `CHAOSAI_API_KEY` from `.env` into the key box and click Connect |
| "Controller: Unavailable" | Controller down or key mismatch | `docker logs chaosai-controller-1`. Make sure `CHAOS_CONTROL_API_KEY` is set in `.env`, then `up -d` again |
| "OpenMeter: Unreachable" | OpenMeter still starting, or crashed | Wait 60 s. `docker logs chaosai-openmeter-1`. If it says it cannot reach `svix`, the svix container is down: `docker compose ... up -d svix openmeter` |
| Run button disabled | An experiment is active, or controller/OpenMeter not reachable | Wait for the active one, or press Emergency abort |
| "Real injections require REAL_INJECTIONS_ENABLED=true" | Safety switch is off | Set `REAL_INJECTIONS_ENABLED=true` in `.env`, then `up -d backend`. Only do this in your own sandbox |
| Experiment FAILED in PLANNING with "NVIDIA inference failed" | LLM unreachable, key or model invalid, reply cut off | Check internet. Test: `curl -H "Authorization: Bearer $NVIDIA_API_KEY" https://integrate.api.nvidia.com/v1/models`. If the model returns 410 or 404 it was retired or is not on your account: list models, pick a working one, set `NVIDIA_MODEL`, restart backend |
| Planning fails but nothing was injected | Real mode fails closed by design | Just retry. Nothing ran |
| Experiment stuck in ACTIVE or ROLLING_BACK | Controller waiting for a service to become healthy | Wait up to 2 minutes (Kafka and OpenMeter are slow to restart). Press Emergency abort if it persists |
| A container is left stopped after a run | Rollback did not complete | `docker compose ... up -d`. The controller also heals allowlisted services when it restarts |
| Responses are still slow after a latency test | A Toxiproxy toxic is left | `docker compose ... restart controller` (it removes leftover toxics on startup) |
| `kafka` is `unhealthy` | Slow start, or the health command fails | Wait 60 s. `docker logs chaosai-kafka-1`. The check in `docker-compose.real.yml` unsets the JMX variables; do not remove that |
| Image pull fails with DNS or timeout errors | Network change (for example Wi-Fi to hotspot) | Retry. Docker keeps finished layers, so retrying resumes. If it persists, restart Docker Desktop |
| `docker` not found | Docker Desktop not running or not on PATH | Start Docker Desktop and wait for "Engine running". In Git Bash: `export PATH="$PATH:/c/Program Files/Docker/Docker/resources/bin"` |
| Out of memory or very slow | Real stack needs about 4 to 6 GB RAM | Close other apps. Docker Desktop, Settings, Resources: give it 6 GB or more |
| Score shows "Simulated run" | You are in mock mode | Expected. Use the real stack for measured scores |
| Experiment ends RECOVERY_FAILED but all containers are healthy | The service came back, but usage events were not counted within the window | Check the meter directly: `curl "localhost:48888/api/v1/meters/api_requests_total/query?subject=chaosai-<experiment_id>"`. If the count catches up later it was lag (ChaosAI now waits up to 120 s and reports "Recovered late"). If it never does, events were lost |
| Diagnosis says "uncertain" | The measurements did not point to a clear cause | That is the analyzer being honest. A Kafka outage that leaves the health endpoint green is one such case |
| Controller refuses a plan | Target or limit not allowed | Targets: openmeter, sink-worker, kafka, clickhouse, redis, postgres. Limits: 30 s, 2000 ms, 20 rps. Latency only on `openmeter`. Traffic spike only on `openmeter` |

## 5. Emergency procedures

**Stop a running fault right now:** dashboard Emergency abort. If the dashboard is unavailable:
```bash
curl -X POST -H "Authorization: Bearer <CHAOSAI_API_KEY>" http://localhost:8000/api/experiments/<experiment_id>/abort
```

**Everything is broken and you must demo in 2 minutes:** use simulation.
```bash
docker compose -f docker-compose.yml -f docker-compose.real.yml down
docker compose up -d     # with MODE=mock in .env
```
Set `MODE=mock` in `.env` first. The dashboard shows SIMULATION in the corner. Say so out loud.

**Reset to a clean state:** `docker compose -f docker-compose.yml -f docker-compose.real.yml down -v`, then the `up --build -d` command. First start after a wipe takes about 2 minutes.

## 6. Health checks in one go

```bash
docker compose -f docker-compose.yml -f docker-compose.real.yml ps          # 12 services, all healthy or Up
curl -s -H "Authorization: Bearer <CHAOSAI_API_KEY>" localhost:8000/api/health
curl -s -H "Authorization: Bearer <CHAOSAI_API_KEY>" localhost:8000/api/capabilities
curl -s -H "Authorization: Bearer <CHAOSAI_API_KEY>" localhost:8000/api/config
```
Healthy output: `mode: real`, `controller_authenticated: true`, `openmeter_reachable: true`, `network_latency_ready: true`.

## 7. Glossary for the judges

- **Chaos engineering:** deliberately breaking parts of a system to learn whether it survives and recovers.
- **Blast radius:** everything that depends on the broken part.
- **Watchdog:** a timer in the controller that rolls a fault back on its own, even if ChaosAI disappears.
- **Rollback evidence:** proof returned by the controller that the fault is gone (container healthy, toxic removed, load stopped).
- **Resilience score:** 0 to 100 from measured components only; unmeasured parts are shown as unmeasured, never as zero.
- **Grounding rate:** share of the LLM's claims that cite evidence the code actually computed.
- **Fail closed:** when unsure, do nothing. Real mode will not inject a fault it could not plan and validate.
