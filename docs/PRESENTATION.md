# Presentation guide: what to show, what to say, how to defend it

Read this top to bottom once, then use the cheat sheet in section 9 on the day. Every number below came from a real run on this stack on 2026-10-10 (see section 8 for where each is stored). Never quote a number that is not in this file.

## 1. The pitch

**One sentence:** ChaosAI is an AI that decides which failure to inject into a running cloud-native app, injects it through a controller that always rolls back, measures what really broke, and explains it.

**30 seconds:** Chaos engineering means breaking parts of a system on purpose to see whether it survives. Doing it by hand is slow and risky. ChaosAI plans the experiment with an LLM that sees the dependency map and past results, then hands it to a separate controller with an allowlist, hard limits and its own watchdog. We measure latency, errors, recovery time and whether usage is still metered correctly, stop automatically if impact crosses a threshold, and produce a resilience score with an evidence-cited diagnosis. The sample app is OpenMeter, an open-source usage-metering platform with Kafka, ClickHouse, Postgres and Redis.

## 2. What the brief asked for and where we show it

| Brief requirement | What you show | Where |
|---|---|---|
| Pick and map a sample app | OpenMeter, 12 containers, dependency map built from the running containers | Dashboard "What depends on what" |
| Plan experiments intelligently | The LLM chose latency on the API because it "is a central ingress point with many downstream dependencies" | Dashboard "Preview plan" |
| Inject controlled failures with safety limits | Crash, restart, latency, traffic spike, failed dependency. Hard caps, allowlist, watchdog | Controller, `docs/BRIEF-MAP.md` |
| Stop automatically on a threshold | Auto-stop fired in the real crash run | Dashboard safety banner |
| Observe errors, slowdowns, recovery | Probe chart with baseline, fault, recovery windows | Dashboard "Measurements" |
| Analyze, recommend, ranked score | Resilience score with breakdown, LLM diagnosis, experiments ranked weakest first | Dashboard |
| Sandbox only | Loopback ports, allowlist, README rules section | README |

## 3. Why this is not a wrapper (the most important section)

A wrapper is a UI in front of an LLM call: the model decides, and whatever it says happens. Here the LLM is two of about ten components, and it is **never trusted**.

| Question | A wrapper | ChaosAI |
|---|---|---|
| Who decides what runs? | The model's text | The model proposes. Pydantic validates the shape, then a safety validator checks target, fault, duration, latency and rate against allowlists and limits. It can reject the plan |
| Who touches Docker? | The model, or code it wrote | A separate controller. The model never sees the Docker socket |
| How do we know the fault happened? | The API returned 200 | The controller must return evidence (container exited, toxic present, load counters). No evidence, no experiment |
| What if it breaks things and nobody is watching? | Stays broken | A watchdog is armed before the fault and rolls back on its own deadline, even if ChaosAI crashes |
| Who measures impact? | The model's opinion | Deterministic probes: latency percentiles, availability, and whether events sent during the fault were actually metered |
| Who computes the score? | The model | Deterministic weighted formula. Missing measurements are excluded, never scored as zero |
| Can the model invent facts in the report? | Yes | Every claim must cite an evidence id the code computed. Unsupported claims are dropped and the grounding rate is reported. The injected fault label is withheld from the model |
| What if the LLM is down? | Nothing works | Real mode refuses to run an unplanned fault (it fails closed, on purpose). If the LLM fails after a run, measurement and scoring are unaffected and the report falls back to a deterministic summary that says so. Mock mode runs with no LLM at all |

**Three live proofs you can show in under a minute:**
1. `docker ps` before and after an experiment: the OpenMeter container really exits and returns healthy. A wrapper cannot do that.
2. Dashboard "Fault verified" shows the controller's evidence type. Open the downloaded report JSON and show `activation_evidence` and `rollback_evidence`.
3. Run mock mode (`docker compose up`, no LLM involved): the whole workflow, safety checks, measurement and score still run, all labelled as simulated. In real mode, an LLM failure after the run still leaves the measurements and the score intact; only the narrative falls back to "Source: deterministic". That shows the LLM is a component, not the whole product.

**One-line answer if challenged:** "The LLM chooses and explains. Code proves, limits, measures and rolls back."

## 4. Slide outline (7 to 8 minutes)

1. **Problem (30 s).** Resilience is tested by breaking things; by hand it is slow, unfocused and risky.
2. **Idea (30 s).** The pitch sentence from section 1, plus the flow: plan, validate, baseline, inject, observe, roll back, verify recovery, analyze.
3. **Architecture (60 s).** One diagram: dashboard, backend (LangGraph workflow, planner, observer, analyzer), controller (only Docker access), OpenMeter stack. Say: "The AI sits on the left, the power sits on the right, and a checked boundary sits between them."
4. **Safety (45 s).** Allowlist, hard limits (30 s, 2000 ms, 20 rps), watchdog armed first, evidence for every step, automatic stop, kill switch, loopback only.
5. **Live demo (2.5 min).** Script in section 5.
6. **Results (60 s).** The real numbers from section 8, including the honest Kafka finding.
7. **Honest limits and next steps (45 s).** Section 7.
8. **Close (15 s).** The pitch sentence again.

## 5. Live demo script

Before you start: stack is up (`docker compose -f docker-compose.yml -f docker-compose.real.yml up -d`), dashboard open on <http://localhost:5173>, API key pasted and Connect clicked, a second window with `docker ps` ready.

1. **Status strip (15 s).** Point at: Controller Connected, OpenMeter Reachable, AI planner NVIDIA LLM, fault limits, auto-stop threshold. Say: "Everything is real: these are live containers on this laptop."
2. **Dependency map (15 s).** "This map is read from the running containers, not drawn by hand. Dashed lines are discovered, solid lines are documented data flow. Red is the target, amber is everything that depends on it."
3. **Preview plan (30 s).** Pick "Add network latency", click Preview plan. Read the reason the LLM gives. Say: "It chose the target from the dependency map. Nothing has run yet."
4. **Run experiment (60 s).** Click Run. Narrate the timeline: baseline, fault activated, rollback, recovery verified. Point at "Watchdog armed: Yes" and "Fault verified: toxic_active". Show the chart: latency jumps, then returns.
5. **Kill switch (30 s).** Start "Restart the sink worker", and when it says ACTIVE press **Emergency abort**. Say: "This is the human override. The controller rolls back and proves it." Show Rollback verified: Yes and `docker ps` showing the worker healthy.
6. **Result (30 s).** Show the resilience score, the breakdown, the LLM diagnosis and the experiments ranked weakest first.

**If something fails live:** do not debug on stage. Say "Let me show the same flow in simulation mode", open the mock dashboard (`docker compose up`, port 5173 with the default `.env` setting `MODE=mock`) or the screenshots in `docs/`. The simulation is clearly labelled and runs without Docker-in-Docker.

## 6. Questions you will be asked

1. **"Is the LLM just a wrapper?"** Section 3.
2. **"What stops the AI breaking something it shouldn't?"** Three layers: Pydantic schema, local safety validator (allowlist and limits), and the controller re-enforces the same limits independently. The model has no Docker access.
3. **"What if ChaosAI crashes mid-experiment?"** The controller's watchdog was armed first and rolls back at its deadline. On restart ChaosAI reconciles interrupted experiments and the controller heals the sandbox.
4. **"How do you know the fault actually happened?"** The controller returns evidence. We reject the experiment if the evidence type or target does not match the plan.
5. **"How is the score computed? Is it standard?"** Weights: availability under fault 35, recovery speed 20, usage metered correctly 20, latency impact 15, rollback verified 10. It is our design, shown transparently with a per-component breakdown. Measurements that are missing are excluded and the coverage is shown.
6. **"Why is the LLM diagnosis trustworthy?"** Every claim cites a computed evidence id, claims without support are removed, the injected fault label is hidden from it, and a grounding rate is reported. When evidence is thin it says "uncertain" instead of guessing. That happened in our Kafka run.
7. **"Why OpenMeter?"** Public, Apache-2.0, containerized, multi-service, and it has a pipeline whose correctness we can check: events sent during a fault either end up metered or they do not.
8. **"Why is the controller separate?"** The only privileged component is small and auditable. The planner never holds the Docker socket.
9. **"What does the automatic stop watch?"** The last two fault-phase probes: stop when 80% fail or any probe is slower than 5 s. Thresholds are settings. After the stop the controller rolls back and recovery is still verified.
10. **"What is simulated?"** Only mock mode, and every simulated value is labelled in the dashboard, the report and the score (`basis: simulated`).
11. **"Does it generalize beyond OpenMeter?"** The controller, safety layer, orchestrator and score are application-agnostic. The dependency map, probes and metering check are OpenMeter-specific today. Another compose app needs a probe config and a map.
12. **"What would you do with another week?"** Section 7.

## 7. Honest limits (say them before anyone asks)

- Four real runs so far, small probe counts (2 to 10 per window). These show the system works. They are not a statistical benchmark.
- With Kafka stopped, OpenMeter's health endpoint stayed green. The two Kafka runs differ: run 1 metered all 3 test events after 55 s; run 2 accepted events that were never metered. Two runs show the outcome varies, not why; do not claim a root cause or a loss rate. What you can say: a green health check hides a Kafka outage, accepted events are not guaranteed to be metered afterwards, and ChaosAI measures exactly that instead of trusting the health endpoint. The analyzer said "uncertain" on run 1 because the evidence did not point to a cause, which is its honest behavior.
- The automatic stop is a threshold on probe results. The planner chose 10 s faults, and in our crash run the stop fired about 10 s after activation, which is when the fault was ending anyway. It is demonstrated end to end, but it did not shorten that run, so lead with the kill switch for the visual.
- The experiment list contains one FAILED crash run. That was a real bug we found during Day 1: the controller held the abort request open until OpenMeter was healthy and ChaosAI timed out. The fault itself was rolled back and verified. The controller now returns immediately and ChaosAI polls, and the next run passed. Say this if asked: it is evidence the system was tested for real. For a clean list on the day, run `docker compose -f docker-compose.yml -f docker-compose.real.yml down -v` and bring it back up.
- The blind Analyzer is not reliably right about the fault type. On the 2026-10-12 traffic-spike rerun it diagnosed `network_latency` on the sink worker (the real fault was a traffic spike), and its grounding filter dropped half of its claims. Latency rise looks the same for several faults. Present the Analyzer as evidence-grounded, not as an accurate root-cause detector, and never quote a diagnosis accuracy.
- On 2026-10-12 the Docker engine restarted mid-experiment on a memory-starved laptop. The run was recorded as `RECOVERY_FAILED` with the cause stated, which is the intended behaviour. The cure was capping the WSL VM at 5 GB.
- No full-disk or overloaded-database fault yet. Failed dependency is covered by stopping Kafka, ClickHouse, Redis or Postgres.
- Single laptop. Chaos Control mounts the Docker socket, which is why it is isolated and only reachable with its own key.

## 8. Evidence and numbers

Source: the experiments stored by the backend (`GET /api/experiments`, or the Download report button). IDs start with `exp_`.

| Run | Fault | What was measured | Result |
|---|---|---|---|
| Latency | 500 ms via Toxiproxy on the OpenMeter API | Probe p95 latency: baseline 14.6 ms, fault 528 ms, recovery 9.8 ms. Availability 100% throughout. Events metered 3 of 3 | Score 81.2 (B), 80% of the score weight measured, analysis from the NVIDIA model |
| Traffic spike | 15 usage events per second for 10 s sent to OpenMeter's ingest endpoint | Controller logs a progress line every second. 150 of 150 accepted, and all 150 appeared in the meter (checked directly). Probe p95 11.6 ms to 27.1 ms. Own test events metered 3 of 3 | Score 77.8 |
| Crash | OpenMeter container killed | Availability 100%, then 0% during the fault, then 100%. Detection 3.7 s. Rollback verified healthy | Score 46.2 (D) |
| Crash with auto-stop | Same | Stop fired: "error rate 100% over the last 2 probes reached the 80% limit". Controller aborted, rollback verified, recovery verified | Report completed |
| Kill switch | Sink worker restart, abort while ACTIVE | ABORTED within about 4 s, rollback verified, worker healthy | Pass |
| Failed dependency, run 1 | Kafka container killed | Health endpoint stayed green, 3 of 3 events metered, recovery 54.8 s | Score 79.8 (B), diagnosis "uncertain" |
| Failed dependency, run 2 | Same | Rollback verified and all 12 containers healthy again, but the 2 events OpenMeter accepted during the outage were never metered, even minutes later (checked directly against OpenMeter's meter API) | Status RECOVERY_FAILED: the tool reported the data problem instead of a pass |

Tests: 165 automated tests, no Docker, network or NVIDIA credit needed. Run `pytest -q`.

## 9. Cheat sheet for the day

**Start:** `docker compose -f docker-compose.yml -f docker-compose.real.yml up -d` then open <http://localhost:5173>. The API key is in `.env` as `CHAOSAI_API_KEY`. First start needs about 5 GB of images; do this the day before.

**Say:** "The LLM chooses and explains. Code proves, limits, measures and rolls back."

**Show:** map, preview plan, run, chart, kill switch, score.

**Never say:** that the score is an industry standard; that the runs are a benchmark; that anything in simulation was measured.

**If it breaks:** section 5 fallback, then `docs/TROUBLESHOOTING.md`.

**Checklist, one hour before:** Docker Desktop running; all 12 containers healthy (`docker compose ps`); dashboard shows Controller Connected and OpenMeter Reachable; one rehearsal experiment done; NVIDIA key valid (dashboard shows NVIDIA LLM); phone hotspot or Wi-Fi tested (the LLM needs internet); screenshots saved as a backup.
