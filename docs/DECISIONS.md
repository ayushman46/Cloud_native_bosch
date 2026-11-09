# Decision log

Short records of the choices that shaped the project and why. Newest last.

| # | Decision | Why | Alternative rejected |
|---|---|---|---|
| 1 | Sample app is **OpenMeter** (Apache-2.0), run from its published images | Public, free, containerized, multi-service (API, Kafka, ClickHouse, Postgres, Redis, workers), has a real data pipeline whose correctness we can check (is usage still metered?) | Cal.com: 6 GB, heavy Node stack, nothing in it measurable as simply |
| 2 | **One backend package** (`backend/chaosai/`), Observer and Analyzer under `insight/` | Two overlapping backends made it unclear which was the product | Keeping two services and an HTTP hop between them |
| 3 | **Chaos Control is a separate service** (`controller/`) and the only one with Docker access | The AI planner must never hold the Docker socket. The controller enforces its own allowlist, limits and watchdog | Giving the backend the Docker socket |
| 4 | **Watchdog armed before any fault**, and a fault only counts when the controller returns evidence | A POST that returns 200 proves nothing; a crash that is never rolled back is the worst outcome | Trusting the HTTP status |
| 5 | **Latency via Toxiproxy**, traffic via a bounded in-controller generator, crash/restart via the Docker API | Each is the smallest tool that gives verifiable evidence (toxic present, container exited, load counters) | `tc netem` inside containers (needs NET_ADMIN), external k6 |
| 6 | **Mock mode is the default** and every simulated value is labelled | Anyone can run the whole workflow in 5 minutes without Docker-in-Docker. A simulated score is marked `basis: simulated` | Silent simulation |
| 7 | **Resilience score weights**: availability under fault 35, recovery speed 20, usage metered correctly 20, latency impact 15, rollback verified 10. Unmeasured components are left out, not scored as zero, and coverage is reported | Availability and data correctness matter most for a metering system. A missing measurement must not look like a failure or a pass | A single opaque number |
| 8 | **Automatic stop** when 80% of the last 3 fault-phase probes fail or any is slower than 5 s | The brief asks for a stop on error rate, latency or blast radius. Thresholds are settings, the stop uses the controller's verified rollback | A fixed duration only |
| 9 | **Failed-dependency experiments** are crash/restart on Kafka, ClickHouse, Redis, Postgres, all on an allowlist | Covers the brief's "failed dependency" example | A separate fault type |
| 10 | **Planner prompt carries the dependency map and past results** | The brief says to choose the most informative experiment from the map and observed behavior | Letting the LLM guess from the goal text |
| 11 | **LLM: `nvidia/nemotron-3-super-120b-a12b`** | The earlier default `meta/llama-3.3-70b-instruct` was retired on 2026-08-26 (HTTP 410). Tested with the team key: this model returns valid JSON in about 3 s | `openai/gpt-oss-20b` also worked; larger model preferred |
| 12 | **License: MIT** | The brief requires an open-source license; MIT is the simplest | Apache-2.0 |
| 13 | **Dependency map = documented data flows + `depends_on` read from the running containers** | The brief wants mapping done automatically or semi-automatically | A hand-drawn picture only |
