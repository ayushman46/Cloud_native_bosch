# Observer & Analyzer (Bosch ChaosAI)

**The Observer measures what OpenMeter actually did under an injected fault (including whether usage was really metered); the Analyzer diagnoses the cause from those measurements without being told which fault was injected.**

Observer = deterministic Python + HTTPX, no LLM. Analyzer = NVIDIA-hosted LLM, validated with Pydantic, every claim tied to a computed evidence id.

## Run

```bash
cd chaos-platform/backend
pip install -r requirements.txt
cp .env.example .env            # NVIDIA_API_KEY only needed for the Analyzer
python -m pytest tests -q       # 76 tests, ~35 s, no Docker / network / NVIDIA credits
uvicorn app.main:app --reload
```

## Integration contract (what the Planner / Injector team calls)

| When | Call |
|---|---|
| Before injecting | `POST /api/monitoring/start {"experiment_id": "exp_001"}`; poll `GET /api/experiments/exp_001/metrics` until `baseline_ready: true` |
| Fault activated / rollback started / rollback completed | `POST /api/monitoring/exp_001/event` with the shared Injection Event JSON (resend as fields fill in) |
| After rollback | nothing: the Observer auto-finalizes once recovery is confirmed (or `recovery_deadline_s` passes). `POST .../finalize` forces it |
| Read results | `GET /api/experiments/{id}/metrics`, `GET /api/experiments/{id}/observations` |
| Diagnose | `POST /api/analysis {"experiment_id": ...}`, then `GET /api/analysis/{id}` |
| Score (after diagnosis) | `GET /api/analysis/{id}/evaluation` compares with the stored ground truth |

In Python: `ObserverAgent.start_baseline / handle_event / finalize`, `AnalyzerAgent.analyze(build_diagnostic_evidence(report))`.

**Injector must send `rollback_completed_at`.** Recovery time is measured from it. For `service_restart` set it to the moment the restart finished.

## Rules that matter

- Contract field names, the four fault ids, UTC timestamps, `_ms`, fractions for `_rate`, `null` for unavailable: all as agreed. Extensions are additive and listed in [docs/DESIGN.md](docs/DESIGN.md).
- The injected fault label is stored apart (`ExecutionGroundTruth`) and never enters the prompt or the report.
- NVIDIA down or no key: the Observer is unaffected; analysis returns `status: unavailable` with no invented report. The Analyzer cannot reach Docker or trigger rollback.

More: [docs/DESIGN.md](docs/DESIGN.md) (metric definitions, failure policy, approximations, status of each component).
