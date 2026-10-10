# Hacker Agent — Shared Observer Contract

## Why this exists
This is the shared interface between the Observer Agent, Orchestrator, Independent Safety Controller, Injector, and Analyzer/Recommender Agent. Commit this folder to the team's repository and have both contributors import the same model definitions instead of recreating field names locally.

## Scope note
The current architecture diagram targets `Cal.com + PostgreSQL`. `target_app` is intentionally generic so the same Observer interface can later support Bosch SUNRISE and other repositories.

## First implementation
1. Orchestrator creates an `ExperimentContext` with unique `experiment_id` and `run_id`.
2. Observer collects read-only `MetricSample` records during `baseline`, `injection`, and `recovery` phases.
3. Observer emits `ObserverEvent` records when a health state changes or a threshold is crossed.
4. Safety Controller independently watches critical metrics and can stop/rollback without waiting for the Observer or an LLM.
5. Observer produces an `ObserverReport` with baseline/during-fault/recovery summaries, threshold breaches, recovery verification, and evidence references.
6. Analyzer/Recommender consumes the report; it should not invent metrics or directly execute fixes.

## Initial collection sources
- `http_probe`: synthetic requests to the sandbox application; record status codes and latency.
- `docker`: container state, CPU and memory; read only unless using a separately approved injector.
- `postgres`: connection/health checks and connection count, where permitted.
- `app_log`: errors and application-specific failure signals.
- `prometheus`: optional if the sandbox exposes metrics.

## Shared variable rules
- `experiment_id` identifies a logical experiment; `run_id` identifies one actual attempt. Generate new IDs for retries.
- All timestamps must be timezone-aware UTC.
- Percentages are always `0..100`, never fractions.
- `None` means unknown/unavailable; never convert missing telemetry to `0` or `healthy`.
- Keep `target_app`, `target_profile`, and `fault_type` explicit for every report.
- Evidence references should be locators (log path, sample ID, trace ID), not huge log payloads.
- Thresholds are per-target configuration; don't hardcode universal limits into the Observer.
- `schema_version` must be updated for breaking changes. Coordinate changes before merging.

## Safety contract
The Observer is read-only. It may emit a critical event or recommend stopping, but it must not perform Docker actions itself. The Independent Safety Controller must obtain critical signals independently or through a reliable watchdog path; it must not depend solely on an LLM or the Observer process being healthy.

## Install and use
```bash
pip install 'pydantic>=2.5'
```

Example:
```python
from observer_contract import (
    ExperimentContext, FaultType, ThresholdPolicy, MetricSample,
    ExperimentPhase, HealthState, ObserverReport,
)

context = ExperimentContext(
    experiment_id="exp-20261010-001",
    run_id="run-001",
    target_app="cal_diy",
    target_profile="cal_diy_local_v1",
    sandbox_id="local-compose-001",
    target_services=["web", "postgres"],
    target_service="web",
    fault_type=FaultType.NETWORK_DELAY,
    thresholds=ThresholdPolicy(
        max_duration_seconds=10,
        max_http_5xx_rate_pct=10,
        max_p95_latency_ms=1500,
    ),
)

sample = MetricSample(
    experiment_id=context.experiment_id,
    run_id=context.run_id,
    service="web",
    source="http_probe",
    health=HealthState.HEALTHY,
    http_5xx_rate_pct=0,
    p95_latency_ms=240,
)

print(context.model_dump_json(indent=2))
print(sample.model_dump_json(indent=2))
```

## Ownership suggestion for two contributors
- Contributor A (Observer): metric collectors, time-series samples, phase summaries, event detection, `ObserverReport` generation.
- Contributor B (Injector/Safety integration): shared `ExperimentContext`, fault adapter, policy thresholds, stop/rollback interface.
- Both import `observer_contract.py`. One contributor owns changes to the contract; changes are reviewed before merge.
