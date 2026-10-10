# Observer & Analyzer design notes

## Metric definitions (implemented in `app/monitoring/metrics.py`, `meter_probes.py`)

| Metric | Definition |
|---|---|
| availability | OK health probes / scheduled health probes of the API (`GET /api/v1/debug/metrics`, the endpoint the official compose healthcheck uses) |
| error_rate | infrastructure-failed ingest requests / attempted ingest requests. Failures: 5xx, 429/408, refused, DNS, connect/read timeout, reset, unparsable body. Other 4xx (and a non-204 on ingest) mean *our probe is wrong*: not counted in error_rate, but the probe is not "OK" and an observer note is added |
| p95_latency_ms | nearest-rank p95 of ingest request durations. Timeouts count at their elapsed time (right-censored); fast local failures (refused/DNS/reset) are excluded. Needs >= 5 requests (configurable) else `null` |
| throughput_rps | OK (204) ingest requests / window seconds |
| processing_lag_ms | acceptance (204) -> visible in `GET /api/v1/meters/{slug}/query` for the experiment's own subject. Phase value = p95 over events accepted in that phase; sample value = max over events that became visible in the window or are certainly still pending at its end |
| missing_events | accepted events not reflected in the meter after the drain deadline (`drain_deadline_s`) |
| duplicate_events | meter count above accepted + ambiguous events |
| metering_accuracy | `1 - (missing + duplicates) / accepted` (`null` if nothing accepted or the meter was never read) |
| detection_latency_ms | first qualifying anomaly time - `fault_activated_at` (Injector's timestamp). Anomalies before activation are counted as pre-fault false positives |
| functional_recovery_seconds | `rollback_completed_at` -> first moment ALL hold: k consecutive OK probes (started after the reference) of every health target and of ingestion, AND an event accepted after the reference is visible in the meter (backlog drained). "Container is up" is never enough |

Detection rules and defaults: `app/monitoring/detection.py` (consecutive failures, error rate, availability, baseline-aware p95 and lag: `max(floor, factor x baseline)`; minimum request counts per window).

## Honest approximations

- The meter query returns an aggregate, not per-event status. Event n is "visible" at the first meter read with count >= n (FIFO assumption), so lag resolution = meter poll interval.
- Ingest requests that fail ambiguously (read timeout, reset, 5xx) may or may not be enqueued: tracked as `uncertain_events`, never reported as duplicates.
- The meter must be COUNT, or SUM of a property the probe sets to `value_per_event`. Default meter: `api_requests_total`, type `request`.
- CPU / memory / Kafka consumer lag: not collected by default (no Docker access in the Observer). Pass a `resource_probe` to `ExperimentMonitor`; until then they are `null`.
- Injection and observer clocks are assumed to be the same host/NTP. Future timestamps beyond 1 s are clamped with a note.

## Additions beyond the shared contract (all additive; coordinate before relying on them)

- `ObservationReport`: `lifecycle`, `monitoring_coverage`, `observer_notes`; `PhaseMetrics.timeout_rate`, `service_availability`, `request_count`...; `RecoverySummary.functional_recovery_confirmed`, `accepted_events`, `uncertain_events`, `metrics`.
- `AnalysisReport`: `recovery`, `grounding`, `validation_notes`, `generation_latency_ms`, `diagnosis.evidence_refs`.
- `suspected_failure` may also be `uncertain` or `no_fault_detected` (needed for healthy controls).
- `schemas/injection.py` (`status` is a free string; the contract only shows `"active"`), `config.py`, `main.py`, `container.py`, `storage/repository.py` were created because the repo was empty. Replace/merge with the team's versions.

## Status of components

| Component | State |
|---|---|
| Models, probes, metrics, detection, lifecycle, report, API | implemented, tested against a fake OpenMeter |
| Analyzer, NVIDIA client, evidence grounding, evaluation metrics | implemented, tested with mocked NVIDIA |
| Real OpenMeter run | **not done** (no Docker on the dev machine yet). Unverified: ingest `Content-Type: application/cloudevents+json` acceptance, meter query row shape with a `subject` filter, sink dedupe on `source`+`id`, whether `api_requests_total` counts our probe events as 1 each |
| Real NVIDIA call | **not done**: model id `meta/llama-3.3-70b-instruct` and JSON-mode support are unverified for the account |
| LangGraph wiring | not mine; call the API or the two classes above |
| Evaluation dataset | none yet: `app/evaluation/metrics.py` returns counts with every rate; do not quote accuracy before real runs |
