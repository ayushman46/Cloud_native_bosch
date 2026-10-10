"""Deterministic measurements. None means no measurement, never an inferred zero."""
from math import ceil
from .schemas import Probe, SyntheticEvent


def phase_metrics(probes: list[Probe]) -> dict:
    latencies = sorted(p.latency_ms for p in probes if p.latency_ms is not None)
    availability = sum(p.reachable for p in probes) / len(probes) if probes else None
    return {
        'samples': len(probes),
        'availability': availability,
        'error_rate': 1 - availability if availability is not None else None,
        'p95_latency_ms': latencies[ceil(.95 * len(latencies)) - 1] if latencies else None,
        'throughput_rps': None,
        'processing_lag_ms': None,
        'cpu_percent': None,
        'memory_mb': None,
    }


def summarize(probes: list[Probe], events: list[SyntheticEvent], metered_count: float | None, timestamps: dict | None = None) -> dict:
    timestamps = timestamps or {}
    real = [p for p in probes if not p.simulated]
    result = {phase: phase_metrics([p for p in real if p.phase == phase]) for phase in ('baseline', 'fault', 'recovery')}
    accepted = sum(e.accepted for e in events if not e.simulated)
    has_events = any(not e.simulated for e in events)
    measured = has_events and metered_count is not None
    result['metering'] = {
        'attempted_events': sum(not e.simulated for e in events) if has_events else None,
        'accepted_events': accepted if has_events else None,
        'metered_count': metered_count if measured else None,
        'missing_events': max(0, accepted - metered_count) if measured else None,
        'duplicate_events': max(0, metered_count - accepted) if measured else None,
        'metering_accuracy': max(0.0, 1 - abs(metered_count - accepted) / accepted) if measured and accepted else None,
    }
    activated = timestamps.get('fault_activated_at')
    baseline = result['baseline']['p95_latency_ms']
    anomalies = [p for p in real if p.phase == 'fault' and (not p.reachable or (baseline is not None and p.latency_ms is not None and p.latency_ms > max(100, baseline * 2)))]
    detection = min((p.at for p in anomalies if activated and p.at >= activated), default=None)
    recovered, rolled_back = timestamps.get('functional_recovery_at'), timestamps.get('rollback_completed_at')
    result['recovery'] |= {
        'detection_latency_ms': max(0, (detection - activated).total_seconds() * 1000) if detection and activated else None,
        'functional_recovery_seconds': max(0, (recovered - rolled_back).total_seconds()) if recovered and rolled_back else None,
    }
    result['agent_performance'] = {
        'planner_valid_plan_rate': None, 'injection_success_rate': None,
        'rollback_success_rate': None, 'detection_rate': None,
        'mean_time_to_detect_ms': None, 'root_cause_accuracy': None,
        'safety_violations': None,
    }
    return result
