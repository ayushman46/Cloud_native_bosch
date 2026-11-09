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


WEIGHTS = {'availability_during_fault': 35, 'latency_impact': 15, 'recovery_speed': 20, 'data_integrity': 20, 'rollback_verified': 10}
GRADES = [(90, 'A'), (75, 'B'), (60, 'C'), (40, 'D'), (0, 'F')]


def _clamp(x: float) -> float:
    return max(0.0, min(1.0, x))


def resilience_score(probes: list[Probe], events: list[SyntheticEvent], metered_count: float | None, timestamps: dict | None, rollback_verified: bool) -> dict:
    """0-100 score from measured components only. Components without data are listed as unmeasured, not scored as zero.
    basis is 'simulated' when any probe is simulated, so a mock run can never be mistaken for a measurement."""
    simulated = any(p.simulated for p in probes)
    m = {phase: phase_metrics([p for p in probes if p.phase == phase]) for phase in ('baseline', 'fault', 'recovery')}
    timestamps = timestamps or {}
    base_p95, fault_p95 = m['baseline']['p95_latency_ms'], m['fault']['p95_latency_ms']
    recovered, rolled_back = timestamps.get('functional_recovery_at'), timestamps.get('rollback_completed_at')
    accepted = sum(e.accepted for e in events)
    comps: dict[str, float | None] = {
        'availability_during_fault': m['fault']['availability'],
        'latency_impact': _clamp(1 - (fault_p95 / base_p95 - 1) / 9) if base_p95 and fault_p95 is not None else None,
        'recovery_speed': _clamp(1 - (max(0, (recovered - rolled_back).total_seconds()) - 2) / 28) if recovered and rolled_back else None,
        'data_integrity': _clamp(1 - abs(metered_count - accepted) / accepted) if events and accepted and metered_count is not None else None,
        'rollback_verified': 1.0 if rollback_verified else 0.0,
    }
    scored = {k: v for k, v in comps.items() if v is not None}
    total_weight = sum(WEIGHTS[k] for k in scored)
    score = round(sum(WEIGHTS[k] * v for k, v in scored.items()) / total_weight * 100, 1) if total_weight else None
    return {
        'score': score,
        'grade': next((g for floor, g in GRADES if score is not None and score >= floor), None),
        'basis': 'simulated' if simulated else 'measured',
        'coverage': round(total_weight / sum(WEIGHTS.values()), 2),
        'components': {k: {'value': None if v is None else round(v, 3), 'weight': WEIGHTS[k], 'points': None if v is None else round(WEIGHTS[k] * v, 1)} for k, v in comps.items()},
        'unmeasured': [k for k, v in comps.items() if v is None],
    }
