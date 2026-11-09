import asyncio
from datetime import datetime, timedelta, timezone
from chaosai.metrics import resilience_score, WEIGHTS
from chaosai.schemas import PlanRequest, Probe, Status, SyntheticEvent
from test_workflow import setup, close

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def probes(base, fault, recovery, base_ms=10, fault_ms=10, simulated=False):
    out = []
    for phase, flags, ms in (('baseline', base, base_ms), ('fault', fault, fault_ms), ('recovery', recovery, base_ms)):
        out += [Probe(phase=phase, endpoint='x', reachable=ok, latency_ms=ms if ok else None, simulated=simulated) for ok in flags]
    return out


def test_perfect_run_scores_100_and_is_measured():
    r = resilience_score(probes([True] * 5, [True] * 5, [True] * 5), [SyntheticEvent(event_id='a', phase='x', accepted=True)], 1.0,
                         {'rollback_completed_at': T0, 'functional_recovery_at': T0 + timedelta(seconds=1)}, True)
    assert r['score'] == 100 and r['grade'] == 'A' and r['basis'] == 'measured' and r['coverage'] == 1 and not r['unmeasured']


def test_outage_scores_low_and_weights_add_up():
    assert sum(WEIGHTS.values()) == 100
    r = resilience_score(probes([True] * 5, [False] * 5, [True] * 5), [], None, {}, True)
    assert r['components']['availability_during_fault']['value'] == 0
    assert r['score'] < 60 and {'recovery_speed', 'data_integrity'} <= set(r['unmeasured'])
    assert r['components']['data_integrity']['value'] is None  # unmeasured, not zero


def test_unverified_rollback_costs_points_and_simulated_is_labelled():
    ok = resilience_score(probes([True] * 3, [True] * 3, [True] * 3, simulated=True), [], None, {}, True)
    bad = resilience_score(probes([True] * 3, [True] * 3, [True] * 3, simulated=True), [], None, {}, False)
    assert ok['basis'] == 'simulated' and ok['score'] > bad['score']


def test_slow_latency_reduces_latency_component():
    r = resilience_score(probes([True] * 5, [True] * 5, [True] * 5, base_ms=10, fault_ms=100), [], None, {}, True)
    assert r['components']['latency_impact']['value'] == 0


def test_no_data_means_no_score():
    r = resilience_score([], [], None, {}, False)
    assert r['components']['rollback_verified']['value'] == 0 and r['coverage'] == 0.1


def run_crash(cfg):
    async def go():
        r, c, o, llm = setup(cfg)
        try:
            e = await r.start(PlanRequest(goal='Test the authorized sandbox', preferred_fault='application_crash', preferred_target='openmeter'))
            await r.tasks[e.experiment_id]
            return r.store.get(e.experiment_id)
        finally:
            await close(r, c, o, llm)
    return asyncio.run(go())


def test_threshold_breach_stops_fault_early_with_verified_rollback(cfg):
    cfg.abort_min_probes = 2
    done = run_crash(cfg)
    assert done.status == Status.completed, done.error
    assert done.safety_stop and 'error rate' in done.safety_stop['reason']
    assert 'safety_stop_at' in done.timestamps
    assert done.controller.rollback_verified and done.controller.status == 'aborted'
    assert done.report.metrics['resilience']['score'] is not None


def test_auto_stop_can_be_disabled(cfg):
    cfg.safety_auto_stop = False
    done = run_crash(cfg)
    assert done.status == Status.completed, done.error
    assert done.safety_stop is None and done.controller.status == 'rolled_back'


def test_recovery_time_survives_to_the_report(cfg):
    done = run_crash(cfg)
    assert done.status == Status.completed, done.error
    assert 'functional_recovery_at' in done.timestamps
    rec = done.report.metrics['recovery']['functional_recovery_seconds']
    assert rec is not None and rec >= 0
    assert 'recovery_speed' not in done.report.metrics['resilience']['unmeasured']
