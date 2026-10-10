import asyncio
import pytest
from chaosai.agents import Planner, Analyzer
from chaosai.controller import MockController, ControllerError
from chaosai.llm import NvidiaClient
from chaosai.observer import Observer
from chaosai.orchestration import Runner
from chaosai.schemas import PlanRequest, Experiment, Status, Probe
from chaosai.storage import Store


def setup(cfg):
    c=MockController(cfg); c.activation_delay_seconds=.02; c.rollback_delay_seconds=.02
    o=Observer(cfg); llm=NvidiaClient(cfg)
    return Runner(cfg,Store(cfg.database_path),c,o,Planner(cfg,llm),Analyzer(cfg,llm)),c,o,llm

async def close(r,c,o,llm):
    await r.close(); await c.close(); await o.close(); await llm.close()

@pytest.mark.parametrize('fault',['application_crash','network_latency','service_restart','traffic_spike'])
def test_complete_four_faults(cfg,fault):
    async def run():
        r,c,o,llm=setup(cfg)
        try:
            e=await r.start(PlanRequest(goal='Test the authorized sandbox',preferred_fault=fault,preferred_target='sink-worker' if fault=='service_restart' else 'openmeter'))
            await r.tasks[e.experiment_id]
            done=r.store.get(e.experiment_id)
            assert done.status==Status.completed,done.error
            assert done.controller.injection_verified and done.controller.rollback_verified
            assert {'baseline','fault','recovery'} <= {p.phase for p in done.probes}
            assert done.report and done.metered_count==sum(x.accepted for x in done.events)
            assert Experiment.model_validate_json(done.model_dump_json())==done
            assert done.history[0]['status']=='PLANNING'
            if fault=='application_crash': assert any(not p.reachable for p in done.probes if p.phase=='fault')
            if fault=='network_latency': assert any(p.latency_ms>200 for p in done.probes if p.phase=='fault')
            if fault=='traffic_spike': assert any(p.latency_ms>100 for p in done.probes if p.phase=='fault')
            if fault=='service_restart': assert any(x['metered_count']<x['accepted_events'] for x in done.meter_samples if x['phase']=='fault')
        finally: await close(r,c,o,llm)
    asyncio.run(run())

def test_concurrent_start_and_abort(cfg):
    async def run():
        r,c,o,llm=setup(cfg)
        try:
            req=PlanRequest(goal='Test service restart')
            results=await asyncio.gather(r.start(req),r.start(req),return_exceptions=True)
            assert sum(isinstance(x,RuntimeError) for x in results)==1
            exp=next(x for x in results if isinstance(x,Experiment))
            await r.abort(exp.experiment_id)
            await r.tasks[exp.experiment_id]
            assert r.store.get(exp.experiment_id).status==Status.aborted
        finally: await close(r,c,o,llm)
    asyncio.run(run())

def test_abort_active_restores_controller(cfg):
    async def run():
        r,c,o,llm=setup(cfg)
        try:
            e=await r.start(PlanRequest(goal='Test service restart'))
            while r.store.get(e.experiment_id).status!=Status.active: await asyncio.sleep(.01)
            await r.abort(e.experiment_id); await r.tasks[e.experiment_id]
            done=r.store.get(e.experiment_id)
            assert done.status==Status.aborted and done.controller.rollback_verified
        finally: await close(r,c,o,llm)
    asyncio.run(run())

def test_activation_never_confirmed(cfg):
    async def run():
        r,c,o,llm=setup(cfg); c.activation_delay_seconds=10
        try:
            e=await r.start(PlanRequest(goal='Test service restart'))
            await r.tasks[e.experiment_id]
            done=r.store.get(e.experiment_id)
            assert done.status==Status.failed and done.controller.rollback_verified
            assert 'activation' in done.error.lower()
        finally: await close(r,c,o,llm)
    asyncio.run(run())

def test_rollback_failure_never_completes(cfg):
    async def run():
        r,c,o,llm=setup(cfg); c.rollback_delay_seconds=10
        original_abort=c.abort
        async def fail_abort(eid): raise ControllerError('rollback failed')
        c.abort=fail_abort
        try:
            e=await r.start(PlanRequest(goal='Test service restart'))
            await r.tasks[e.experiment_id]
            assert r.store.get(e.experiment_id).status==Status.recovery_failed
        finally:
            c.abort=original_abort
            await close(r,c,o,llm)
    asyncio.run(run())

def test_unhealthy_baseline_blocks_injection(cfg):
    async def run():
        r,c,o,llm=setup(cfg)
        async def unhealthy(phase): return [Probe(phase=phase,endpoint='test',reachable=False)]
        o.probe=unhealthy
        try:
            e=await r.start(PlanRequest(goal='Test service restart'))
            await r.tasks[e.experiment_id]
            assert r.store.get(e.experiment_id).status==Status.failed
            assert not c.states
        finally: await close(r,c,o,llm)
    asyncio.run(run())

def test_restart_reconciles_stale_record(cfg):
    async def run():
        r,c,o,llm=setup(cfg)
        try:
            e=Experiment(experiment_id='exp_interrupted',goal='Test interruption',status=Status.active,simulated=True)
            r.store.save(e)
            await r.reconcile_interrupted()
            assert r.store.get(e.experiment_id).status==Status.failed
            assert 'restart' in r.store.get(e.experiment_id).error
        finally: await close(r,c,o,llm)
    asyncio.run(run())
