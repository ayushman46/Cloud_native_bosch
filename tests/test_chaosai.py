import asyncio
import pytest
from chaosai.config import Settings
from chaosai.schemas import Plan, Parameters, FaultType, PlanRequest, Status
from chaosai.safety import validate_plan, SafetyError
from chaosai.controller import MockController
from chaosai.storage import Store
from chaosai.observer import Observer
from chaosai.llm import NvidiaClient
from chaosai.agents import Planner, Analyzer
from chaosai.orchestration import Runner


def settings(tmp_path):
    return Settings(mode='mock', database_path=str(tmp_path/'test.db'), baseline_seconds=1, recovery_seconds=1, poll_seconds=.05)


def test_safety_rejects_non_allowlisted_and_excessive(tmp_path):
    s = settings(tmp_path)
    caps = {'targets':s.targets,'fault_types':[x.value for x in FaultType], 'watchdog_supported':True,'network_latency_ready':True,'traffic_spike_ready':True}
    plan = Plan(target_service='kafka', failure_type=FaultType.service_restart, parameters=Parameters(), reason='test target', expected_impact='some impact', required_observations=['health'])
    with pytest.raises(SafetyError): validate_plan(plan,s,caps)
    plan.target_service='openmeter'
    plan.parameters.duration_seconds=100
    with pytest.raises(SafetyError): validate_plan(plan,s,caps)
    plan.parameters.duration_seconds=10
    caps['watchdog_supported']=False
    with pytest.raises(SafetyError): validate_plan(plan,s,caps)


def test_mock_idempotent_and_abort(tmp_path):
    async def run():
        c = MockController(settings(tmp_path))
        p = Plan(target_service='sink-worker', failure_type=FaultType.service_restart, parameters=Parameters(duration_seconds=2), reason='test worker', expected_impact='some impact', required_observations=['health'])
        first=await c.submit(p)
        second=await c.submit(p)
        assert first.experiment_id==second.experiment_id and first.watchdog_armed
        aborted=await c.abort(p.experiment_id)
        assert aborted.rollback_verified
    asyncio.run(run())


def test_mock_end_to_end(tmp_path):
    async def run():
        s=settings(tmp_path)
        s.max_duration_seconds=2
        c=MockController(s); o=Observer(s); llm=NvidiaClient(s)
        runner=Runner(s,Store(s.database_path),c,o,Planner(s,llm),Analyzer(s,llm))
        exp=await runner.start(PlanRequest(goal='Test worker restart', preferred_fault=FaultType.service_restart, preferred_target='sink-worker'))
        await runner.tasks[exp.experiment_id]
        done=runner.store.get(exp.experiment_id)
        assert done.status==Status.completed, done.error
        assert done.controller.rollback_verified
        assert done.report.metrics['baseline']['samples']==0
        assert done.simulated
        await o.close(); await llm.close()
    asyncio.run(run())


def test_http_api_mock(tmp_path):
    from fastapi.testclient import TestClient
    from chaosai.api import create_app
    app = create_app(settings(tmp_path))
    with TestClient(app) as client:
        health = client.get('/api/health')
        assert health.status_code == 200
        assert health.json()['simulated'] is True
        preview = client.post('/api/experiments/plan', json={'goal':'Test worker restart', 'preferred_fault':'service_restart', 'preferred_target':'sink-worker'})
        assert preview.status_code == 200, preview.text
        assert preview.json()['failure_type'] == 'service_restart'
        missing = client.get('/api/experiments/not-found')
        assert missing.status_code == 404
        assert client.get('/api/experiments/not-found/observer-report').status_code == 404


def test_real_mode_requires_auth_and_remote_config(tmp_path):
    from fastapi.testclient import TestClient
    from chaosai.api import create_app
    s = Settings(mode='real', database_path=str(tmp_path/'test.db'), chaos_control_url='http://127.0.0.1:9', chaos_control_api_key='remote-secret', chaosai_api_key='dashboard-secret', openmeter_base_url='http://127.0.0.1:9')
    app = create_app(s)
    with TestClient(app) as client:
        assert client.get('/api/health').status_code == 401
        result = client.get('/api/health', headers={'Authorization':'Bearer dashboard-secret'})
        assert result.status_code == 200
        assert result.json()['controller_reachable'] is False
        assert result.json()['openmeter_reachable'] is False


def test_synthetic_mock_end_to_end(tmp_path):
    async def run():
        s=settings(tmp_path)
        s.enable_synthetic_events=True
        s.max_duration_seconds=2
        c=MockController(s); o=Observer(s); llm=NvidiaClient(s)
        runner=Runner(s,Store(s.database_path),c,o,Planner(s,llm),Analyzer(s,llm))
        exp=await runner.start(PlanRequest(goal='Test worker restart with event counts', preferred_fault=FaultType.service_restart, preferred_target='sink-worker'))
        await runner.tasks[exp.experiment_id]
        done=runner.store.get(exp.experiment_id)
        assert done.status==Status.completed, done.error
        assert done.metered_count == sum(e.accepted for e in done.events)
        assert len(done.events)>=2
        await o.close(); await llm.close()
    asyncio.run(run())


def test_openmeter_event_contract(tmp_path):
    import httpx
    async def run():
        s=settings(tmp_path)
        s.mode='real'; s.openmeter_base_url='http://openmeter.test'
        requests=[]
        def handle(request):
            requests.append(request)
            if request.method=='POST':
                assert request.url.path=='/api/v1/events'
                assert request.headers['content-type']=='application/cloudevents+json'
                return httpx.Response(204)
            assert request.url.path=='/api/v1/meters/api_requests_total/query'
            return httpx.Response(200,json={'data':[{'value':1}]})
        o=Observer(s)
        await o.client.aclose()
        o.client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
        event=await o.send_event('exp_test','baseline')
        assert event.accepted and event.event_id=='exp_test-baseline'
        assert await o.query_count('exp_test')==1
        assert len(requests)==2
        await o.close()
    asyncio.run(run())


def test_metering_mismatch_fails_recovery(tmp_path):
    async def run():
        s=settings(tmp_path)
        s.enable_synthetic_events=True
        s.max_duration_seconds=2
        c=MockController(s); o=Observer(s); llm=NvidiaClient(s)
        async def wrong_count(_): return 0.0
        o.query_count=wrong_count
        runner=Runner(s,Store(s.database_path),c,o,Planner(s,llm),Analyzer(s,llm))
        exp=await runner.start(PlanRequest(goal='Test worker restart with missing metering', preferred_fault=FaultType.service_restart, preferred_target='sink-worker'))
        await runner.tasks[exp.experiment_id]
        done=runner.store.get(exp.experiment_id)
        assert done.status==Status.recovery_failed
        assert done.controller.rollback_verified
        assert done.report is not None
        await o.close(); await llm.close()
    asyncio.run(run())
