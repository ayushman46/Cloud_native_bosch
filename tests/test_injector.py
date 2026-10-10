import asyncio
import httpx
import pytest
from pydantic import ValidationError
from chaosai.controller import RemoteController, MockController, ControllerError
from chaosai.schemas import ControllerStatus, Plan


def plan():
    return Plan(target_service='openmeter',failure_type='application_crash',parameters={'duration_seconds':1,'observation_seconds':1},reason='Test an outage',expected_impact='Temporary unavailability',required_observations=['health'])

@pytest.mark.parametrize('fault', ['application_crash','network_latency','service_restart','traffic_spike'])
def test_remote_dispatch_contract(cfg,fault):
    async def run():
        cfg.chaos_control_url='http://controller.test'; cfg.chaos_control_api_key='test-secret'
        p=plan(); p.failure_type=type(p.failure_type)(fault)
        if fault=='network_latency': p.parameters.latency_ms=10
        if fault=='traffic_spike': p.parameters.requests_per_second=2
        calls=[]
        def handle(r):
            calls.append(r)
            assert r.headers['authorization']=='Bearer test-secret'
            assert r.headers['idempotency-key']==p.experiment_id
            return httpx.Response(200,json={'experiment_id':p.experiment_id,'status':'armed','watchdog_armed':True})
        c=RemoteController(cfg); await c.client.aclose()
        c.client=httpx.AsyncClient(base_url=cfg.chaos_control_url,headers={'Authorization':'Bearer test-secret'},transport=httpx.MockTransport(handle))
        try:
            assert (await c.submit(p)).watchdog_armed
            assert len(calls)==1
        finally: await c.close()
    asyncio.run(run())

@pytest.mark.parametrize('failure', [401,403,500,'timeout','refused'])
def test_controller_failures(cfg,failure):
    async def run():
        cfg.chaos_control_url='http://controller.test'
        def handle(r):
            if failure=='timeout': raise httpx.ReadTimeout('timeout',request=r)
            if failure=='refused': raise httpx.ConnectError('refused',request=r)
            return httpx.Response(failure)
        c=RemoteController(cfg); await c.client.aclose()
        c.client=httpx.AsyncClient(base_url=cfg.chaos_control_url,transport=httpx.MockTransport(handle))
        try:
            with pytest.raises(ControllerError): await c.health()
        finally: await c.close()
    asyncio.run(run())

def test_uncertain_submit_queries_status_without_reposting(cfg):
    async def run():
        cfg.chaos_control_url='http://controller.test'; p=plan(); methods=[]
        def handle(r):
            methods.append(r.method)
            if r.method=='POST': raise httpx.ReadTimeout('after dispatch',request=r)
            return httpx.Response(200,json={'experiment_id':p.experiment_id,'status':'armed','watchdog_armed':True})
        c=RemoteController(cfg); await c.client.aclose()
        c.client=httpx.AsyncClient(base_url=cfg.chaos_control_url,transport=httpx.MockTransport(handle))
        try:
            assert (await c.submit(p)).experiment_id==p.experiment_id
            assert methods==['POST','GET']
        finally: await c.close()
    asyncio.run(run())

@pytest.mark.parametrize('endpoint', ['status','abort'])
def test_wrong_id_rejected(cfg,endpoint):
    async def run():
        cfg.chaos_control_url='http://controller.test'
        c=RemoteController(cfg); await c.client.aclose()
        c.client=httpx.AsyncClient(base_url=cfg.chaos_control_url,transport=httpx.MockTransport(lambda r:httpx.Response(200,json={'experiment_id':'wrong','status':'armed'})))
        try:
            with pytest.raises(ControllerError): await getattr(c,endpoint)('expected')
        finally: await c.close()
    asyncio.run(run())

@pytest.mark.parametrize('field',['injection_verified','rollback_verified'])
def test_verified_booleans_require_evidence(field):
    with pytest.raises(ValidationError): ControllerStatus(experiment_id='exp_test',status='active',**{field:True})

def test_mock_watchdog_without_polling_and_duplicate_conflict(cfg):
    async def run():
        c=MockController(cfg); c.activation_delay_seconds=.01; c.rollback_delay_seconds=.01
        p=plan()
        await c.submit(p)
        changed=p.model_copy(deep=True); changed.parameters.duration_seconds=2
        with pytest.raises(ControllerError): await c.submit(changed)
        await asyncio.sleep(1.05)
        status=await c.status(p.experiment_id)
        assert status.rollback_verified and status.activation_evidence and status.rollback_evidence
        await c.close()
    asyncio.run(run())
