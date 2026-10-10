import asyncio
import httpx
import pytest
from pydantic import ValidationError
from chaosai.agents import Planner
from chaosai.controller import MockController
from chaosai.llm import NvidiaClient, LLMError
from chaosai.schemas import PlanRequest, Plan, Parameters
from chaosai.safety import SafetyError, validate_plan

GOALS = [
 ('application_crash','Test OpenMeter ingestion availability during an application crash.','openmeter'),
 ('network_latency','Test OpenMeter behavior when Kafka communication becomes slow.','openmeter'),
 ('service_restart','Test whether usage events survive a sink-worker restart.','sink-worker'),
 ('traffic_spike','Test OpenMeter ingestion under elevated request traffic.','openmeter'),
]

@pytest.mark.parametrize('fault,goal,target', GOALS)
def test_planner_four_goals(cfg,fault,goal,target):
    async def run():
        llm=NvidiaClient(cfg)
        try:
            plan=await Planner(cfg,llm).plan(PlanRequest(goal=goal),await MockController(cfg).capabilities())
            assert plan.failure_type==fault and plan.target_service==target
            assert plan.parameters.duration_seconds<=cfg.max_duration_seconds
            assert Plan.model_validate_json(plan.model_dump_json())==plan
        finally: await llm.close()
    asyncio.run(run())

@pytest.mark.parametrize('field,value', [('target_service','unrelated'),('duration_seconds',500),('latency_ms',2001),('requests_per_second',21)])
def test_reject_unsafe_plan(cfg,field,value):
    async def run():
        caps=await MockController(cfg).capabilities()
        fault='network_latency' if field=='latency_ms' else 'traffic_spike' if field=='requests_per_second' else 'application_crash'
        params={'duration_seconds':1,'observation_seconds':1}
        if field in ('latency_ms','requests_per_second'): params[field]=value
        if field=='duration_seconds': params[field]=value
        p=Plan(target_service=value if field=='target_service' else 'openmeter',failure_type=fault,parameters=params,reason='Test safety policy',expected_impact='Temporary degradation',required_observations=['health'])
        with pytest.raises(SafetyError): validate_plan(p,cfg,caps)
    asyncio.run(run())

@pytest.mark.parametrize('parameters', [{'duration_seconds':0},{'command':'rm -rf /'},{'latency_ms':10}])
def test_strict_schema_rejects_bad_parameters(parameters):
    with pytest.raises(ValidationError):
        Plan(target_service='openmeter',failure_type='application_crash',parameters=parameters,reason='Testing plan',expected_impact='Temporary fault',required_observations=['health'])

@pytest.mark.parametrize('payload', ['not json', '[]', '{"choices":[]}'])
def test_nvidia_malformed(cfg,payload):
    async def run():
        cfg.nvidia_api_key='test-key'; cfg.nvidia_model='test-model'
        llm=NvidiaClient(cfg)
        await llm.client.aclose()
        llm.client=httpx.AsyncClient(base_url='https://nvidia.test/v1',transport=httpx.MockTransport(lambda r:httpx.Response(200,json={'choices':[{'message':{'content':payload}}]})))
        try:
            if payload=='{"choices":[]}':
                result=await llm.json_completion('system','user')
                assert result=={'choices':[]}
            else:
                with pytest.raises(LLMError): await llm.json_completion('system','user')
        finally: await llm.close()
    asyncio.run(run())

@pytest.mark.parametrize('failure', ['timeout',401,429,500])
def test_nvidia_errors(cfg,failure):
    async def run():
        cfg.nvidia_api_key='test-key'; cfg.nvidia_model='test-model'
        def handle(request):
            if failure=='timeout': raise httpx.ReadTimeout('simulated',request=request)
            return httpx.Response(failure)
        llm=NvidiaClient(cfg); await llm.client.aclose()
        llm.client=httpx.AsyncClient(base_url='https://nvidia.test/v1',transport=httpx.MockTransport(handle))
        try:
            with pytest.raises(LLMError): await llm.json_completion('system','user')
        finally: await llm.close()
    asyncio.run(run())

def test_nvidia_missing_key(cfg):
    async def run():
        llm=NvidiaClient(cfg)
        try:
            with pytest.raises(LLMError): await llm.json_completion('system','user')
        finally: await llm.close()
    asyncio.run(run())

def test_real_planner_rejects_command_output(cfg):
    class LLM:
        async def json_completion(self,*args):
            return {'target_service':'openmeter','failure_type':'application_crash','parameters':{'duration_seconds':1,'observation_seconds':1},'reason':'Test API outage','expected_impact':'Some failures','required_observations':['health'],'command':'docker kill anything'}
    async def run():
        cfg.mode='real'
        with pytest.raises(LLMError):
            await Planner(cfg,LLM()).plan(PlanRequest(goal='Test application crash'),await MockController(cfg).capabilities())
    asyncio.run(run())
