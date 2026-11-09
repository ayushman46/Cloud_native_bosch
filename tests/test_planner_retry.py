import asyncio
import json
import pytest
from chaosai.agents import Planner
from chaosai.safety import SafetyError
from chaosai.schemas import PlanRequest

CAPS = {'targets': ['openmeter', 'kafka'], 'fault_types': ['application_crash', 'network_latency'], 'watchdog_supported': True,
        'network_latency_ready': True, 'proxy_targets': ['openmeter'], 'traffic_spike_ready': True}


def plan_json(fault, target):
    params = {'duration_seconds': 1, 'observation_seconds': 1}
    if fault == 'network_latency':
        params['latency_ms'] = 200
    return {'target_service': target, 'failure_type': fault, 'parameters': params, 'reason': f'{fault} on {target}', 'expected_impact': 'Some impact expected', 'required_observations': ['health']}


class Scripted:
    """An LLM stand-in that returns one scripted plan per call and records what it was sent."""
    def __init__(self, *plans):
        self.plans, self.sent = list(plans), []

    async def json_completion(self, system, user):
        self.sent.append(json.loads(user))
        return dict(self.plans[min(len(self.sent), len(self.plans)) - 1])


def run(cfg, llm):
    cfg.mode = 'real'
    return asyncio.run(Planner(cfg, llm).plan(PlanRequest(goal='Find the weakest dependency of OpenMeter'), CAPS, {}))


def test_refused_plan_is_fed_back_and_the_model_corrects_itself(cfg):
    llm = Scripted(plan_json('network_latency', 'kafka'), plan_json('network_latency', 'openmeter'))
    plan = run(cfg, llm)
    assert (plan.failure_type.value, plan.target_service) == ('network_latency', 'openmeter')
    assert len(llm.sent) == 2
    assert 'previous_plan_rejected' not in llm.sent[0]
    assert 'network_latency on kafka' in llm.sent[1]['previous_plan_rejected']


def test_model_that_never_corrects_is_still_refused_by_the_safety_layer(cfg):
    llm = Scripted(plan_json('network_latency', 'kafka'))
    with pytest.raises(SafetyError):
        run(cfg, llm)
    assert len(llm.sent) == Planner.plan_attempts


def test_valid_first_plan_makes_exactly_one_call(cfg):
    llm = Scripted(plan_json('application_crash', 'openmeter'))
    plan = run(cfg, llm)
    assert plan.target_service == 'openmeter' and len(llm.sent) == 1
