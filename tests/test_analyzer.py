import asyncio
from datetime import timedelta
import pytest
from chaosai.metrics import phase_metrics, summarize
from chaosai.schemas import Probe, SyntheticEvent, utcnow
from chaosai.agents import Analyzer
from chaosai.llm import LLMError


def test_known_rates_and_p95():
    p=[Probe(phase='fault',endpoint='test',reachable=i<19,latency_ms=i+1) for i in range(20)]
    m=phase_metrics(p)
    assert m['availability']==.95
    assert m['error_rate']==pytest.approx(.05)
    assert m['p95_latency_ms']==19
    assert phase_metrics(p[:2])['p95_latency_ms']==2
    assert m['throughput_rps'] is None

def test_empty_and_simulated_metrics_unavailable():
    m=summarize([Probe(phase='fault',endpoint='mock',reachable=False,simulated=True)],[],None)
    assert m['fault']['samples']==0 and m['fault']['availability'] is None
    assert m['metering']['missing_events'] is None
    assert m['agent_performance']['root_cause_accuracy'] is None

@pytest.mark.parametrize('count,missing,duplicates',[(1,1,0),(2,0,0),(3,0,1)])
def test_metering_math(count,missing,duplicates):
    events=[SyntheticEvent(event_id=str(i),phase='fault',accepted=True) for i in range(2)]
    m=summarize([],events,count)['metering']
    assert m['missing_events']==missing and m['duplicate_events']==duplicates
    assert 0<=m['metering_accuracy']<=1

def test_detection_and_recovery_units():
    t=utcnow()
    p=[Probe(at=t-timedelta(seconds=1),phase='baseline',endpoint='test',reachable=True,latency_ms=10),Probe(at=t+timedelta(seconds=.5),phase='fault',endpoint='test',reachable=False)]
    m=summarize(p,[],None,{'fault_activated_at':t,'rollback_completed_at':t+timedelta(seconds=2),'functional_recovery_at':t+timedelta(seconds=5)})
    assert m['recovery']['detection_latency_ms']==500
    assert m['recovery']['functional_recovery_seconds']==3

@pytest.mark.parametrize('output',[{'diagnosis':'Some diagnosis','recommendations':'invalid string'},LLMError('timeout'),{'diagnosis':3,'recommendations':[]}])
def test_analyzer_invalid_or_unavailable_fallback(cfg,output):
    class LLM:
        async def json_completion(self,system,user):
            assert 'failure_type' not in user
            if isinstance(output,Exception): raise output
            return output
    async def run():
        cfg.mode='real'
        report=await Analyzer(cfg,LLM()).analyze([],False,[],None)
        assert report.analysis_source=='deterministic'
        assert any('NVIDIA analysis unavailable' in x for x in report.limitations)
    asyncio.run(run())

def test_analyzer_validated_inference(cfg):
    class LLM:
        async def json_completion(self,system,user):
            assert 'failure_type' not in user
            return {'diagnosis':'Availability decreased during the observed fault window.','recommendations':['Inspect API logs for failed requests.']}
    async def run():
        cfg.mode='real'
        r=await Analyzer(cfg,LLM()).analyze([Probe(phase='fault',endpoint='test',reachable=False)],True,[],None)
        assert r.analysis_source=='nvidia' and r.metrics['fault']['availability']==0
    asyncio.run(run())
