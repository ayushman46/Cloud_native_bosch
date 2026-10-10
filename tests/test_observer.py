import asyncio
import httpx
import pytest
from chaosai.observer import Observer

@pytest.mark.parametrize('code,healthy',[(200,True),(204,True),(401,False),(404,False),(500,False)])
def test_http_health_classification(cfg,code,healthy):
    async def run():
        cfg.mode='real'; cfg.openmeter_base_url='http://openmeter.test'
        o=Observer(cfg); await o.client.aclose()
        o.client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(code)))
        p=(await o.probe('baseline'))[0]
        assert p.reachable is healthy and p.status_code==code and p.latency_ms is not None
        await o.close()
    asyncio.run(run())

@pytest.mark.parametrize('error',[httpx.ConnectError,httpx.ReadTimeout])
def test_probe_network_errors(cfg,error):
    async def run():
        cfg.mode='real'; cfg.openmeter_base_url='http://openmeter.test'
        def handle(r): raise error('simulated',request=r)
        o=Observer(cfg); await o.client.aclose()
        o.client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
        p=(await o.probe('fault'))[0]
        assert not p.reachable and p.error and p.status_code is None
        await o.close()
    asyncio.run(run())

@pytest.mark.parametrize('body,expected',[({'data':[]},0),({'data':[{'value':1},{'value':2}]},3),({'data':[{}]},None)])
def test_meter_results(cfg,body,expected):
    async def run():
        cfg.mode='real'; cfg.openmeter_base_url='http://openmeter.test'
        o=Observer(cfg); await o.client.aclose()
        o.client=httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,json=body)))
        assert await o.query_count('exp_test')==expected
        if expected is None: assert o.last_query_error
        await o.close()
    asyncio.run(run())
