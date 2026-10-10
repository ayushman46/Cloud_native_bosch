#!/usr/bin/env python3
"""Exercise every ChaosAI HTTP route in mock mode. Never starts a real fault."""
import asyncio
import json
import sys
from pathlib import Path
import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else 'http://127.0.0.1:8000'
OUT = Path('verification/api-smoke.json')
FAULTS = [
    ('application_crash','openmeter','Test API availability during an application crash'),
    ('network_latency','openmeter','Test slow Kafka communication'),
    ('service_restart','sink-worker','Test accepted events through a worker restart'),
    ('traffic_spike','openmeter','Test ingestion during elevated request traffic'),
]

async def main():
    async with httpx.AsyncClient(base_url=BASE, timeout=10) as client:
        config=(await client.get('/api/config')).json()
        if config.get('mode') != 'mock':
            raise SystemExit('Refusing smoke test: target API is not in mock mode')
        assert (await client.get('/healthz')).status_code==200
        assert (await client.get('/api/health')).status_code==200
        assert (await client.get('/api/capabilities')).status_code==200
        assert (await client.post('/api/experiments',json={})).status_code==422
        assert (await client.get('/api/experiments/not-found')).status_code==404
        assert (await client.get('/api/experiments/not-found/metrics')).status_code==404
        assert (await client.get('/api/experiments/not-found/report')).status_code==404
        assert (await client.get('/api/experiments/not-found/observer-report')).status_code==404
        evidence=[]
        for fault,target,goal in FAULTS:
            request={'goal':goal,'preferred_fault':fault,'preferred_target':target}
            preview=await client.post('/api/experiments/plan',json=request)
            preview.raise_for_status()
            plan=preview.json()
            assert plan['failure_type']==fault and plan['target_service']==target
            created=await client.post('/api/experiments',json={**request,'approved_plan':plan})
            created.raise_for_status()
            eid=created.json()['experiment_id']
            assert (await client.get(f'/api/experiments/{eid}/report')).status_code==409
            final=None
            for _ in range(100):
                response=await client.get(f'/api/experiments/{eid}')
                response.raise_for_status()
                final=response.json()
                if final['status'] in ('COMPLETED','FAILED','ABORTED','RECOVERY_FAILED','REJECTED'):
                    break
                await asyncio.sleep(.2)
            assert final and final['status']=='COMPLETED',final
            assert final['controller']['activation_evidence']['simulated']
            assert final['controller']['rollback_evidence']['simulated']
            assert final['controller']['rollback_verified']
            assert (await client.get(f'/api/experiments/{eid}/metrics')).status_code==200
            assert (await client.get(f'/api/experiments/{eid}/report')).status_code==200
            shared=await client.get(f'/api/experiments/{eid}/observer-report')
            shared.raise_for_status()
            assert shared.json()['experiment_id']==eid
            assert shared.json()['recovery_verified'] is True
            assert shared.json()['baseline']['success_rate_pct'] is None
            evidence.append({'fault':fault,'experiment_id':eid,'status':final['status'],'activation_evidence':final['controller']['activation_evidence']['evidence_type'],'fault_probes':sum(p['phase']=='fault' for p in final['probes']),'rollback_verified':final['controller']['rollback_verified'],'functional_recovery_at':final['timestamps'].get('functional_recovery_at'),'simulated':final['simulated']})
            print(f'{fault}: {final["status"]} ({eid})',flush=True)
        listing=await client.get('/api/experiments')
        assert listing.status_code==200 and len(listing.json())>=4
        OUT.parent.mkdir(exist_ok=True)
        OUT.write_text(json.dumps(evidence,indent=2)+'\n')
        print(f'PASS: {len(evidence)} mock HTTP workflows; evidence: {OUT}',flush=True)

if __name__=='__main__':
    asyncio.run(main())
