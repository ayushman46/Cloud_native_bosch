import asyncio
from chaosai.agents import Planner, Analyzer
from chaosai.controller import MockController
from chaosai.llm import NvidiaClient
from chaosai.observer import Observer
from chaosai.orchestration import Runner
from chaosai.schemas import PlanRequest, Experiment
from chaosai.storage import Store
from chaosai.observer_adapter import to_shared_report
from hacker_agent_observer_contract.observer_contract import ObserverReport


def test_complete_shared_handoff(cfg):
    async def run():
        c=MockController(cfg);c.activation_delay_seconds=.02;c.rollback_delay_seconds=.02
        o=Observer(cfg);llm=NvidiaClient(cfg)
        r=Runner(cfg,Store(cfg.database_path),c,o,Planner(cfg,llm),Analyzer(cfg,llm))
        try:
            e=await r.start(PlanRequest(goal='Test sink worker restart',preferred_fault='service_restart',preferred_target='sink-worker'))
            await r.tasks[e.experiment_id]
            exp=Experiment.model_validate_json(r.store.get(e.experiment_id).model_dump_json())
            shared=to_shared_report(exp)
            parsed=ObserverReport.model_validate_json(shared.model_dump_json())
            assert parsed.experiment_id==e.experiment_id
            assert parsed.fault_type=='container_restart'
            assert parsed.baseline.success_rate_pct is None # simulated evidence is excluded
            assert parsed.recovery_verified
        finally:
            await r.close();await c.close();await o.close();await llm.close()
    asyncio.run(run())
