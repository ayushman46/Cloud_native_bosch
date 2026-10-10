import asyncio
import httpx
from pydantic import ValidationError
from .schemas import Plan, ControllerStatus, FaultEvidence, utcnow
from .config import Settings


class ControllerError(RuntimeError):
    pass


class RemoteController:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = httpx.AsyncClient(base_url=settings.chaos_control_url.rstrip('/'), timeout=5, headers={'Authorization': f'Bearer {settings.chaos_control_api_key}'})

    async def close(self):
        await self.client.aclose()

    async def _request(self, method: str, path: str, **kwargs) -> dict:
        try:
            response = await self.client.request(method, path, **kwargs)
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise ControllerError(f'Controller {method} {path} failed: {exc}') from exc

    async def health(self):
        return await self._request('GET', '/api/health')

    async def capabilities(self):
        return await self._request('GET', '/api/capabilities')

    async def submit(self, plan: Plan) -> ControllerStatus:
        body = plan.model_dump(mode='json', include={'experiment_id','application','target_service','failure_type','parameters'})
        try:
            raw = await self._request('POST', '/api/experiments', json=body, headers={'Idempotency-Key': plan.experiment_id})
        except ControllerError as exc:
            # A timeout or disconnect may occur after execution starts. Never POST twice.
            try:
                return await self.status(plan.experiment_id)
            except ControllerError as status_exc:
                raise ControllerError(f'Submission outcome unknown; status lookup failed: {status_exc}') from exc
        try:
            result = ControllerStatus.model_validate(raw)
        except ValidationError as exc:
            raise ControllerError(f'Invalid controller response: {exc}') from exc
        if result.experiment_id != plan.experiment_id:
            raise ControllerError('Controller returned a different experiment ID')
        return result

    async def status(self, experiment_id: str) -> ControllerStatus:
        try:
            result = ControllerStatus.model_validate(await self._request('GET', f'/api/experiments/{experiment_id}'))
        except ValidationError as exc:
            raise ControllerError(f'Invalid controller status: {exc}') from exc
        if result.experiment_id != experiment_id:
            raise ControllerError('Controller status returned a different experiment ID')
        return result

    async def abort(self, experiment_id: str) -> ControllerStatus:
        try:
            result = ControllerStatus.model_validate(await self._request('POST', f'/api/experiments/{experiment_id}/abort'))
        except ValidationError as exc:
            raise ControllerError(f'Invalid controller abort response: {exc}') from exc
        if result.experiment_id != experiment_id:
            raise ControllerError('Controller abort returned a different experiment ID')
        return result


class MockController:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.states: dict[str, ControllerStatus] = {}
        self.plans: dict[str, Plan] = {}
        self.starts: dict[str, float] = {}
        self.aborted: set[str] = set()
        self.activation_delay_seconds = 0.2
        self.rollback_delay_seconds = 1.0
        self._watchdogs: dict[str, asyncio.Task] = {}

    async def close(self):
        for eid, task in self._watchdogs.items():
            if not task.done():
                await self.abort(eid)
        await asyncio.gather(*self._watchdogs.values(), return_exceptions=True)

    async def health(self):
        return {'status': 'ok', 'simulated': True}

    async def capabilities(self):
        return {'targets': self.settings.targets, 'fault_types': ['application_crash','network_latency','service_restart','traffic_spike'], 'watchdog_supported': True, 'network_latency_ready': True, 'proxy_targets': self.settings.targets, 'traffic_spike_ready': True, 'simulated': True}

    async def submit(self, plan: Plan) -> ControllerStatus:
        if plan.experiment_id in self.states:
            if self.plans[plan.experiment_id] != plan:
                raise ControllerError('Idempotency key already belongs to a different plan')
            return await self.status(plan.experiment_id)
        self.plans[plan.experiment_id] = plan
        self.starts[plan.experiment_id] = asyncio.get_running_loop().time()
        state = ControllerStatus(experiment_id=plan.experiment_id, status='armed', watchdog_armed=True, message='Simulated controller; no infrastructure changed')
        self.states[plan.experiment_id] = state
        self._watchdogs[plan.experiment_id] = asyncio.create_task(self._lifecycle(plan.experiment_id))
        return state

    async def status(self, experiment_id: str) -> ControllerStatus:
        if experiment_id not in self.states:
            raise ControllerError('Experiment not found by mock controller')
        return self.states[experiment_id].model_copy(deep=True)

    async def _lifecycle(self, experiment_id: str):
        state = self.states[experiment_id]
        duration = self.plans[experiment_id].parameters.duration_seconds
        fault = self.plans[experiment_id].failure_type.value
        target = self.plans[experiment_id].target_service
        evidence_types = {'application_crash': 'container_exited', 'network_latency': 'toxic_active', 'service_restart': 'container_restarted', 'traffic_spike': 'load_generator_running'}
        await asyncio.sleep(self.activation_delay_seconds)
        state.status = 'active'
        state.injection_verified = True
        state.fault_activated_at = utcnow()
        state.activation_evidence = FaultEvidence(evidence_type=evidence_types[fault], target_service=target, observed_at=state.fault_activated_at, details={'fault_type': fault}, simulated=True)
        await asyncio.sleep(duration)
        state.status = 'rolling_back'
        state.rollback_started_at = utcnow()
        await asyncio.sleep(self.rollback_delay_seconds)
        state.status = 'rolled_back'
        state.rollback_verified = True
        state.rollback_completed_at = utcnow()
        state.rollback_evidence = FaultEvidence(evidence_type='fault_removed', target_service=target, observed_at=state.rollback_completed_at, details={'fault_type': fault}, simulated=True)

    async def abort(self, experiment_id: str) -> ControllerStatus:
        if experiment_id not in self.states:
            raise ControllerError('Experiment not found by mock controller')
        state = self.states[experiment_id]
        self.aborted.add(experiment_id)
        task = self._watchdogs.get(experiment_id)
        if task and not task.done():
            task.cancel()
        state.status = 'aborted'
        state.rollback_verified = True
        state.rollback_started_at = state.rollback_started_at or utcnow()
        state.rollback_completed_at = utcnow()
        target = self.plans[experiment_id].target_service
        state.rollback_evidence = FaultEvidence(evidence_type='fault_removed', target_service=target, observed_at=state.rollback_completed_at, simulated=True)
        return state
