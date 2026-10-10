import asyncio
from typing import TypedDict
from langgraph.graph import StateGraph, START, END
from .schemas import Experiment, Status, TERMINAL, PlanRequest, utcnow
from .safety import validate_plan, SafetyError
from .controller import ControllerError


class FlowState(TypedDict):
    experiment_id: str
    request: PlanRequest


class Runner:
    def __init__(self, settings, store, controller, observer, planner, analyzer):
        self.settings, self.store, self.controller = settings, store, controller
        self.observer, self.planner, self.analyzer = observer, planner, analyzer
        self.tasks: dict[str, asyncio.Task] = {}
        self.cancelled: set[str] = set()
        self._start_lock = asyncio.Lock()
        if settings.mode == 'mock':
            observer.mock_controller = controller
        graph = StateGraph(FlowState)
        for name, method in [('plan', self._plan), ('baseline', self._baseline), ('inject', self._inject), ('observe', self._observe), ('recover', self._recover), ('analyze', self._analyze)]:
            graph.add_node(name, method)
        graph.add_edge(START, 'plan')
        graph.add_edge('plan', 'baseline')
        graph.add_edge('baseline', 'inject')
        graph.add_edge('inject', 'observe')
        graph.add_edge('observe', 'recover')
        graph.add_edge('recover', 'analyze')
        graph.add_edge('analyze', END)
        self.graph = graph.compile()

    def _update(self, exp: Experiment, status: Status, **fields):
        next_states = {
            Status.created: Status.planning, Status.planning: Status.planned,
            Status.planned: Status.validating, Status.validating: Status.approved,
            Status.approved: Status.baselining, Status.baselining: Status.injecting,
            Status.injecting: Status.armed, Status.armed: Status.active,
            Status.active: Status.rolling_back, Status.rolling_back: Status.verifying_recovery,
            Status.verifying_recovery: Status.completed,
        }
        if status not in {Status.failed, Status.rejected, Status.aborted, Status.recovery_failed, Status.rolling_back} and next_states.get(exp.status) != status:
            raise RuntimeError(f'Invalid transition: {exp.status} -> {status}')
        exp.status = status
        exp.timestamps[status.value.lower() + '_at'] = utcnow()
        exp.history.append({'status': status.value, 'at': utcnow().isoformat()})
        for k, v in fields.items():
            setattr(exp, k, v)
        self.store.save(exp)

    async def start(self, request: PlanRequest) -> Experiment:
        async with self._start_lock:
            if any(not t.done() for t in self.tasks.values()) or any(e.status not in TERMINAL for e in self.store.list()):
                raise RuntimeError('Another experiment is active')
            from uuid import uuid4
            exp = Experiment(experiment_id=f'exp_{uuid4().hex}', goal=request.goal, simulated=self.settings.mode == 'mock')
            self.store.save(exp)
            task = asyncio.create_task(self._run(exp.experiment_id, request))
            self.tasks[exp.experiment_id] = task
            return exp

    async def close(self):
        for eid, task in self.tasks.items():
            if not task.done():
                self.cancelled.add(eid)
                task.cancel()
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)

    def _rollback_ok(self, exp, status):
        evidence = status.rollback_evidence
        return bool(status.rollback_verified and evidence and exp.plan and evidence.target_service == exp.plan.target_service and (self.settings.mode == 'mock' or not evidence.simulated))

    async def _stop(self, exp):
        status = await self.controller.abort(exp.experiment_id)
        deadline = asyncio.get_running_loop().time() + self.settings.rollback_timeout_seconds
        while not self._rollback_ok(exp, status) and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(self.settings.poll_seconds)
            status = await self.controller.status(exp.experiment_id)
        exp.controller = status
        self.store.save(exp)
        if not self._rollback_ok(exp, status):
            raise RuntimeError('Rollback evidence was not verified before timeout')
        return status

    async def reconcile_interrupted(self):
        for exp in self.store.list():
            if exp.status in TERMINAL:
                continue
            if self.settings.mode == 'mock':
                self._update(exp, Status.failed, error='Mock experiment interrupted by ChaosAI restart')
                continue
            if exp.status in {Status.created, Status.planning, Status.planned, Status.validating, Status.approved, Status.baselining}:
                self._update(exp, Status.failed, error='Experiment interrupted before injection')
                continue
            try:
                status = await self.controller.status(exp.experiment_id)
                if not status.rollback_verified:
                    status = await self._stop(exp)
                exp.controller = status
                if self._rollback_ok(exp, status):
                    self._update(exp, Status.failed, error='ChaosAI restarted; controller rollback verified, observations incomplete')
                else:
                    self._update(exp, Status.recovery_failed, error='ChaosAI restarted; controller rollback not verified')
            except Exception as exc:
                self._update(exp, Status.recovery_failed, error=f'ChaosAI restarted; controller state unknown: {exc}')

    async def _run(self, eid: str, request: PlanRequest):
        try:
            await self.graph.ainvoke({'experiment_id': eid, 'request': request})
        except (Exception, asyncio.CancelledError) as exc:
            exp = self.store.get(eid)
            if not exp or exp.status in TERMINAL:
                return
            exp.error = str(exc) or 'Experiment interrupted'
            if exp.status in {Status.injecting, Status.armed, Status.active, Status.rolling_back} and exp.controller is None:
                try:
                    exp.controller = await self.controller.status(eid)
                    self.store.save(exp)
                except Exception as lookup_exc:
                    try:
                        exp.controller = await self._stop(exp)
                    except Exception as stop_exc:
                        exp.error += f'; remote injection outcome unknown: {lookup_exc}; abort: {stop_exc}'
                        self._update(exp, Status.recovery_failed)
                        return
            if exp.controller and not self._rollback_ok(exp, exp.controller):
                try:
                    self._update(exp, Status.rolling_back)
                    exp.controller = await self._stop(exp)
                    self.store.save(exp)
                except Exception as rollback_exc:
                    exp.error += f'; abort failed: {rollback_exc}'
                    self._update(exp, Status.recovery_failed)
                    return
                if not self._rollback_ok(exp, exp.controller):
                    exp.error += '; rollback unverified'
                    self._update(exp, Status.recovery_failed)
                    return
            self._update(exp, Status.aborted if eid in self.cancelled else Status.rejected if isinstance(exc, SafetyError) else Status.failed)

    async def abort(self, eid: str) -> Experiment:
        exp = self.store.get(eid)
        if not exp:
            raise KeyError(eid)
        if exp.status in TERMINAL and exp.status != Status.recovery_failed:
            return exp
        self.cancelled.add(eid)
        if exp.status == Status.recovery_failed and exp.plan:
            try:
                exp.controller = await self._stop(exp)
                self._update(exp, Status.aborted)
            except Exception as exc:
                exp.error = f'Abort request failed: {exc}'
                self._update(exp, Status.recovery_failed)
                return exp
        return exp

    def _check_abort(self, eid):
        if eid in self.cancelled:
            raise RuntimeError('Experiment aborted by user')

    async def _plan(self, state: FlowState):
        eid = state['experiment_id']
        exp = self.store.get(eid)
        self._update(exp, Status.planning)
        self._check_abort(eid)
        await self.controller.health()
        capabilities = await self.controller.capabilities()
        plan = getattr(state['request'], 'approved_plan', None)
        if plan is None:
            plan = await self.planner.plan(state['request'], capabilities)
        else:
            plan = plan.model_copy(deep=True)
            requested_fault = state['request'].preferred_fault
            requested_target = state['request'].preferred_target
            if (requested_fault and plan.failure_type != requested_fault) or (requested_target and plan.target_service != requested_target):
                raise SafetyError('Approved plan differs from selected fault or target')
        plan.experiment_id = eid
        exp.plan = plan
        self._update(exp, Status.planned)
        self._update(exp, Status.validating)
        validate_plan(plan, self.settings, capabilities)
        if self.settings.mode == 'real':
            probes = await self.observer.probe('preflight')
            exp.probes.extend(probes)
            self.store.save(exp)
            if not probes or not all(p.reachable for p in probes):
                raise RuntimeError('OpenMeter monitoring endpoints are unreachable')
        self._update(exp, Status.approved)
        return {}

    async def _baseline(self, state: FlowState):
        eid = state['experiment_id']
        exp = self.store.get(eid)
        self._update(exp, Status.baselining)
        if self.settings.enable_synthetic_events:
            event = await self.observer.send_event(eid, 'baseline')
            exp.events.append(event)
            self.store.save(exp)
            if not event.accepted:
                raise RuntimeError('Baseline synthetic event was not accepted')
        deadline = asyncio.get_running_loop().time() + self.settings.baseline_seconds
        while asyncio.get_running_loop().time() < deadline:
            self._check_abort(eid)
            probes = await self.observer.probe('baseline')
            exp.probes.extend(probes)
            self.store.save(exp)
            if not probes or not all(p.reachable for p in probes):
                raise RuntimeError('Baseline is unhealthy; injection cancelled')
            await asyncio.sleep(self.settings.poll_seconds)
        return {}

    async def _inject(self, state: FlowState):
        eid = state['experiment_id']
        self._check_abort(eid)
        exp = self.store.get(eid)
        self._update(exp, Status.injecting)
        exp.timestamps['dispatched_at'] = utcnow()
        self.store.save(exp)
        status = await self.controller.submit(exp.plan)
        exp.controller = status
        self.store.save(exp)
        if not status.watchdog_armed:
            try:
                exp.controller = await self.controller.abort(eid)
                self.store.save(exp)
            finally:
                raise RuntimeError('Controller did not confirm independent rollback watchdog')
        self._update(exp, Status.armed)
        deadline = asyncio.get_running_loop().time() + self.settings.activation_timeout_seconds
        while asyncio.get_running_loop().time() < deadline:
            self._check_abort(eid)
            status = await self.controller.status(eid)
            exp.controller = status
            exp.probes.extend(await self.observer.probe('fault' if status.injection_verified else 'activation'))
            self.store.save(exp)
            if status.injection_verified and status.fault_activated_at:
                expected = {'application_crash': 'container_exited', 'network_latency': 'toxic_active', 'service_restart': 'container_restarted', 'traffic_spike': 'load_generator_running'}[exp.plan.failure_type.value]
                evidence = status.activation_evidence
                if not evidence or evidence.evidence_type != expected or evidence.target_service != exp.plan.target_service or (self.settings.mode == 'real' and evidence.simulated):
                    raise RuntimeError('Controller activation evidence does not match the planned fault and target')
                exp.timestamps['fault_activated_at'] = status.fault_activated_at
                self._update(exp, Status.active)
                return {}
            if status.status in ('failed', 'rejected', 'aborted'):
                raise RuntimeError(f'Controller failed to activate fault: {status.message}')
            await asyncio.sleep(self.settings.poll_seconds)
        raise RuntimeError('Fault activation was not verified before timeout')

    async def _observe(self, state: FlowState):
        eid = state['experiment_id']
        exp = self.store.get(eid)
        deadline = asyncio.get_running_loop().time() + exp.plan.parameters.duration_seconds + self.settings.rollback_timeout_seconds
        fault_event_sent = False
        while asyncio.get_running_loop().time() < deadline:
            self._check_abort(eid)
            status = await self.controller.status(eid)
            exp.controller = status
            phase = 'fault' if status.status == 'active' else 'recovery'
            if self.settings.enable_synthetic_events and phase == 'fault' and not fault_event_sent:
                exp.events.append(await self.observer.send_event(eid, 'fault'))
                fault_event_sent = True
            exp.probes.extend(await self.observer.probe(phase))
            if self.settings.enable_synthetic_events:
                count = await self.observer.query_count(eid)
                exp.meter_samples.append({'at': utcnow().isoformat(), 'phase': phase, 'accepted_events': sum(e.accepted for e in exp.events), 'metered_count': count, 'error': self.observer.last_query_error, 'simulated': self.settings.mode == 'mock'})
            if status.rollback_started_at:
                exp.timestamps['rollback_started_at'] = status.rollback_started_at
            self.store.save(exp)
            if status.rollback_verified:
                return {}
            await asyncio.sleep(self.settings.poll_seconds)
        raise RuntimeError('Controller did not verify rollback before deadline')

    async def _recover(self, state: FlowState):
        eid = state['experiment_id']
        exp = self.store.get(eid)
        self._update(exp, Status.rolling_back)
        status = await self.controller.status(eid)
        exp.controller = status
        self.store.save(exp)
        if not status.rollback_verified:
            raise RuntimeError('Infrastructure rollback was not verified')
        if not status.rollback_evidence or status.rollback_evidence.target_service != exp.plan.target_service or (self.settings.mode == 'real' and status.rollback_evidence.simulated):
            raise RuntimeError('Controller rollback evidence does not match the planned target')
        exp.timestamps['rollback_completed_at'] = status.rollback_completed_at or utcnow()
        self._update(exp, Status.verifying_recovery)
        if self.settings.enable_synthetic_events:
            recovery_event = await self.observer.send_event(eid, 'recovery')
            exp.events.append(recovery_event)
            self.store.save(exp)
        recovered = False
        deadline = asyncio.get_running_loop().time() + self.settings.recovery_seconds
        while asyncio.get_running_loop().time() < deadline:
            self._check_abort(eid)
            probes = await self.observer.probe('recovery')
            exp.probes.extend(probes)
            self.store.save(exp)
            http_ok = bool(probes) and all(p.reachable for p in probes)
            if self.settings.enable_synthetic_events:
                exp.metered_count = await self.observer.query_count(eid)
                self.store.save(exp)
                accepted = sum(event.accepted for event in exp.events)
                metering_ok = recovery_event.accepted and exp.metered_count is not None and exp.metered_count == accepted
            else:
                metering_ok = True
            if http_ok and metering_ok:
                recovered = True
                exp.timestamps['functional_recovery_at'] = utcnow()
                break
            await asyncio.sleep(self.settings.poll_seconds)
        if not recovered:
            exp.report = await self.analyzer.analyze(exp.probes, exp.controller.rollback_verified, exp.events, exp.metered_count, exp.timestamps)
            self._update(exp, Status.recovery_failed, error='OpenMeter functional recovery was not verified')
            raise RuntimeError('OpenMeter functional recovery was not verified')
        return {}

    async def _analyze(self, state: FlowState):
        eid = state['experiment_id']
        exp = self.store.get(eid)
        self._check_abort(eid)
        exp.report = await self.analyzer.analyze(exp.probes, exp.controller.rollback_verified, exp.events, exp.metered_count, exp.timestamps)
        self._check_abort(eid)
        self._update(exp, Status.completed)
        return {}
