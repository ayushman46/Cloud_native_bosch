import json
from uuid import uuid4
from pydantic import ValidationError
from .schemas import Plan, Parameters, PlanRequest, FaultType, Probe, Report, SyntheticEvent
from .safety import validate_plan
from .llm import NvidiaClient, LLMError
from .config import Settings
from .metrics import summarize
from pydantic import BaseModel, ConfigDict, Field


class AnalysisOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    diagnosis: str = Field(min_length=5)
    recommendations: list[str] = Field(min_length=1)


class Planner:
    def __init__(self, settings: Settings, llm: NvidiaClient):
        self.settings, self.llm = settings, llm

    async def plan(self, request: PlanRequest, capabilities: dict) -> Plan:
        if self.settings.mode == 'mock':
            goal = request.goal.lower()
            fault = request.preferred_fault or (FaultType.service_restart if 'restart' in goal or 'worker' in goal else FaultType.network_latency if 'latency' in goal or 'slow' in goal else FaultType.traffic_spike if 'load' in goal or 'traffic' in goal else FaultType.application_crash)
            target = request.preferred_target or ('sink-worker' if fault == FaultType.service_restart and 'sink-worker' in self.settings.targets else 'openmeter')
            params = Parameters(duration_seconds=min(5, self.settings.max_duration_seconds), observation_seconds=min(5, self.settings.max_duration_seconds), latency_ms=min(250, self.settings.max_latency_ms) if fault == FaultType.network_latency else None, requests_per_second=min(5, self.settings.max_rps) if fault == FaultType.traffic_spike else None)
            plan = Plan(target_service=target, failure_type=fault, parameters=params, reason=f'Mock planner selected {fault.value} to examine: {request.goal}', expected_impact='Availability or processing latency may change during the bounded fault.', required_observations=['health', 'latency', 'recovery'])
        else:
            system = ('You plan controlled chaos experiments for OpenMeter. Return ONLY a JSON object with keys target_service, failure_type, parameters, reason, expected_impact, required_observations, safety_requirements. Select only supplied targets and faults. Parameters: duration_seconds, observation_seconds and latency_ms only for network_latency or requests_per_second only for traffic_spike. No shell commands or arbitrary URLs.')
            user = json.dumps({'goal': request.goal, 'preferred_fault': request.preferred_fault, 'preferred_target': request.preferred_target, 'targets': self.settings.targets, 'capabilities': capabilities, 'limits': {'max_duration_seconds': self.settings.max_duration_seconds, 'max_latency_ms': self.settings.max_latency_ms, 'max_rps': self.settings.max_rps}})
            raw = await self.llm.json_completion(system, user)
            raw['experiment_id'] = f'exp_{uuid4().hex}'
            raw['application'] = 'openmeter'
            try:
                plan = Plan.model_validate(raw)
            except ValidationError as exc:
                raise LLMError(f'Invalid NVIDIA plan: {exc}') from exc
            if request.preferred_fault and plan.failure_type != request.preferred_fault:
                raise LLMError('Planner did not honor selected fault')
            if request.preferred_target and plan.target_service != request.preferred_target:
                raise LLMError('Planner did not honor selected target')
        validate_plan(plan, self.settings, capabilities)
        return plan


class Analyzer:
    def __init__(self, settings: Settings, llm: NvidiaClient):
        self.settings, self.llm = settings, llm

    async def analyze(self, probes: list[Probe], controller_rollback_verified: bool, events: list[SyntheticEvent], metered_count: float | None, timestamps: dict | None = None) -> Report:
        real = [p for p in probes if not p.simulated]
        phases = ['baseline', 'fault', 'recovery']
        metrics = summarize(probes, events, metered_count, timestamps)
        evidence = [f'{phase}: {metrics[phase]["samples"]} real probe(s), availability {metrics[phase]["availability"]}' for phase in phases]
        evidence.append(f'Controller rollback verified: {controller_rollback_verified}')
        limitations = []
        real_events = [e for e in events if not e.simulated]
        if real_events and metered_count is not None:
            accepted = sum(e.accepted for e in real_events)
            evidence.append(f'Isolated subject: {accepted} accepted event(s), metered value {metered_count}')
        else:
            limitations.append('Metering correctness was not measured; enable synthetic events on a compatible OpenMeter quickstart.')
        if not real:
            limitations.append('No real OpenMeter measurements were collected; this is a simulated workflow.')
        base = Report(diagnosis=('Simulated rollback only; no real OpenMeter impact was measured.' if self.settings.mode == 'mock' else 'Rollback confirmed by controller; application behavior is summarized by measured probes.'), evidence=evidence, recommendations=['Inspect OpenMeter logs and event counts before claiming data correctness.'], metrics=metrics, analysis_source='deterministic', limitations=limitations)
        if self.settings.mode == 'mock':
            return base
        try:
            raw = await self.llm.json_completion('Analyze only supplied evidence. Return JSON with diagnosis and recommendations. Do not invent metrics, root causes, or event correctness.', json.dumps({'evidence': evidence, 'metrics': metrics, 'limitations': limitations}))
            output = AnalysisOutput.model_validate(raw)
            base.diagnosis = output.diagnosis
            base.recommendations = output.recommendations
            base.analysis_source = 'nvidia'
        except (LLMError, KeyError, TypeError, ValidationError):
            base.limitations.append('NVIDIA analysis unavailable; deterministic summary shown.')
        return base
