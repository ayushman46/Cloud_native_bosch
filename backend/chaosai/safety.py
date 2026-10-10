from .schemas import Plan, FaultType
from .config import Settings


class SafetyError(ValueError):
    pass


def validate_plan(plan: Plan, settings: Settings, capabilities: dict) -> None:
    if plan.target_service not in settings.targets:
        raise SafetyError(f'Target {plan.target_service!r} is not locally allowlisted')
    remote_targets = capabilities.get('targets', [])
    if plan.target_service not in remote_targets:
        raise SafetyError(f'Target {plan.target_service!r} is not allowed by controller')
    faults = capabilities.get('fault_types', [])
    if plan.failure_type.value not in faults:
        raise SafetyError(f'Fault {plan.failure_type.value} is not supported by controller')
    p = plan.parameters
    if p.duration_seconds > settings.max_duration_seconds or p.observation_seconds > settings.max_duration_seconds:
        raise SafetyError('Duration exceeds local safety limit')
    if p.latency_ms and p.latency_ms > settings.max_latency_ms:
        raise SafetyError('Latency exceeds local safety limit')
    if p.requests_per_second and p.requests_per_second > settings.max_rps:
        raise SafetyError('Request rate exceeds local safety limit')
    if plan.failure_type == FaultType.application_crash and plan.target_service != 'openmeter':
        raise SafetyError('Application crash is restricted to the OpenMeter API')
    if plan.failure_type == FaultType.traffic_spike and plan.target_service != 'openmeter':
        raise SafetyError('Traffic spike is restricted to the OpenMeter API')
    if plan.failure_type == FaultType.network_latency:
        if not capabilities.get('network_latency_ready', False) or plan.target_service not in capabilities.get('proxy_targets', []):
            raise SafetyError('Controller has not confirmed Toxiproxy wiring for this target')
    if plan.failure_type == FaultType.traffic_spike and not capabilities.get('traffic_spike_ready', False):
        raise SafetyError('Controller has not confirmed safe load generation')
    if capabilities.get('watchdog_supported') is not True:
        raise SafetyError('Controller does not confirm an independent rollback watchdog')
