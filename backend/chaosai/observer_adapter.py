"""Translate ChaosAI observations to the shared Observer contract in this repository."""
from datetime import datetime
from hacker_agent_observer_contract.observer_contract import (
    ExperimentPhase, ExperimentStatus, FaultType as SharedFaultType,
    HealthState, MetricSummary, ObserverReport, RecoveryStatus,
)
from .schemas import Experiment, Status, utcnow
from .metrics import phase_metrics

FAULT_MAP = {
    'application_crash': SharedFaultType.CONTAINER_CRASH,
    'network_latency': SharedFaultType.NETWORK_DELAY,
    'service_restart': SharedFaultType.CONTAINER_RESTART,
    'traffic_spike': SharedFaultType.HEAVY_LOAD,
}


def to_shared_report(exp: Experiment) -> ObserverReport:
    if not exp.plan:
        raise ValueError('Experiment plan is required for Observer handoff')
    def summary(name: str, phase: ExperimentPhase) -> MetricSummary:
        samples = [p for p in exp.probes if p.phase == name and not p.simulated]
        metrics = phase_metrics(samples)
        return MetricSummary(
            phase=phase, sample_count=metrics['samples'],
            error_rate_pct=metrics['error_rate'] * 100 if metrics['error_rate'] is not None else None,
            success_rate_pct=metrics['availability'] * 100 if metrics['availability'] is not None else None,
            p95_latency_ms=metrics['p95_latency_ms'],
            target_unhealthy_samples=sum(not p.reachable for p in samples),
        )
    terminal = exp.status in {Status.completed, Status.failed, Status.aborted, Status.recovery_failed, Status.rejected}
    status = ExperimentStatus.COMPLETED if exp.status == Status.completed else ExperimentStatus.ABORTED if exp.status == Status.aborted else ExperimentStatus.FAILED if terminal else ExperimentStatus.RUNNING
    recovered = exp.status == Status.completed and exp.controller is not None and exp.controller.rollback_verified
    return ObserverReport(
        experiment_id=exp.experiment_id, run_id=exp.experiment_id,
        target_app=exp.plan.application, target_profile='openmeter-compose',
        fault_type=FAULT_MAP[exp.plan.failure_type.value], status=status,
        started_at=exp.timestamps.get('planning_at') or utcnow(),
        finished_at=exp.timestamps.get('completed_at') if terminal else None,
        baseline=summary('baseline', ExperimentPhase.BASELINE),
        during_fault=summary('fault', ExperimentPhase.INJECTION),
        recovery=summary('recovery', ExperimentPhase.RECOVERY),
        recovery_status=RecoveryStatus.VERIFIED if recovered else RecoveryStatus.FAILED if exp.status == Status.recovery_failed else RecoveryStatus.RECOVERING if terminal else RecoveryStatus.NOT_STARTED,
        recovery_verified=recovered,
        detection_latency_ms=exp.report.metrics['recovery'].get('detection_latency_ms') if exp.report else None,
        recovery_time_ms=exp.report.metrics['recovery'].get('functional_recovery_seconds') * 1000 if exp.report and exp.report.metrics['recovery'].get('functional_recovery_seconds') is not None else None,
        evidence_refs=[p.endpoint for p in exp.probes if not p.simulated][:20],
        observer_notes=['Simulated experiment; no real measurements' if exp.simulated else 'HTTP and synthetic event observations from ChaosAI'],
    )
