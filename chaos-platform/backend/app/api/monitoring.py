"""Observer endpoints. The Observer never needs the LLM, so none of these touch NVIDIA."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, status

from ..container import Container
from ..schemas.injection import InjectionEvent
from ..schemas.monitoring import MetricsResponse, MonitoringStartRequest, ObservationReport

router = APIRouter(tags=["monitoring"])


def _c(request: Request) -> Container:
    return request.app.state.container


@router.post("/api/monitoring/start", status_code=status.HTTP_202_ACCEPTED)
async def start_monitoring(body: MonitoringStartRequest, request: Request) -> dict:
    """Start baseline monitoring. Call BEFORE injecting; poll `baseline_ready` on the metrics endpoint."""
    mon = await _c(request).observer.start_baseline(body.experiment_id, body.target_service)
    return {"experiment_id": body.experiment_id, "state": mon.state}


@router.post("/api/monitoring/{experiment_id}/event")
async def lifecycle_event(experiment_id: str, event: InjectionEvent, request: Request) -> dict:
    """Injector lifecycle event (fault activation, rollback started/completed). Safe to resend."""
    if event.experiment_id != experiment_id:
        raise HTTPException(422, "experiment_id in path and body differ")
    try:
        mon = _c(request).observer.handle_event(event)
    except KeyError:
        raise HTTPException(404, f"monitoring for {experiment_id} has not been started") from None
    lc = mon.lifecycle
    return {"experiment_id": experiment_id, "phase": mon.lifecycle.phase_at(mon.clock.now()), "notes": mon.notes,
            "fault_activated_at": lc.fault_activated_at, "rollback_completed_at": lc.rollback_completed_at}


@router.post("/api/monitoring/{experiment_id}/finalize", response_model=ObservationReport)
async def finalize(experiment_id: str, request: Request) -> ObservationReport:
    """Stop probing, drain the meter, and produce the Observation Report (also happens automatically
    once recovery is confirmed or the recovery deadline passes)."""
    try:
        return await _c(request).observer.finalize(experiment_id)
    except KeyError:
        raise HTTPException(404, f"unknown experiment {experiment_id}") from None


@router.get("/api/experiments/{experiment_id}/metrics", response_model=MetricsResponse)
async def get_metrics(experiment_id: str, request: Request) -> MetricsResponse:
    mon = _c(request).observer.get(experiment_id)
    if mon is None:
        raise HTTPException(404, f"unknown experiment {experiment_id}")
    snap = mon.snapshot()
    return MetricsResponse(**snap, report=mon.report)


@router.get("/api/experiments/{experiment_id}/observations", response_model=ObservationReport)
async def get_observations(experiment_id: str, request: Request) -> ObservationReport:
    c = _c(request)
    report = c.repo.get_observation(experiment_id)
    if report is None:
        mon = c.observer.get(experiment_id)
        if mon is None:
            raise HTTPException(404, f"unknown experiment {experiment_id}")
        raise HTTPException(409, f"observation report not ready (state={mon.state}); POST .../finalize or wait")
    return report
