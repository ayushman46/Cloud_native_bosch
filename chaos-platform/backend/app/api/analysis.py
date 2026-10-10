"""Analyzer endpoints."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from ..agents.evidence import build_diagnostic_evidence
from ..container import Container
from ..evaluation.metrics import evaluate_against_truth
from ..schemas.analysis import AnalysisRequest, AnalysisResult, GroundTruthEvaluation

router = APIRouter(tags=["analysis"])


def _c(request: Request) -> Container:
    return request.app.state.container


@router.post("/api/analysis", response_model=AnalysisResult)
async def run_analysis(body: AnalysisRequest, request: Request) -> AnalysisResult:
    """Analyze an Observation Report (the stored one unless supplied). If the LLM is unavailable the
    response has status 'unavailable' and NO report - the observation itself is unaffected."""
    c = _c(request)
    report = body.observation_report or c.repo.get_observation(body.experiment_id)
    if report is None:
        raise HTTPException(409, f"no Observation Report for {body.experiment_id} yet")
    if report.experiment_id != body.experiment_id:
        raise HTTPException(422, "experiment_id mismatch")
    result = await c.analyzer.analyze(build_diagnostic_evidence(report))
    c.repo.save_analysis(result)
    return result


@router.get("/api/analysis/{experiment_id}", response_model=AnalysisResult)
async def get_analysis(experiment_id: str, request: Request) -> AnalysisResult:
    result = _c(request).repo.get_analysis(experiment_id)
    if result is None:
        raise HTTPException(404, f"no analysis stored for {experiment_id}")
    return result


@router.get("/api/analysis/{experiment_id}/evaluation", response_model=GroundTruthEvaluation)
async def evaluate(experiment_id: str, request: Request) -> GroundTruthEvaluation:
    """Compare the stored diagnosis with the hidden ground truth (only possible after diagnosis)."""
    c = _c(request)
    result, truth = c.repo.get_analysis(experiment_id), c.repo.get_ground_truth(experiment_id)
    if result is None or truth is None:
        raise HTTPException(404, "need both a stored analysis and a recorded ground truth")
    ev = evaluate_against_truth(result, truth)
    if ev is None:
        raise HTTPException(409, f"analysis has no report (status={result.status.value})")
    return ev
