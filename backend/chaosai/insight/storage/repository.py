"""In-memory store (hackathon scope). Keeps the injected-fault ground truth apart from observations
so the Analyzer can never read it while diagnosing."""
from __future__ import annotations

from ..schemas.analysis import AnalysisResult, ExecutionGroundTruth
from ..schemas.monitoring import ObservationReport


class InMemoryRepository:
    def __init__(self) -> None:
        self._truth: dict[str, ExecutionGroundTruth] = {}
        self._obs: dict[str, ObservationReport] = {}
        self._analysis: dict[str, AnalysisResult] = {}

    # ground truth: written by the Observer from injection events, read ONLY by evaluation
    def save_ground_truth(self, gt: ExecutionGroundTruth) -> None:
        self._truth[gt.experiment_id] = gt

    def get_ground_truth(self, experiment_id: str) -> ExecutionGroundTruth | None:
        return self._truth.get(experiment_id)

    def save_observation(self, report: ObservationReport) -> None:
        self._obs[report.experiment_id] = report

    def get_observation(self, experiment_id: str) -> ObservationReport | None:
        return self._obs.get(experiment_id)

    def save_analysis(self, result: AnalysisResult) -> None:
        self._analysis[result.experiment_id] = result

    def get_analysis(self, experiment_id: str) -> AnalysisResult | None:
        return self._analysis.get(experiment_id)
