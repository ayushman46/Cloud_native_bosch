"""Observer Agent: deterministic, LLM-free. Owns one ExperimentMonitor per experiment."""
from __future__ import annotations

import asyncio

import httpx

from ..clients.openmeter_client import OpenMeterClient
from ..config import Settings
from ..monitoring.collector import ExperimentMonitor, MonitorConfig, ResourceProbe
from ..monitoring.records import Clock
from ..schemas.analysis import ExecutionGroundTruth
from ..schemas.injection import InjectionEvent
from ..schemas.monitoring import ObservationReport
from ..storage.repository import InMemoryRepository


def monitor_config_from_settings(s: Settings) -> MonitorConfig:
    base = s.openmeter_base_url.rstrip("/")
    targets = {"openmeter-api": f"{base}{s.openmeter_health_path}"}
    if s.sink_worker_health_url:
        targets["sink-worker"] = s.sink_worker_health_url
    if s.balance_worker_health_url:
        targets["balance-worker"] = s.balance_worker_health_url
    return MonitorConfig(
        primary_service="openmeter-api",
        health_targets=targets,
        meter_slug=s.openmeter_meter_slug,
        event_type=s.openmeter_event_type,
    )


class ObserverAgent:
    def __init__(
        self,
        settings: Settings,
        repository: InMemoryRepository,
        *,
        monitor_config: MonitorConfig | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Clock | None = None,
        resource_probe: ResourceProbe | None = None,
    ) -> None:
        self.settings = settings
        self.repo = repository
        self.monitor_config = monitor_config or monitor_config_from_settings(settings)
        self._transport = transport
        self._clock = clock
        self._resource_probe = resource_probe
        self.monitors: dict[str, ExperimentMonitor] = {}
        self._clients: dict[str, OpenMeterClient] = {}
        self._watchers: dict[str, asyncio.Task] = {}

    async def start_baseline(self, experiment_id: str, target_service: str | None = None) -> ExperimentMonitor:
        if experiment_id in self.monitors:
            return self.monitors[experiment_id]
        client = OpenMeterClient(
            self.settings.openmeter_base_url,
            self.settings.openmeter_api_key,
            timeout_seconds=self.settings.probe_timeout_seconds,
            transport=self._transport,
            clock=self._clock,
        )
        mon = ExperimentMonitor(experiment_id, client, self.monitor_config, clock=self._clock,
                                resource_probe=self._resource_probe, target_service=target_service)
        self.monitors[experiment_id] = mon
        self._clients[experiment_id] = client
        await mon.start()
        self._watchers[experiment_id] = asyncio.create_task(self._persist_when_done(mon))
        return mon

    async def _persist_when_done(self, mon: ExperimentMonitor) -> None:
        report = await mon.wait_finalized()
        if report is not None:
            self.repo.save_observation(report)

    def handle_event(self, event: InjectionEvent) -> ExperimentMonitor:
        mon = self.monitors.get(event.experiment_id)
        if mon is None:
            raise KeyError(event.experiment_id)
        mon.apply_event(event)
        # ground truth is stored apart from observations and is never handed to the Analyzer
        ts = event.timestamps
        self.repo.save_ground_truth(ExecutionGroundTruth(
            experiment_id=event.experiment_id,
            failure_type=event.failure_type,
            target_service=event.target_service,
            fault_activated_at=ts.fault_activated_at,
            actual_parameters=event.parameters,
        ))
        return mon

    async def finalize(self, experiment_id: str) -> ObservationReport:
        mon = self.monitors[experiment_id]
        report = await mon.finalize()
        self.repo.save_observation(report)
        return report

    def get(self, experiment_id: str) -> ExperimentMonitor | None:
        return self.monitors.get(experiment_id)

    async def shutdown(self) -> None:
        for mon in self.monitors.values():
            await mon.stop()
        for w in self._watchers.values():
            w.cancel()
        await asyncio.gather(*self._watchers.values(), return_exceptions=True)
        for c in self._clients.values():
            await c.aclose()
