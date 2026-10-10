import time
import httpx
from .config import Settings
from .schemas import Probe, SyntheticEvent


class Observer:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = httpx.AsyncClient(timeout=3)
        self._mock_events: dict[str, int] = {}
        self.mock_controller = None
        self.last_query_error: str | None = None

    def _active_mock_fault(self):
        if self.mock_controller:
            for eid, state in self.mock_controller.states.items():
                if state.status == 'active':
                    return self.mock_controller.plans[eid]
        return None

    async def close(self):
        await self.client.aclose()

    async def probe(self, phase: str) -> list[Probe]:
        if self.settings.mode == 'mock':
            fault = self._active_mock_fault()
            reachable, latency = True, 12
            if fault:
                if fault.failure_type.value == 'application_crash':
                    reachable, latency = False, 4
                elif fault.failure_type.value == 'network_latency':
                    latency += fault.parameters.latency_ms
                elif fault.failure_type.value == 'traffic_spike':
                    latency = 120 + 8 * fault.parameters.requests_per_second
                else:
                    latency = 40
            return [Probe(phase=phase, endpoint='mock://openmeter/health', reachable=reachable, latency_ms=latency, status_code=200 if reachable else 503, simulated=True)]
        if not self.settings.openmeter_base_url:
            return [Probe(phase=phase, endpoint='unconfigured', reachable=False, error='OPENMETER_BASE_URL is missing')]
        paths = [self.settings.openmeter_health_path]
        for optional in [self.settings.openmeter_ingest_path, self.settings.openmeter_query_path]:
            if optional and optional not in paths:
                paths.append(optional)
        result = []
        for path in paths:
            url = self.settings.openmeter_base_url.rstrip('/') + '/' + path.lstrip('/')
            started = time.monotonic()
            try:
                response = await self.client.get(url)
                result.append(Probe(phase=phase, endpoint=url, reachable=200 <= response.status_code < 400, status_code=response.status_code, latency_ms=round((time.monotonic() - started) * 1000, 2), error=f'HTTP {response.status_code}' if response.status_code >= 400 else None))
            except httpx.HTTPError as exc:
                result.append(Probe(phase=phase, endpoint=url, reachable=False, error=str(exc)))
        return result

    async def send_event(self, experiment_id: str, phase: str) -> SyntheticEvent:
        event_id = f'{experiment_id}-{phase}'
        if self.settings.mode == 'mock':
            fault = self._active_mock_fault()
            accepted = not fault or fault.failure_type.value != 'application_crash'
            self._mock_events[experiment_id] = self._mock_events.get(experiment_id, 0) + int(accepted)
            return SyntheticEvent(event_id=event_id, phase=phase, accepted=accepted, status_code=204 if accepted else 503, latency_ms=12, simulated=True)
        url = self.settings.openmeter_base_url.rstrip('/') + '/api/v1/events'
        payload = {'specversion': '1.0', 'type': 'request', 'id': event_id, 'source': 'chaosai', 'subject': f'chaosai-{experiment_id}', 'data': {'method': 'GET', 'route': f'/chaosai/{phase}'}}
        started = time.monotonic()
        try:
            response = await self.client.post(url, json=payload, headers={'Content-Type': 'application/cloudevents+json'})
            return SyntheticEvent(event_id=event_id, phase=phase, accepted=200 <= response.status_code < 300, status_code=response.status_code, latency_ms=round((time.monotonic()-started)*1000, 2))
        except httpx.HTTPError as exc:
            return SyntheticEvent(event_id=event_id, phase=phase, accepted=False, error=str(exc))

    async def query_count(self, experiment_id: str) -> float | None:
        if self.settings.mode == 'mock':
            count = self._mock_events.get(experiment_id, 0)
            fault = self._active_mock_fault()
            return float(max(0, count - 1) if fault and fault.failure_type.value == 'service_restart' else count)
        url = self.settings.openmeter_base_url.rstrip('/') + f'/api/v1/meters/{self.settings.openmeter_meter_slug}/query'
        try:
            self.last_query_error = None
            response = await self.client.get(url, params={'subject': f'chaosai-{experiment_id}'})
            response.raise_for_status()
            data = response.json().get('data', [])
            return sum(float(row['value']) for row in data)
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            self.last_query_error = type(exc).__name__
            return None
