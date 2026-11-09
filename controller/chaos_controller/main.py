"""Chaos Control API: the only component allowed to touch Docker.

It runs next to the sample app (OpenMeter), accepts bounded fault requests from ChaosAI,
verifies every activation and rollback with evidence, and rolls back on its own watchdog
even if ChaosAI disconnects. Faults only ever target allowlisted compose services of the
project it is configured for.
"""
import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from hmac import compare_digest
from typing import Any

import docker
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

log = logging.getLogger('uvicorn.error')  # shows up in `docker logs` next to the access log
API_KEY = os.environ.get('CHAOS_CONTROL_API_KEY', '')
PROJECT = os.environ.get('CHAOS_COMPOSE_PROJECT', 'chaosai')
TARGETS = [x.strip() for x in os.environ.get('ALLOWED_TARGETS', 'openmeter,sink-worker,kafka,clickhouse,redis,postgres').split(',') if x.strip()]
PROXY_TARGETS = ['openmeter']
CRASHABLE = {'openmeter', 'sink-worker', 'kafka', 'clickhouse', 'redis', 'postgres'}
TOXIPROXY_URL = os.environ.get('TOXIPROXY_URL', 'http://toxiproxy:8474')
PROXY_LISTEN = os.environ.get('PROXY_LISTEN', '0.0.0.0:8666')
PROXY_UPSTREAM = os.environ.get('PROXY_UPSTREAM', 'openmeter:8888')
LOAD_URL = os.environ.get('LOAD_URL', 'http://openmeter:8888/api/v1/events')  # real usage-event ingestion, so Kafka and the sink worker feel the spike
MAX_DURATION = int(os.environ.get('MAX_DURATION_SECONDS', '30'))
MAX_LATENCY_MS = int(os.environ.get('MAX_LATENCY_MS', '2000'))
MAX_RPS = int(os.environ.get('MAX_RPS', '20'))
WATCHDOG_GRACE = float(os.environ.get('WATCHDOG_GRACE_SECONDS', '5'))
HEAL_TIMEOUT = float(os.environ.get('HEAL_TIMEOUT_SECONDS', '120'))
FAULTS = ['application_crash', 'network_latency', 'service_restart', 'traffic_spike']
EVIDENCE = {'application_crash': 'container_exited', 'network_latency': 'toxic_active', 'service_restart': 'container_restarted', 'traffic_spike': 'load_generator_running'}


def now() -> datetime:
    return datetime.now(timezone.utc)


class Params(BaseModel):
    model_config = ConfigDict(extra='forbid')
    duration_seconds: int = Field(default=10, ge=1)
    latency_ms: int | None = Field(default=None, ge=1)
    requests_per_second: int | None = Field(default=None, ge=1)
    observation_seconds: int = Field(default=10, ge=1)


class Submission(BaseModel):
    model_config = ConfigDict(extra='ignore')
    experiment_id: str = Field(min_length=1)
    application: str = 'openmeter'
    target_service: str
    failure_type: str
    parameters: Params


class Evidence(BaseModel):
    evidence_type: str
    target_service: str
    observed_at: datetime
    details: dict[str, Any] = {}
    simulated: bool = False


class Status(BaseModel):
    experiment_id: str
    status: str
    watchdog_armed: bool = False
    injection_verified: bool = False
    rollback_verified: bool = False
    activation_evidence: Evidence | None = None
    rollback_evidence: Evidence | None = None
    fault_activated_at: datetime | None = None
    rollback_started_at: datetime | None = None
    rollback_completed_at: datetime | None = None
    message: str = ''


class Chaos:
    def __init__(self):
        self.docker = docker.from_env()
        self.http = httpx.AsyncClient(timeout=5)
        self.states: dict[str, Status] = {}
        self.subs: dict[str, Submission] = {}
        self.tasks: dict[str, list[asyncio.Task]] = {}
        self.locks: dict[str, asyncio.Lock] = {}
        self.load_stats: dict[str, dict] = {}

    # --- docker helpers (blocking SDK calls run in a thread) ---
    def _container(self, service: str):
        found = self.docker.containers.list(all=True, filters={'label': [f'com.docker.compose.project={PROJECT}', f'com.docker.compose.service={service}']})
        if not found:
            raise RuntimeError(f'No container for service {service!r} in project {PROJECT!r}')
        return found[0]

    def _healthy(self, c) -> bool:
        c.reload()
        state = c.attrs['State']
        health = state.get('Health', {}).get('Status')
        return bool(state.get('Running')) and health in (None, 'healthy')

    def _is_exited(self, c) -> bool:
        c.reload()
        return c.status == 'exited'

    async def _wait(self, predicate, timeout: float, what: str):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if await asyncio.to_thread(predicate):
                return
            await asyncio.sleep(0.5)
        raise RuntimeError(f'Timed out waiting for {what}')

    # --- toxiproxy ---
    async def ensure_proxy(self) -> bool:
        try:
            r = await self.http.get(f'{TOXIPROXY_URL}/proxies/openmeter')
            if r.status_code == 404:
                r = await self.http.post(f'{TOXIPROXY_URL}/proxies', json={'name': 'openmeter', 'listen': PROXY_LISTEN, 'upstream': PROXY_UPSTREAM, 'enabled': True})
            r.raise_for_status()
            return True
        except httpx.HTTPError:
            return False

    async def toxics(self) -> list[dict]:
        r = await self.http.get(f'{TOXIPROXY_URL}/proxies/openmeter/toxics')
        r.raise_for_status()
        return r.json()

    # --- fault activation / rollback ---
    async def activate(self, eid: str, sub: Submission) -> Evidence:
        fault, target, p = sub.failure_type, sub.target_service, sub.parameters
        details: dict[str, Any] = {'fault_type': fault}
        if fault == 'application_crash':
            c = await asyncio.to_thread(self._container, target)
            await asyncio.to_thread(c.kill)
            await self._wait(lambda: self._is_exited(c), 15, 'container exit')
            details.update(container=c.name, state=c.status)
        elif fault == 'service_restart':
            c = await asyncio.to_thread(self._container, target)
            before = c.attrs['State']['StartedAt']
            await asyncio.to_thread(c.restart, timeout=5)
            c.reload()
            if c.attrs['State']['StartedAt'] == before:
                raise RuntimeError('Container start time did not change after restart')
            details.update(container=c.name, started_at=c.attrs['State']['StartedAt'])
        elif fault == 'network_latency':
            r = await self.http.post(f'{TOXIPROXY_URL}/proxies/openmeter/toxics', json={'name': f'latency_{eid}', 'type': 'latency', 'stream': 'downstream', 'toxicity': 1.0, 'attributes': {'latency': p.latency_ms, 'jitter': 0}})
            r.raise_for_status()
            if not any(t['name'] == f'latency_{eid}' for t in await self.toxics()):
                raise RuntimeError('Toxic not visible after creation')
            details.update(proxy='openmeter', latency_ms=p.latency_ms)
        elif fault == 'traffic_spike':
            stats = {'sent': 0, 'ok': 0, 'errors': 0}
            self.load_stats[eid] = stats
            self.tasks[eid].append(asyncio.create_task(self._load(eid, stats, p.requests_per_second, p.duration_seconds), name='load'))
            await self._wait(lambda: stats['sent'] > 0, 5, 'load generator start')
            details.update(requests_per_second=p.requests_per_second, url=LOAD_URL)
        log.warning('FAULT ACTIVATED  %s on %s (experiment %s) evidence=%s %s', fault, target, eid, EVIDENCE[fault], details)
        return Evidence(evidence_type=EVIDENCE[fault], target_service=target, observed_at=now(), details=details)

    async def _load(self, eid: str, stats: dict, rps: int, duration: int):
        """Bounded load: `rps` CloudEvents per second to OpenMeter's ingest endpoint for `duration` seconds."""
        start = time.monotonic()
        end = start + duration
        async with httpx.AsyncClient(timeout=3) as client:
            async def one():
                stats['sent'] += 1
                event = {'specversion': '1.0', 'type': 'request', 'id': f'{eid}-load-{stats["sent"]}', 'source': 'chaos-controller',
                         'subject': 'chaos-load', 'data': {'method': 'GET', 'route': '/chaos/load'}}
                try:
                    r = await client.post(LOAD_URL, json=event, headers={'Content-Type': 'application/cloudevents+json'})
                    stats['ok' if r.status_code < 300 else 'errors'] += 1
                except httpx.HTTPError:
                    stats['errors'] += 1
            while time.monotonic() < end:
                tick = time.monotonic()
                await asyncio.gather(*(one() for _ in range(rps)))
                log.warning('LOAD             experiment %s t=%ds sent=%d ok=%d errors=%d (target %d/s)', eid, round(time.monotonic() - start), stats['sent'], stats['ok'], stats['errors'], rps)
                await asyncio.sleep(max(0, 1 - (time.monotonic() - tick)))

    async def rollback(self, eid: str) -> Evidence:
        sub = self.subs[eid]
        fault, target = sub.failure_type, sub.target_service
        details: dict[str, Any] = {'fault_type': fault}
        loads = [t for t in self.tasks.get(eid, []) if t.get_name() == 'load']
        for t in loads:
            if not t.done():
                t.cancel()
        if fault in ('application_crash', 'service_restart'):
            c = await asyncio.to_thread(self._container, target)
            c.reload()
            if c.status != 'running':
                await asyncio.to_thread(c.start)
            await self._wait(lambda: self._healthy(c), HEAL_TIMEOUT, f'{target} healthy')
            details.update(container=c.name, healthy=True)
        elif fault == 'network_latency':
            r = await self.http.delete(f'{TOXIPROXY_URL}/proxies/openmeter/toxics/latency_{eid}')
            if r.status_code not in (204, 404):
                r.raise_for_status()
            remaining = await self.toxics()
            if any(t['name'] == f'latency_{eid}' for t in remaining):
                raise RuntimeError('Toxic still present after removal')
            details.update(proxy='openmeter', toxics_remaining=len(remaining))
        elif fault == 'traffic_spike':
            await asyncio.gather(*loads, return_exceptions=True)
            details.update(**self.load_stats.get(eid, {}))
        log.warning('ROLLBACK VERIFIED %s on %s (experiment %s) %s', fault, target, eid, details)
        return Evidence(evidence_type='fault_removed', target_service=target, observed_at=now(), details=details)

    async def finish(self, eid: str, final: str):
        """Idempotent rollback shared by the lifecycle, abort and the watchdog."""
        async with self.locks[eid]:
            st = self.states[eid]
            if st.rollback_verified:
                return
            st.status = 'rolling_back'
            st.rollback_started_at = st.rollback_started_at or now()
            log.warning('ROLLING BACK     experiment %s (%s)', eid, final)
            try:
                ev = await self.rollback(eid)
            except Exception as exc:
                st.status, st.message = 'failed', f'Rollback failed: {exc}'
                return
            st.rollback_evidence, st.rollback_completed_at, st.rollback_verified = ev, ev.observed_at, True
            st.status = final

    async def lifecycle(self, eid: str):
        st, sub = self.states[eid], self.subs[eid]
        try:
            ev = await self.activate(eid, sub)
            st.activation_evidence, st.fault_activated_at, st.injection_verified, st.status = ev, ev.observed_at, True, 'active'
            await asyncio.sleep(sub.parameters.duration_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            st.message = f'Activation failed: {exc}'
            await self.finish(eid, 'failed')
            return
        await self.finish(eid, 'rolled_back')

    async def watchdog(self, eid: str):
        """Independent deadline: forces rollback even if the lifecycle task or ChaosAI is gone."""
        await asyncio.sleep(self.subs[eid].parameters.duration_seconds + WATCHDOG_GRACE)
        if not self.states[eid].rollback_verified:
            self.states[eid].message += ' Watchdog forced rollback.'
            await self.finish(eid, 'rolled_back')

    async def heal_all(self):
        """Startup reconciliation: leave the sample app and proxy in a clean state."""
        for target in TARGETS:
            try:
                c = await asyncio.to_thread(self._container, target)
                if c.status != 'running':
                    await asyncio.to_thread(c.start)
            except Exception:
                pass
        if await self.ensure_proxy():
            try:
                for t in await self.toxics():
                    await self.http.delete(f'{TOXIPROXY_URL}/proxies/openmeter/toxics/{t["name"]}')
            except httpx.HTTPError:
                pass


chaos: Chaos | None = None
app = FastAPI(title='Chaos Control API')


@app.on_event('startup')
async def startup():
    global chaos
    if not API_KEY:
        raise RuntimeError('CHAOS_CONTROL_API_KEY is required')
    chaos = Chaos()
    await chaos.heal_all()


@app.middleware('http')
async def auth(request: Request, call_next):
    header = request.headers.get('Authorization', '')
    if not header.startswith('Bearer ') or not compare_digest(header[7:].strip().encode(), API_KEY.encode()):
        return JSONResponse({'detail': 'authentication required'}, status_code=401)
    return await call_next(request)


@app.get('/api/health')
async def health():
    return {'status': 'ok', 'project': PROJECT}


@app.get('/api/capabilities')
async def capabilities():
    ready = await chaos.ensure_proxy()
    return {'targets': TARGETS, 'fault_types': FAULTS, 'watchdog_supported': True, 'network_latency_ready': ready, 'proxy_targets': PROXY_TARGETS if ready else [], 'traffic_spike_ready': True}


@app.get('/api/topology')
async def topology():
    """Services of the sandbox project, read from the running containers (compose labels), not hard-coded."""
    def read():
        out = []
        for c in chaos.docker.containers.list(all=True, filters={'label': f'com.docker.compose.project={PROJECT}'}):
            labels = c.labels
            deps = [d.split(':')[0] for d in labels.get('com.docker.compose.depends_on', '').split(',') if d]
            out.append({'service': labels.get('com.docker.compose.service', c.name), 'image': (c.image.tags[0] if c.image.tags else c.attrs['Config']['Image']),
                        'state': c.status, 'health': c.attrs['State'].get('Health', {}).get('Status'), 'depends_on': deps})
        return sorted(out, key=lambda x: x['service'])
    return {'project': PROJECT, 'services': await asyncio.to_thread(read)}


def active_experiment() -> str | None:
    for eid, st in chaos.states.items():
        if st.status in ('armed', 'active', 'rolling_back'):
            return eid
    return None


@app.post('/api/experiments')
async def submit(sub: Submission, request: Request):
    key = request.headers.get('Idempotency-Key')
    if key != sub.experiment_id:
        raise HTTPException(422, 'Idempotency-Key must equal experiment_id')
    if sub.experiment_id in chaos.states:
        if chaos.subs[sub.experiment_id] != sub:
            raise HTTPException(409, 'Idempotency key already belongs to a different plan')
        return chaos.states[sub.experiment_id]
    p, fault = sub.parameters, sub.failure_type
    problems = []
    if sub.application != 'openmeter':
        problems.append('application not allowed')
    if sub.target_service not in TARGETS:
        problems.append('target not allowlisted')
    if fault not in FAULTS:
        problems.append('fault not allowlisted')
    if p.duration_seconds > MAX_DURATION:
        problems.append('duration over limit')
    if fault == 'network_latency' and (not p.latency_ms or p.latency_ms > MAX_LATENCY_MS or sub.target_service not in PROXY_TARGETS or not await chaos.ensure_proxy()):
        problems.append('latency invalid, over limit, or target not behind proxy')
    if fault == 'traffic_spike' and (not p.requests_per_second or p.requests_per_second > MAX_RPS or sub.target_service != 'openmeter'):
        problems.append('traffic spike invalid, over limit, or wrong target')
    if fault == 'application_crash' and sub.target_service not in CRASHABLE:
        problems.append('crash target not allowed')
    if problems:
        chaos.subs[sub.experiment_id] = sub
        log.warning('REJECTED         experiment %s: %s', sub.experiment_id, '; '.join(problems))
        chaos.states[sub.experiment_id] = Status(experiment_id=sub.experiment_id, status='rejected', message='; '.join(problems))
        return chaos.states[sub.experiment_id]
    busy = active_experiment()
    if busy:
        raise HTTPException(409, f'Experiment {busy} is already running')
    eid = sub.experiment_id
    chaos.subs[eid] = sub
    chaos.locks[eid] = asyncio.Lock()
    chaos.tasks[eid] = []
    chaos.states[eid] = Status(experiment_id=eid, status='armed', watchdog_armed=True, message='Watchdog armed before activation')
    log.warning('WATCHDOG ARMED   experiment %s: %s on %s for %ss (deadline +%ss)', eid, fault, sub.target_service, p.duration_seconds, WATCHDOG_GRACE)
    # watchdog first, then the fault
    chaos.tasks[eid].append(asyncio.create_task(chaos.watchdog(eid), name='watchdog'))
    chaos.tasks[eid].append(asyncio.create_task(chaos.lifecycle(eid), name='lifecycle'))
    return chaos.states[eid]


@app.get('/api/experiments/{eid}')
async def status(eid: str):
    if eid not in chaos.states:
        raise HTTPException(404, 'unknown experiment')
    return chaos.states[eid]


@app.post('/api/experiments/{eid}/abort')
async def abort(eid: str):
    if eid not in chaos.states:
        raise HTTPException(404, 'unknown experiment')
    st = chaos.states[eid]
    if st.status == 'rejected':
        return st
    for t in chaos.tasks[eid]:
        if t.get_name() == 'lifecycle' and not t.done():
            t.cancel()
    # Rolling back can take as long as the service needs to become healthy, so do not hold the request open:
    # start it, give it a moment, and let the caller poll GET /api/experiments/{id} for rollback_verified.
    task = asyncio.create_task(chaos.finish(eid, 'aborted'), name='abort-rollback')
    chaos.tasks[eid].append(task)
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=2)
    except asyncio.TimeoutError:
        pass
    return st
