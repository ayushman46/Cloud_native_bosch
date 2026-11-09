import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from .config import Settings
from .schemas import PlanRequest, RunRequest, Experiment
from .storage import Store
from .controller import MockController, RemoteController
from .observer import Observer
from .llm import NvidiaClient
from .agents import Planner, Analyzer
from .orchestration import Runner, planning_context
from .observer_adapter import to_shared_report


class QuietPolling(logging.Filter):
    """Hide the health check and the dashboard's 1.5 s polling from the access log so experiment events stand out."""
    def filter(self, record: logging.LogRecord) -> bool:
        line = record.getMessage()
        return not ('GET /healthz' in line or ('GET /api/' in line and 'POST' not in line))


def create_app(settings: Settings | None = None):
    settings = settings or Settings()
    if settings.mode not in ('mock', 'real'):
        raise ValueError('MODE must be mock or real')
    if settings.mode == 'real' and (not settings.chaos_control_url or not settings.chaos_control_api_key or not settings.openmeter_base_url or not settings.chaosai_api_key):
        raise ValueError('Real mode requires CHAOS_CONTROL_URL, CHAOS_CONTROL_API_KEY, CHAOSAI_API_KEY, and OPENMETER_BASE_URL')
    store = Store(settings.database_path)
    controller = MockController(settings) if settings.mode == 'mock' else RemoteController(settings)
    observer = Observer(settings)
    llm = NvidiaClient(settings)
    planner = Planner(settings, llm)
    analyzer = Analyzer(settings, llm)
    insight = None
    if settings.mode == 'real':
        from .insight.config import Settings as InsightSettings
        from .insight.container import build_container
        insight = build_container(InsightSettings(openmeter_base_url=settings.openmeter_base_url, openmeter_health_path=settings.openmeter_health_path,
                                                  openmeter_meter_slug=settings.openmeter_meter_slug, nvidia_api_key=settings.nvidia_api_key or None,
                                                  **({'nvidia_model': settings.nvidia_model} if settings.nvidia_model else {}), nvidia_base_url=settings.nvidia_base_url))
    runner = Runner(settings, store, controller, observer, planner, analyzer, insight)

    @asynccontextmanager
    async def lifespan(app):
        await runner.reconcile_interrupted()
        try:
            await controller.health()
            await controller.capabilities()
            app.state.controller_startup_ok = True
        except Exception as exc:
            app.state.controller_startup_ok = False
            app.state.controller_startup_error = str(exc)
        startup_probes = await observer.probe('startup')
        app.state.openmeter_startup_ok = settings.mode == 'real' and bool(startup_probes) and all(p.reachable for p in startup_probes)
        yield
        await runner.close()
        if insight:
            await insight.shutdown()
        await controller.close()
        await observer.close()
        await llm.close()

    logging.getLogger('uvicorn.access').addFilter(QuietPolling())
    app = FastAPI(title='ChaosAI', lifespan=lifespan)
    app.add_middleware(CORSMiddleware, allow_origins=[settings.frontend_origin], allow_methods=['*'], allow_headers=['*'])

    @app.middleware('http')
    async def protect_api(request: Request, call_next):
        if settings.mode == 'real' and request.url.path.startswith('/api/'):
            from hmac import compare_digest
            from fastapi.responses import JSONResponse
            header = request.headers.get('Authorization', '')
            token = header.removeprefix('Bearer ').strip()
            if not header.startswith('Bearer ') or not compare_digest(token.encode(), settings.chaosai_api_key.encode()):
                return JSONResponse({'detail': 'ChaosAI API authentication required'}, status_code=401)
        return await call_next(request)

    @app.get('/api/health')
    async def health():
        result = {'mode': settings.mode, 'controller_reachable': False, 'controller_authenticated': False, 'openmeter_reachable': False, 'monitoring_reachable': False, 'errors': []}
        try:
            await controller.health()
            result['controller_reachable'] = True
            await controller.capabilities()
            result['controller_authenticated'] = True
        except Exception as exc:
            result['errors'].append(str(exc))
        probes = await observer.probe('connectivity')
        result['openmeter_reachable'] = bool(probes) and all(p.reachable for p in probes) and settings.mode == 'real'
        result['monitoring_reachable'] = result['openmeter_reachable']
        if settings.mode == 'mock':
            result['simulated'] = True
        elif not result['openmeter_reachable']:
            result['errors'].append('OpenMeter probe failed')
        return result

    @app.get('/healthz')
    async def liveness():
        return {'status': 'ok'}

    @app.get('/api/config')
    async def configuration():
        return {'mode': settings.mode, 'targets': settings.targets, 'nvidia_configured': bool(settings.nvidia_api_key and settings.nvidia_model), 'controller_configured': bool(settings.chaos_control_url and settings.chaos_control_api_key), 'openmeter_configured': bool(settings.openmeter_base_url), 'synthetic_events_enabled': settings.enable_synthetic_events, 'real_injections_enabled': settings.real_injections_enabled, 'auto_stop': {'enabled': settings.safety_auto_stop, 'error_rate': settings.abort_error_rate, 'latency_ms': settings.abort_latency_ms, 'min_probes': settings.abort_min_probes}, 'limits': {'duration_seconds': settings.max_duration_seconds, 'latency_ms': settings.max_latency_ms, 'requests_per_second': settings.max_rps}}

    @app.get('/api/dependency-map')
    async def dependency_map():
        """Documented data flows, merged with dependencies read from the running containers when the controller is reachable."""
        from .insight.monitoring.dependency_map import OPENMETER_DEPENDENCY_MAP
        # 'openmeter-api' in the documented map is the compose service named 'openmeter'
        alias = {'openmeter': 'openmeter-api'}
        deployed = {'client', 'openmeter-api', 'sink-worker', 'balance-worker', 'kafka', 'clickhouse', 'redis', 'postgres'}
        edges = {(e.source, e.target): {**e.model_dump(), 'origin': 'documented'} for e in OPENMETER_DEPENDENCY_MAP if e.source in deployed and e.target in deployed}
        derived, source = 0, 'documented flows only (no running containers to read in mock mode)'
        try:
            topo = await controller.topology()
            if not topo.get('simulated'):
                for svc in topo['services']:
                    for dep in svc['depends_on']:
                        key = (alias.get(svc['service'], svc['service']), alias.get(dep, dep))
                        if key[0] in deployed and key[1] in deployed and key not in edges:
                            edges[key] = {'source': key[0], 'target': key[1], 'relation': 'depends_on', 'origin': 'derived'}
                            derived += 1
                source = f"documented flows plus {derived} dependencies derived from {len(topo['services'])} running containers"
        except Exception as exc:
            source = f'documented flows only (controller topology unavailable: {exc})'
        return {'application': 'openmeter', 'edges': list(edges.values()), 'target_aliases': alias, 'source': source}

    @app.get('/api/capabilities')
    async def capabilities():
        try:
            return await controller.capabilities()
        except Exception as exc:
            raise HTTPException(503, str(exc))

    @app.post('/api/experiments/plan')
    async def plan(request: PlanRequest):
        try:
            caps = await controller.capabilities()
            return await planner.plan(request, caps, planning_context(store))
        except Exception as exc:
            raise HTTPException(422, str(exc))

    @app.post('/api/experiments', response_model=Experiment)
    async def create(request: RunRequest):
        if settings.mode == 'real' and not settings.real_injections_enabled:
            raise HTTPException(403, 'Real injections require REAL_INJECTIONS_ENABLED=true after sandbox owner authorization')
        try:
            return await runner.start(request)
        except RuntimeError as exc:
            raise HTTPException(409, str(exc))

    @app.get('/api/experiments', response_model=list[Experiment])
    async def list_experiments():
        return store.list()

    @app.get('/api/experiments/{experiment_id}', response_model=Experiment)
    async def get_experiment(experiment_id: str):
        exp = store.get(experiment_id)
        if not exp:
            raise HTTPException(404, 'Experiment not found')
        return exp

    @app.get('/api/experiments/{experiment_id}/metrics')
    async def metrics(experiment_id: str):
        exp = store.get(experiment_id)
        if not exp:
            raise HTTPException(404, 'Experiment not found')
        return {'experiment_id': experiment_id, 'probes': exp.probes, 'meter_samples': exp.meter_samples, 'metrics': exp.report.metrics if exp.report else None}

    @app.get('/api/experiments/{experiment_id}/report')
    async def report(experiment_id: str):
        exp = store.get(experiment_id)
        if not exp:
            raise HTTPException(404, 'Experiment not found')
        if exp.report is None:
            raise HTTPException(409, 'Report is not yet available')
        return exp.report

    @app.get('/api/experiments/{experiment_id}/observer-report')
    async def shared_observer_report(experiment_id: str):
        exp = store.get(experiment_id)
        if not exp:
            raise HTTPException(404, 'Experiment not found')
        if not exp.plan:
            raise HTTPException(409, 'Experiment plan is not yet available')
        return to_shared_report(exp)

    @app.get('/api/experiments/{experiment_id}/diagnosis')
    async def diagnosis(experiment_id: str):
        exp = store.get(experiment_id)
        if not exp:
            raise HTTPException(404, 'Experiment not found')
        if exp.diagnosis is None:
            raise HTTPException(409, 'Diagnosis is not available (mock mode, or experiment not finished)')
        return {'observation': exp.observation, 'diagnosis': exp.diagnosis}

    @app.post('/api/experiments/{experiment_id}/abort', response_model=Experiment)
    async def abort(experiment_id: str):
        try:
            return await runner.abort(experiment_id)
        except KeyError:
            raise HTTPException(404, 'Experiment not found')

    return app


app = create_app()
