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
from .orchestration import Runner
from .observer_adapter import to_shared_report


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
    runner = Runner(settings, store, controller, observer, planner, analyzer)

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
        await controller.close()
        await observer.close()
        await llm.close()

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
        return {'mode': settings.mode, 'targets': settings.targets, 'nvidia_configured': bool(settings.nvidia_api_key and settings.nvidia_model), 'controller_configured': bool(settings.chaos_control_url and settings.chaos_control_api_key), 'openmeter_configured': bool(settings.openmeter_base_url), 'synthetic_events_enabled': settings.enable_synthetic_events, 'real_injections_enabled': settings.real_injections_enabled, 'limits': {'duration_seconds': settings.max_duration_seconds, 'latency_ms': settings.max_latency_ms, 'requests_per_second': settings.max_rps}}

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
            return await planner.plan(request, caps)
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

    @app.post('/api/experiments/{experiment_id}/abort', response_model=Experiment)
    async def abort(experiment_id: str):
        try:
            return await runner.abort(experiment_id)
        except KeyError:
            raise HTTPException(404, 'Experiment not found')

    return app


app = create_app()
