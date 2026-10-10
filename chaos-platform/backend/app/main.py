"""FastAPI entrypoint. Shared file: the Planner/Injector owners add their routers next to ours."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .api import analysis, monitoring
from .container import Container, build_container


def create_app(container: Container | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.container = container or build_container()
        yield
        await app.state.container.shutdown()

    app = FastAPI(title="Bosch ChaosAI backend", lifespan=lifespan)
    app.include_router(monitoring.router)
    app.include_router(analysis.router)

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    return app


app = create_app()
