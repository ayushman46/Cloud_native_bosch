"""Wires Observer + Analyzer + storage. Teammates can pass their own repository / LLM backend."""
from __future__ import annotations

from dataclasses import dataclass

from .agents.analyzer import AnalyzerAgent
from .agents.observer import ObserverAgent
from .clients.nvidia_client import LLMBackend, NvidiaChatClient
from .config import Settings
from .storage.repository import InMemoryRepository


@dataclass
class Container:
    settings: Settings
    repo: InMemoryRepository
    observer: ObserverAgent
    analyzer: AnalyzerAgent

    async def shutdown(self) -> None:
        await self.observer.shutdown()
        aclose = getattr(self.analyzer.llm, "aclose", None)
        if aclose:
            await aclose()


def build_container(
    settings: Settings | None = None,
    repo: InMemoryRepository | None = None,
    llm: LLMBackend | None = None,
    observer: ObserverAgent | None = None,
) -> Container:
    settings = settings or Settings()
    repo = repo or InMemoryRepository()
    if llm is None and settings.nvidia_api_key:
        llm = NvidiaChatClient(
            settings.nvidia_api_key, settings.nvidia_model, settings.nvidia_base_url,
            timeout_seconds=settings.nvidia_timeout_seconds, max_retries=settings.nvidia_max_retries,
            json_mode=settings.nvidia_json_mode,
        )
    return Container(settings, repo, observer or ObserverAgent(settings, repo), AnalyzerAgent(llm))
