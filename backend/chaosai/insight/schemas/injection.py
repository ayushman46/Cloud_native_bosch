"""Shared injection contract (Planner/Injector -> Observer).

Field names are fixed by the team contract. The Injector team owns this file;
it is created here only because the repository was empty. Replace it with the
Injector's version if theirs differs, and keep these names.
"""
from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class FailureType(str, Enum):
    APPLICATION_CRASH = "application_crash"
    NETWORK_LATENCY = "network_latency"
    SERVICE_RESTART = "service_restart"
    TRAFFIC_SPIKE = "traffic_spike"


class InjectionTimestamps(BaseModel):
    model_config = ConfigDict(extra="forbid")

    planned_at: AwareDatetime | None = None
    dispatched_at: AwareDatetime | None = None
    fault_activated_at: AwareDatetime | None = None
    rollback_started_at: AwareDatetime | None = None
    rollback_completed_at: AwareDatetime | None = None


class InjectionEvent(BaseModel):
    """Lifecycle event emitted by the Injector. May be sent several times per experiment."""

    model_config = ConfigDict(extra="forbid", use_enum_values=False)

    experiment_id: str = Field(min_length=1)
    application: str = "openmeter"
    target_service: str
    failure_type: FailureType
    status: str  # free-form: the contract only shows "active"; Observer relies on timestamps
    injection_success: bool | None = None
    timestamps: InjectionTimestamps = Field(default_factory=InjectionTimestamps)
    parameters: dict[str, Any] = Field(default_factory=dict)
