"""Persisted configuration and records for the custom control center."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import JSON, Column, UniqueConstraint
from sqlmodel import Field

from app.core.time import utcnow
from app.models.tenancy import TenantScoped

RUNTIME_ANNOTATION_TYPES = (datetime, UUID)


class ControlCenterConfig(TenantScoped, table=True):
    """Per-organization control-center navigation/module design config."""

    __tablename__ = "control_center_configs"  # pyright: ignore[reportAssignmentType]
    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            name="uq_control_center_configs_organization_id",
        ),
    )

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    organization_id: UUID = Field(foreign_key="organizations.id", index=True)
    version: int = Field(default=1)
    modules: list[dict[str, object]] = Field(default_factory=list, sa_column=Column(JSON))
    network_marketing_view_mode: str = Field(default="pipeline")
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class ControlCenterRecord(TenantScoped, table=True):
    """Row-level record entries shown inside control-center module workspaces."""

    __tablename__ = "control_center_records"  # pyright: ignore[reportAssignmentType]

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    organization_id: UUID = Field(foreign_key="organizations.id", index=True)
    module_id: str = Field(index=True)
    module_slug: str = Field(index=True)
    module_category: str = Field(index=True)
    title: str
    summary: str | None = None
    stage: str | None = None
    data: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    linked_task_id: UUID | None = Field(default=None, foreign_key="tasks.id", index=True)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
