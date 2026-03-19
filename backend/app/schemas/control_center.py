"""Schemas for custom mission control config and record persistence."""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import model_validator
from sqlmodel import Field, SQLModel

ControlModuleCategory = Literal[
    "finance",
    "network_marketing",
    "newsletters",
    "podcasts",
    "custom",
]
NetworkMarketingViewMode = Literal["pipeline", "team_tree"]
PodcastJobStatus = Literal["uploaded", "pending", "processing", "completed", "failed"]


class ControlModule(SQLModel):
    """Navigation module definition stored in control-center config."""

    id: str
    slug: str
    title: str
    description: str
    category: ControlModuleCategory
    enabled: bool = True
    order: int = 1


class ControlCenterConfigRead(SQLModel):
    """Persisted module config for an organization."""

    version: int = 1
    modules: list[ControlModule] = Field(default_factory=list)
    network_marketing_view_mode: NetworkMarketingViewMode = "pipeline"
    updated_at: datetime | None = None


class ControlCenterConfigUpdate(SQLModel):
    """Payload for replacing control-center module configuration."""

    version: int = 1
    modules: list[ControlModule] = Field(default_factory=list)
    network_marketing_view_mode: NetworkMarketingViewMode = "pipeline"


class PodcastRecordData(SQLModel):
    """Structured payload for podcast/audio workflow records."""

    source_filename: str
    source_format: str
    ingest_status: PodcastJobStatus
    source_path: str | None = None
    size_bytes: int | None = None
    content_type: str | None = None
    transcript_status: PodcastJobStatus | None = None
    summary_status: PodcastJobStatus | None = None
    task_extraction_status: PodcastJobStatus | None = None
    category: str | None = None
    transcript_path: str | None = None
    summary_path: str | None = None
    extracted_actions_count: int | None = None


class ControlCenterRecordBase(SQLModel):
    """Shared record fields for create/update/read payloads."""

    module_id: str
    module_slug: str
    module_category: ControlModuleCategory
    title: str
    summary: str | None = None
    stage: str | None = None
    data: dict[str, object] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_podcast_data(self) -> "ControlCenterRecordBase":
        """Require full audio workflow data for podcast records."""
        if self.module_category != "podcasts":
            return self
        PodcastRecordData.model_validate(self.data)
        return self


class ControlCenterRecordCreate(ControlCenterRecordBase):
    """Payload for creating a module record."""


class ControlCenterRecordUpdate(SQLModel):
    """Payload for updating a module record."""

    title: str | None = None
    summary: str | None = None
    stage: str | None = None
    data: dict[str, object] | None = None

    @model_validator(mode="after")
    def require_any_field(self) -> "ControlCenterRecordUpdate":
        """Reject empty patch payloads."""
        if not self.model_fields_set:
            raise ValueError("At least one field is required")
        return self


class ControlCenterRecordRead(ControlCenterRecordBase):
    """Record payload returned by API."""

    id: UUID
    organization_id: UUID
    linked_task_id: UUID | None = None
    created_at: datetime
    updated_at: datetime


class PodcastIngestResponse(SQLModel):
    """Response payload for successful podcast audio ingest uploads."""

    record: ControlCenterRecordRead


class ControlCenterRecordListResponse(SQLModel):
    """List wrapper for module records."""

    items: list[ControlCenterRecordRead] = Field(default_factory=list)
    total: int = 0


class PromoteRecordToTaskRequest(SQLModel):
    """Create a task from an existing control-center record."""

    board_id: UUID
    priority: str = "medium"


class PromoteRecordToTaskResponse(SQLModel):
    """Result payload when a record is promoted into a task."""

    task_id: UUID
    board_id: UUID


class PodcastTranscriptionResponse(SQLModel):
    """Result payload for uploaded-audio transcription runs."""

    record_id: UUID
    transcript_status: Literal["uploaded", "processing", "completed", "failed"]
    audio_path: str
    transcript_path: str | None = None
    error: str | None = None


class PodcastSummaryResponse(SQLModel):
    """Result payload for transcript-to-summary runs."""

    record_id: UUID
    summary_status: Literal["processing", "completed", "failed"]
    summary_path: str | None = None
    error: str | None = None


class PodcastClassificationResponse(SQLModel):
    """Result payload for category classification and routing."""

    record_id: UUID
    category: Literal[
        "motivational",
        "teaching",
        "self-confidence-mindset",
        "general",
        "habits-productivity",
    ]
    audio_path: str | None = None
    transcript_path: str | None = None
    summary_path: str | None = None
