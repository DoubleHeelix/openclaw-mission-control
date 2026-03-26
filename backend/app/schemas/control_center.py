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
    "paperclip",
    "custom",
]
NetworkMarketingViewMode = Literal["pipeline", "team_tree", "cold_contact"]
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


class PodcastActionExtractionResponse(SQLModel):
    """Result payload for extracted actions promoted to tasks."""

    record_id: UUID
    created_task_ids: list[UUID] = Field(default_factory=list)
    extracted_actions_count: int = 0
    skipped_duplicates: int = 0
    action_hashes: list[str] = Field(default_factory=list)


class PodcastPipelineRunResponse(SQLModel):
    """Result payload for running/continuing podcast processing pipeline."""

    record_id: UUID
    pipeline_status: Literal["pending", "processing", "completed", "failed"]
    completed_stages: list[str] = Field(default_factory=list)
    retries: dict[str, int] = Field(default_factory=dict)
    failed_stage: str | None = None
    max_retries: int = 1


class PodcastRecordViewResponse(SQLModel):
    """Resolved podcast artifact payload for the detail drawer."""

    record_id: UUID
    title: str | None = None
    summary: str | None = None
    category: str | None = None
    transcript_path: str | None = None
    summary_path: str | None = None
    transcript_text: str | None = None
    transcript_words: list[dict[str, object]] = Field(default_factory=list)
    transcript_vtt_text: str | None = None
    summary_text: str | None = None
    action_points: list[str] = Field(default_factory=list)
    key_points: list[str] = Field(default_factory=list)
    decisions: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)


class EventScanRequest(SQLModel):
    """Payload for scanning event source URLs for a single week."""

    module_id: str
    module_slug: str
    module_title: str
    sources: list[str] = Field(default_factory=list)
    week_start: str | None = None


class EventScanItem(SQLModel):
    """Normalized event item returned after scanning sources."""

    title: str
    event_url: str
    source_url: str
    source_name: str
    summary: str | None = None
    start_at: str | None = None
    end_at: str | None = None
    venue: str | None = None
    address: str | None = None
    city: str | None = None
    country: str | None = None
    organizer: str | None = None
    group_name: str | None = None
    price: str | None = None
    currency: str | None = None
    is_free: bool = False
    image_url: str | None = None
    event_type: str | None = None
    status: str | None = None
    cancelled: bool = False
    online_or_hybrid: str | None = None
    attendee_count: int | None = None
    review_count: int | None = None
    ticket_url: str | None = None
    timezone: str | None = None


class EventScanResponse(SQLModel):
    """Summary payload for a weekly event-source scan."""

    imported: int = 0
    created: int = 0
    skipped: int = 0
    week_start: str | None = None
    week_end: str | None = None
    imported_count: int = 0
    skipped_duplicates: int = 0
    message: str | None = None
    events: list[EventScanItem] = Field(default_factory=list)
    diagnostics: list["EventScanSourceDiagnostic"] = Field(default_factory=list)
    skipped_reasons: dict[str, int] = Field(default_factory=dict)


class EventScanSourceDiagnostic(SQLModel):
    """Per-source scan diagnostics so the UI can explain weak sources."""

    source_url: str
    source_name: str
    scanned_candidates: int = 0
    imported: int = 0
    skipped: int = 0
    failure_reasons: dict[str, int] = Field(default_factory=dict)


class EventGeocodeResponse(SQLModel):
    """Simple geocode lookup response for event distance sorting."""

    ok: bool
    lat: float | None = None
    lon: float | None = None
    display_name: str | None = None
