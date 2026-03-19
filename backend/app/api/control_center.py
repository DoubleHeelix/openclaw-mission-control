"""API endpoints for the custom control center experience."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from sqlmodel import col, select

from app.api.deps import require_org_member
from app.core.time import utcnow
from app.db import crud
from app.db.session import get_session
from app.models.boards import Board
from app.models.control_center import ControlCenterConfig, ControlCenterRecord
from app.models.tasks import Task
from app.schemas.control_center import (
    ControlCenterConfigRead,
    ControlCenterConfigUpdate,
    ControlCenterRecordCreate,
    ControlCenterRecordListResponse,
    ControlCenterRecordRead,
    ControlCenterRecordUpdate,
    PodcastActionExtractionResponse,
    PodcastClassificationResponse,
    PodcastIngestResponse,
    PodcastPipelineRunResponse,
    PodcastSummaryResponse,
    PodcastTranscriptionResponse,
    PromoteRecordToTaskRequest,
    PromoteRecordToTaskResponse,
)
from app.services.network_marketing_storage import ensure_member_folder
from app.services.organizations import OrganizationContext
from app.services.podcast_actions import extract_actions, filter_new_actions
from app.services.podcast_classification import classify_podcast_category, load_text_if_exists
from app.services.podcast_ingest import store_podcast_audio_upload
from app.services.podcast_pipeline import increment_retry, merge_pipeline_state
from app.services.podcast_storage import route_completed_artifacts
from app.services.podcast_summary import process_summary_for_record
from app.services.podcast_transcription import (
    process_transcription_for_record,
    save_uploaded_audio,
)


def _normalize_network_marketing_view_mode(value: object) -> str:
    if value in {"pipeline", "team_tree"}:
        return str(value)
    return "pipeline"

router = APIRouter(prefix="/control-center", tags=["control-center"])
SESSION_DEP = Depends(get_session)
ORG_MEMBER_DEP = Depends(require_org_member)


def _default_modules() -> list[dict[str, object]]:
    return [
        {
            "id": "finance",
            "slug": "budget",
            "title": "Budget",
            "description": "Upload bank statements, categorize expenses, and track budget history.",
            "category": "finance",
            "enabled": True,
            "order": 1,
        },
        {
            "id": "network_marketing",
            "slug": "network-marketing",
            "title": "Network Marketing",
            "description": "Conversations, clients, and team operations.",
            "category": "network_marketing",
            "enabled": True,
            "order": 2,
        },
        {
            "id": "newsletters",
            "slug": "newsletters",
            "title": "Newsletters",
            "description": "News digests, key updates, and follow-up actions.",
            "category": "newsletters",
            "enabled": True,
            "order": 3,
        },
        {
            "id": "podcasts",
            "slug": "podcasts",
            "title": "Podcasts",
            "description": "Audio ingest, transcription, summarization, and action extraction jobs.",
            "category": "podcasts",
            "enabled": True,
            "order": 4,
        },
        {
            "id": "events",
            "slug": "events",
            "title": "Events",
            "description": "Track, review, and plan upcoming events.",
            "category": "custom",
            "enabled": True,
            "order": 5,
        },
    ]


def _normalize_modules(modules: list[dict[str, object]]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    existing_ids: set[str] = set()
    for module in modules:
        entry = dict(module)
        if str(entry.get("id", "")).strip() == "finance":
            entry["slug"] = "budget"
            entry["title"] = "Budget"
            entry["description"] = "Upload bank statements, categorize expenses, and track budget history."
        if str(entry.get("id", "")).strip() == "events":
            entry["slug"] = "events"
            entry["title"] = "Events"
            entry["description"] = "Track, review, and plan upcoming events."
            entry["category"] = "custom"
        existing_ids.add(str(entry.get("id", "")).strip())
        normalized.append(entry)
    next_order = max(
        [
            int(entry.get("order", 0))
            for entry in normalized
            if isinstance(entry.get("order"), int)
        ],
        default=0,
    )
    for module in _default_modules():
        if module["id"] in existing_ids:
            continue
        next_order += 1
        fallback = dict(module)
        fallback["order"] = next_order
        normalized.append(fallback)
    return normalized


async def _get_or_create_config(
    *,
    session,
    organization_id: UUID,
) -> ControlCenterConfig:
    existing = await ControlCenterConfig.objects.filter_by(organization_id=organization_id).first(
        session,
    )
    if existing is not None:
        return existing
    return await crud.create(
        session,
        ControlCenterConfig,
        organization_id=organization_id,
        version=1,
        modules=_default_modules(),
        network_marketing_view_mode="pipeline",
    )


async def _get_org_record_or_404(
    *,
    session,
    record_id: UUID,
    ctx: OrganizationContext,
) -> ControlCenterRecord:
    record = await ControlCenterRecord.objects.by_id(record_id).first(session)
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    if record.organization_id != ctx.organization.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
    return record


@router.get("", response_model=ControlCenterConfigRead)
async def get_control_center_config(
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> ControlCenterConfigRead:
    """Return persisted module configuration for the active organization."""
    config = await _get_or_create_config(
        session=session,
        organization_id=ctx.organization.id,
    )
    normalized_modules = _normalize_modules(config.modules)
    if normalized_modules != config.modules:
        config = await crud.patch(
            session,
            config,
            {
                "modules": normalized_modules,
                "updated_at": utcnow(),
            },
        )
    return ControlCenterConfigRead(
        version=config.version,
        modules=normalized_modules,
        network_marketing_view_mode=_normalize_network_marketing_view_mode(
            getattr(config, "network_marketing_view_mode", None),
        ),
        updated_at=config.updated_at,
    )


@router.put("", response_model=ControlCenterConfigRead)
async def update_control_center_config(
    payload: ControlCenterConfigUpdate,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> ControlCenterConfigRead:
    """Replace module configuration for the active organization."""
    config = await _get_or_create_config(
        session=session,
        organization_id=ctx.organization.id,
    )
    normalized_modules = _normalize_modules([module.model_dump() for module in payload.modules])
    updated = await crud.patch(
        session,
        config,
        {
            "version": payload.version,
            "modules": normalized_modules,
            "network_marketing_view_mode": payload.network_marketing_view_mode,
            "updated_at": utcnow(),
        },
    )
    return ControlCenterConfigRead(
        version=updated.version,
        modules=normalized_modules,
        network_marketing_view_mode=_normalize_network_marketing_view_mode(
            getattr(updated, "network_marketing_view_mode", None),
        ),
        updated_at=updated.updated_at,
    )


@router.get("/records", response_model=ControlCenterRecordListResponse)
async def list_control_center_records(
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
    module_id: str | None = Query(default=None),
    module_slug: str | None = Query(default=None),
) -> ControlCenterRecordListResponse:
    """List records optionally filtered by module id or slug."""
    statement = (
        select(ControlCenterRecord)
        .where(col(ControlCenterRecord.organization_id) == ctx.organization.id)
        .order_by(col(ControlCenterRecord.updated_at).desc())
    )
    if module_id:
        statement = statement.where(col(ControlCenterRecord.module_id) == module_id.strip())
    if module_slug:
        statement = statement.where(col(ControlCenterRecord.module_slug) == module_slug.strip())

    items = list(await session.exec(statement))
    return ControlCenterRecordListResponse(
        items=[ControlCenterRecordRead.model_validate(item, from_attributes=True) for item in items],
        total=len(items),
    )


@router.post("/records", response_model=ControlCenterRecordRead)
async def create_control_center_record(
    payload: ControlCenterRecordCreate,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> ControlCenterRecordRead:
    """Create a record entry for a module workspace."""
    created = await crud.create(
        session,
        ControlCenterRecord,
        organization_id=ctx.organization.id,
        module_id=payload.module_id,
        module_slug=payload.module_slug,
        module_category=payload.module_category,
        title=payload.title,
        summary=payload.summary,
        stage=payload.stage,
        data=payload.data,
    )

    if (
        payload.module_category == "network_marketing"
        and str((payload.data or {}).get("kind", "")) == "team_member"
    ):
        member_folder = ensure_member_folder(
            organization_id=ctx.organization.id,
            member_id=created.id,
            display_name=payload.title,
        )
        next_data = dict(created.data or {})
        next_data.update(
            {
                "member_folder_path": str(member_folder.member_dir),
                "member_folder_key": member_folder.folder_key,
                "member_files_dir": str(member_folder.files_dir),
                "member_notes_dir": str(member_folder.notes_dir),
                "member_history_dir": str(member_folder.history_dir),
            },
        )
        created = await crud.patch(
            session,
            created,
            {
                "data": next_data,
                "updated_at": utcnow(),
            },
        )

    return ControlCenterRecordRead.model_validate(created, from_attributes=True)


@router.patch("/records/{record_id}", response_model=ControlCenterRecordRead)
async def update_control_center_record(
    record_id: UUID,
    payload: ControlCenterRecordUpdate,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> ControlCenterRecordRead:
    """Update a module record."""
    record = await _get_org_record_or_404(
        session=session,
        record_id=record_id,
        ctx=ctx,
    )
    updates = payload.model_dump(exclude_unset=True)
    if "data" in updates and isinstance(updates["data"], dict):
        merged_data = dict(record.data or {})
        merged_data.update(updates["data"])
        updates["data"] = merged_data
    updates["updated_at"] = utcnow()
    updated = await crud.patch(session, record, updates)
    return ControlCenterRecordRead.model_validate(updated, from_attributes=True)


@router.post("/podcasts/ingest", response_model=PodcastIngestResponse)
async def ingest_podcast_audio(
    file: UploadFile = File(...),
    title: str | None = Form(default=None),
    summary: str | None = Form(default=None),
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> PodcastIngestResponse:
    """Upload MP3/M4A audio and create an uploaded podcast control-center record."""
    stored = await store_podcast_audio_upload(file)

    record = await crud.create(
        session,
        ControlCenterRecord,
        organization_id=ctx.organization.id,
        module_id="podcasts",
        module_slug="podcasts",
        module_category="podcasts",
        title=(title.strip() if isinstance(title, str) and title.strip() else stored.original_name),
        summary=(summary.strip() if isinstance(summary, str) and summary.strip() else "Podcast audio uploaded"),
        stage="uploaded",
        data={
            "source_filename": stored.original_name,
            "source_format": stored.extension.lstrip("."),
            "source_path": stored.source_path,
            "size_bytes": stored.size_bytes,
            "content_type": stored.content_type,
            "ingest_status": "uploaded",
            "transcript_status": "pending",
            "summary_status": "pending",
            "task_extraction_status": "pending",
        },
    )

    return PodcastIngestResponse(
        record=ControlCenterRecordRead.model_validate(record, from_attributes=True),
    )


@router.post("/records/{record_id}/transcribe", response_model=PodcastTranscriptionResponse)
async def transcribe_control_center_record_audio(
    record_id: UUID,
    file: UploadFile = File(...),
    note: str | None = Form(default=None),
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> PodcastTranscriptionResponse:
    """Upload an audio file and persist transcript artifacts for a record."""
    record = await _get_org_record_or_404(
        session=session,
        record_id=record_id,
        ctx=ctx,
    )

    audio_bytes = await file.read()
    if not audio_bytes:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Audio file is empty.")

    audio_path = await save_uploaded_audio(record_id=record.id, filename=file.filename or "audio.bin", audio_bytes=audio_bytes)

    uploaded_data = dict(record.data or {})
    uploaded_data.update(
        {
            "audio_path": str(audio_path),
            "transcript_status": "uploaded",
            "transcript_error": None,
        },
    )
    if note:
        uploaded_data["notes"] = note

    await crud.patch(
        session,
        record,
        {
            "data": uploaded_data,
            "updated_at": utcnow(),
        },
    )

    result = await process_transcription_for_record(
        session=session,
        record_id=record.id,
        audio_path=str(audio_path),
    )

    return PodcastTranscriptionResponse(
        record_id=record.id,
        transcript_status=str(result.get("transcript_status", "failed")),
        audio_path=str(result.get("audio_path", str(audio_path))),
        transcript_path=(
            str(result["transcript_path"])
            if isinstance(result.get("transcript_path"), str)
            else None
        ),
        error=str(result.get("error")) if result.get("error") is not None else None,
    )


@router.post("/records/{record_id}/summarize", response_model=PodcastSummaryResponse)
async def summarize_control_center_record_audio(
    record_id: UUID,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> PodcastSummaryResponse:
    """Generate and persist a summary artifact from record transcript text."""
    record = await _get_org_record_or_404(
        session=session,
        record_id=record_id,
        ctx=ctx,
    )

    data = dict(record.data or {})
    transcript_status = str(data.get("transcript_status") or "")
    if transcript_status != "completed":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Transcription must be completed before summary generation.",
        )

    result = await process_summary_for_record(session=session, record_id=record.id)
    return PodcastSummaryResponse(
        record_id=record.id,
        summary_status=str(result.get("summary_status", "failed")),
        summary_path=(
            str(result["summary_path"])
            if isinstance(result.get("summary_path"), str)
            else None
        ),
        error=str(result.get("error")) if result.get("error") is not None else None,
    )


@router.post("/records/{record_id}/extract-actions", response_model=PodcastActionExtractionResponse)
async def extract_control_center_record_actions(
    record_id: UUID,
    tasks_board_id: UUID = Query(default=UUID("5fc5d021-13fa-4258-bd0e-8c4d22e151b0")),
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> PodcastActionExtractionResponse:
    """Extract actions and create deduplicated Tasks-board tasks for a podcast record."""
    record = await _get_org_record_or_404(session=session, record_id=record_id, ctx=ctx)
    board = await Board.objects.by_id(tasks_board_id).first(session)
    if board is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tasks board not found.")
    if board.organization_id != ctx.organization.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)

    data = dict(record.data or {})
    transcript_text = load_text_if_exists(str(data.get("transcript_path")))
    summary_text = load_text_if_exists(str(data.get("summary_path")))
    if not transcript_text and not summary_text:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Transcript or summary must exist before action extraction.")

    extracted = extract_actions(transcript_text, summary_text)
    existing_hashes = set(str(x) for x in data.get("action_hashes", []) if isinstance(x, str))
    new_actions, skipped = filter_new_actions(extracted, existing_hashes)

    created_task_ids: list[UUID] = []
    for action in new_actions:
        task = await crud.create(
            session,
            Task,
            board_id=board.id,
            title=action.text[:120],
            description=f"Source podcast record: {record.id}\n\nAction hash: {action.action_hash}\n\n{action.text}",
            priority="medium",
            auto_created=True,
            auto_reason="podcast_action_extraction",
        )
        created_task_ids.append(task.id)

    prior_task_ids = [UUID(str(x)) for x in data.get("created_task_ids", []) if isinstance(x, str)]
    all_task_ids = [*prior_task_ids, *created_task_ids]
    data.update(
        {
            "task_extraction_status": "completed",
            "extracted_actions_count": len(extracted),
            "created_task_ids": [str(x) for x in all_task_ids],
            "action_hashes": sorted(existing_hashes),
        },
    )
    await crud.patch(session, record, {"data": data, "updated_at": utcnow()})

    return PodcastActionExtractionResponse(
        record_id=record.id,
        created_task_ids=created_task_ids,
        extracted_actions_count=len(extracted),
        skipped_duplicates=len(skipped),
        action_hashes=sorted(existing_hashes),
    )


@router.post("/records/{record_id}/classify", response_model=PodcastClassificationResponse)
async def classify_control_center_record_audio(
    record_id: UUID,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> PodcastClassificationResponse:
    """Classify a completed podcast record and route artifacts to category folder."""
    record = await _get_org_record_or_404(
        session=session,
        record_id=record_id,
        ctx=ctx,
    )

    data = dict(record.data or {})
    transcript_status = str(data.get("transcript_status") or "")
    if transcript_status != "completed":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Transcription must be completed before classification.",
        )

    audio_path = (
        str(data.get("audio_path"))
        if isinstance(data.get("audio_path"), str)
        else str(data.get("source_path")) if isinstance(data.get("source_path"), str) else None
    )
    transcript_path = str(data.get("transcript_path")) if isinstance(data.get("transcript_path"), str) else None
    summary_path = str(data.get("summary_path")) if isinstance(data.get("summary_path"), str) else None

    transcript_text = load_text_if_exists(transcript_path)
    summary_text = load_text_if_exists(summary_path)
    category = classify_podcast_category(
        title=record.title,
        summary=(record.summary or summary_text),
        transcript_text=transcript_text,
    )

    routed = route_completed_artifacts(
        category=category,
        audio_path=audio_path,
        transcript_path=transcript_path,
        summary_path=summary_path,
    )

    data.update(
        {
            "category": routed.category,
            "audio_path": routed.audio_path,
            "transcript_path": routed.transcript_path,
            "summary_path": routed.summary_path,
            "completed_audio_path": routed.audio_path,
            "completed_transcript_path": routed.transcript_path,
            "completed_summary_path": routed.summary_path,
        },
    )

    updated_stage = "completed"
    await crud.patch(
        session,
        record,
        {
            "stage": updated_stage,
            "data": data,
            "updated_at": utcnow(),
        },
    )

    return PodcastClassificationResponse(
        record_id=record.id,
        category=routed.category,
        audio_path=routed.audio_path,
        transcript_path=routed.transcript_path,
        summary_path=routed.summary_path,
    )


@router.post("/records/{record_id}/pipeline/run", response_model=PodcastPipelineRunResponse)
async def run_control_center_record_pipeline(
    record_id: UUID,
    fail_stage: str | None = Query(default=None),
    max_retries: int = Query(default=1, ge=0, le=10),
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> PodcastPipelineRunResponse:
    """Run/continue end-to-end pipeline with bounded retries + idempotent stage state."""
    record = await _get_org_record_or_404(session=session, record_id=record_id, ctx=ctx)

    data = merge_pipeline_state(dict(record.data or {}))
    pipeline = data["pipeline"]
    completed_stages = list(pipeline.get("completed_stages") or [])
    retries = dict(pipeline.get("retries") or {})

    if fail_stage is not None and fail_stage not in {"ingest", "transcribe", "summarize", "actions", "categorize"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid fail_stage value.")

    failed_stage: str | None = None
    stage_order = ["ingest", "transcribe", "summarize", "actions", "categorize"]
    for stage in stage_order:
        retries.setdefault(stage, 0)
        if stage in completed_stages:
            continue
        if fail_stage == stage:
            retry_count = increment_retry(retries, stage)
            pipeline["max_retries"] = max_retries
            pipeline["retries"] = retries
            pipeline["completed_stages"] = completed_stages
            if retry_count > max_retries:
                pipeline["status"] = "failed"
                pipeline["last_error"] = f"Retry cap exceeded for stage '{stage}' ({retry_count}>{max_retries})"
            else:
                pipeline["status"] = "processing"
                pipeline["last_error"] = f"Transient failure in stage '{stage}'"
            failed_stage = stage
            break
        completed_stages.append(stage)

    if failed_stage is None:
        pipeline["status"] = "completed"
        pipeline["last_error"] = None

    pipeline["completed_stages"] = completed_stages
    pipeline["retries"] = retries
    pipeline["max_retries"] = max_retries

    updated = await crud.patch(
        session,
        record,
        {
            "stage": "completed" if failed_stage is None else (record.stage or "processing"),
            "data": data,
            "updated_at": utcnow(),
        },
    )

    return PodcastPipelineRunResponse(
        record_id=updated.id,
        pipeline_status=str(pipeline.get("status") or "processing"),
        completed_stages=completed_stages,
        retries={k: int(v) for k, v in retries.items()},
        failed_stage=failed_stage,
        max_retries=max_retries,
    )


@router.delete("/records/{record_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_control_center_record(
    record_id: UUID,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> None:
    """Delete a module record."""
    record = await _get_org_record_or_404(
        session=session,
        record_id=record_id,
        ctx=ctx,
    )
    await crud.delete(session, record)


@router.post("/records/{record_id}/promote", response_model=PromoteRecordToTaskResponse)
async def promote_record_to_task(
    record_id: UUID,
    payload: PromoteRecordToTaskRequest,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> PromoteRecordToTaskResponse:
    """Create a task from a control-center record and link it back."""
    record = await _get_org_record_or_404(
        session=session,
        record_id=record_id,
        ctx=ctx,
    )
    board = await Board.objects.by_id(payload.board_id).first(session)
    if board is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found.")
    if board.organization_id != ctx.organization.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)

    task = await crud.create(
        session,
        Task,
        board_id=board.id,
        title=record.title,
        description=record.summary,
        priority=payload.priority,
        status="inbox",
        created_by_user_id=ctx.member.user_id,
    )
    await crud.patch(
        session,
        record,
        {
            "linked_task_id": task.id,
            "updated_at": utcnow(),
        },
    )
    return PromoteRecordToTaskResponse(task_id=task.id, board_id=board.id)
