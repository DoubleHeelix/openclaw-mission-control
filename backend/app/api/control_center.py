"""API endpoints for the custom control center experience."""

from __future__ import annotations

import mimetypes
import json
from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from fastapi.responses import FileResponse
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
    EventGeocodeResponse,
    EventScanItem,
    EventScanRequest,
    EventScanResponse,
    EventScanSourceDiagnostic,
    ControlCenterRecordCreate,
    ControlCenterRecordListResponse,
    ControlCenterRecordRead,
    ControlCenterRecordUpdate,
    PodcastActionExtractionResponse,
    PodcastClassificationResponse,
    PodcastIngestResponse,
    PodcastPipelineRunResponse,
    PodcastRecordViewResponse,
    PodcastSummaryResponse,
    PodcastTranscriptionResponse,
    PromoteRecordToTaskRequest,
    PromoteRecordToTaskResponse,
)
from app.services.event_scanning import (
    EventCandidate,
    UnsafeEventSourceError,
    geocode_query,
    record_key_from_payload,
    scan_event_sources,
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
    if value in {"pipeline", "team_tree", "cold_contact"}:
        return str(value)
    return "pipeline"


def _path_if_exists(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate:
        return None
    path = Path(candidate)
    if not path.exists() or not path.is_file():
        return None
    return str(path)


def _coerce_string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str) and item.strip()]


def _coerce_words_payload(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    words: list[dict[str, object]] = []
    for item in value:
        if isinstance(item, dict):
            words.append(item)
    return words


def _first_existing_path(*values: object) -> str | None:
    for value in values:
        existing = _path_if_exists(value)
        if existing:
            return existing
    return None


def _derive_podcast_stage(data: dict[str, object]) -> str:
    if (
        _first_existing_path(data.get("completed_audio_path"))
        and _first_existing_path(data.get("completed_transcript_path"))
        and _first_existing_path(data.get("completed_summary_path"))
    ):
        return "completed"
    if _first_existing_path(data.get("summary_path")):
        return "summarized"
    if _first_existing_path(data.get("transcript_path")):
        return "transcribed"
    if _first_existing_path(data.get("audio_path"), data.get("source_path")):
        return "uploaded"
    return "inbox"


def _normalize_podcast_record_state(record: ControlCenterRecord) -> tuple[dict[str, object], str, bool]:
    data = dict(record.data or {})
    changed = False

    audio_path = _first_existing_path(
        data.get("completed_audio_path"),
        data.get("audio_path"),
        data.get("source_path"),
    )
    transcript_path = _first_existing_path(
        data.get("completed_transcript_path"),
        data.get("transcript_path"),
    )
    summary_path = _first_existing_path(
        data.get("completed_summary_path"),
        data.get("summary_path"),
    )

    ingest_status = "completed" if audio_path else str(data.get("ingest_status") or "pending")
    transcript_status = str(data.get("transcript_status") or "pending")
    summary_status = str(data.get("summary_status") or "pending")
    actions_status = str(data.get("task_extraction_status") or "pending")

    if transcript_path:
        transcript_status = "completed"
    elif transcript_status == "completed":
        transcript_status = "failed"
        changed = True

    transcript_error = data.get("transcript_error")
    if transcript_status == "failed" and not (
        transcript_error is None or isinstance(transcript_error, str)
    ):
        transcript_error = None
    if transcript_status == "failed" and not (isinstance(transcript_error, str) and transcript_error.strip()):
        transcript_error = "Transcript artifact is missing. Re-run transcription."
        changed = True
    elif transcript_status != "failed" and transcript_error not in (None, ""):
        transcript_error = None
        changed = True

    if summary_path:
        summary_status = "completed"
    elif summary_status == "completed":
        summary_status = "pending"
        changed = True

    action_hashes = _coerce_string_list(data.get("action_hashes"))
    created_task_ids = _coerce_string_list(data.get("created_task_ids"))
    action_points = _coerce_string_list(data.get("action_points"))
    extracted_actions_count = data.get("extracted_actions_count")
    if not isinstance(extracted_actions_count, int):
        extracted_actions_count = len(action_points) if action_points else 0
    if action_hashes or created_task_ids or action_points or extracted_actions_count:
        actions_status = "completed"
    elif actions_status == "completed":
        actions_status = "pending"
        changed = True

    next_stage = _derive_podcast_stage(
        {
            **data,
            "audio_path": audio_path,
            "transcript_path": transcript_path,
            "summary_path": summary_path,
        },
    )

    normalized = {
        **data,
        "audio_path": audio_path,
        "transcript_path": transcript_path,
        "summary_path": summary_path,
        "ingest_status": ingest_status,
        "transcript_status": transcript_status,
        "summary_status": summary_status,
        "task_extraction_status": actions_status,
        "transcript_error": transcript_error,
        "action_hashes": action_hashes,
        "created_task_ids": created_task_ids,
        "action_points": action_points,
        "extracted_actions_count": extracted_actions_count,
    }
    if normalized != data:
        changed = True
    if (record.stage or "") != next_stage:
        changed = True
    return normalized, next_stage, changed


async def _repair_podcast_record_if_needed(*, session, record: ControlCenterRecord) -> ControlCenterRecord:
    if record.module_category != "podcasts":
        return record
    normalized_data, next_stage, changed = _normalize_podcast_record_state(record)
    if not changed:
        return record
    return await crud.patch(
        session,
        record,
        {
            "data": normalized_data,
            "stage": next_stage,
            "updated_at": utcnow(),
        },
    )


async def _extract_podcast_actions_for_record(
    *,
    session,
    record: ControlCenterRecord,
    board: Board,
) -> PodcastActionExtractionResponse:
    data = dict(record.data or {})
    transcript_text = load_text_if_exists(str(data.get("transcript_path")))
    summary_text = load_text_if_exists(str(data.get("summary_path")))
    if not transcript_text and not summary_text:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Transcript or summary must exist before action extraction.",
        )

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
    all_action_hashes = sorted({*existing_hashes, *(action.action_hash for action in extracted)})
    all_action_points = _coerce_string_list(data.get("action_points"))
    for action in extracted:
        if action.text not in all_action_points:
            all_action_points.append(action.text)

    patched = await crud.patch(
        session,
        record,
        {
            "data": {
                **data,
                "task_extraction_status": "completed",
                "extracted_actions_count": len(extracted),
                "created_task_ids": [str(x) for x in all_task_ids],
                "action_hashes": all_action_hashes,
                "action_points": all_action_points,
            },
            "updated_at": utcnow(),
        },
    )
    await _repair_podcast_record_if_needed(session=session, record=patched)

    return PodcastActionExtractionResponse(
        record_id=record.id,
        created_task_ids=created_task_ids,
        extracted_actions_count=len(extracted),
        skipped_duplicates=len(skipped),
        action_hashes=all_action_hashes,
    )


async def _classify_podcast_record(
    *,
    session,
    record: ControlCenterRecord,
) -> PodcastClassificationResponse:
    data = dict(record.data or {})
    transcript_path = _first_existing_path(data.get("transcript_path"), data.get("completed_transcript_path"))
    if not transcript_path:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Transcription must be completed before classification.",
        )

    audio_path = _first_existing_path(data.get("audio_path"), data.get("source_path"), data.get("completed_audio_path"))
    summary_path = _first_existing_path(data.get("summary_path"), data.get("completed_summary_path"))

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

    patched = await crud.patch(
        session,
        record,
        {
            "stage": "completed",
            "data": {
                **data,
                "category": routed.category,
                "audio_path": routed.audio_path,
                "transcript_path": routed.transcript_path,
                "summary_path": routed.summary_path,
                "completed_audio_path": routed.audio_path,
                "completed_transcript_path": routed.transcript_path,
                "completed_summary_path": routed.summary_path,
            },
            "updated_at": utcnow(),
        },
    )
    await _repair_podcast_record_if_needed(session=session, record=patched)

    return PodcastClassificationResponse(
        record_id=record.id,
        category=routed.category,
        audio_path=routed.audio_path,
        transcript_path=routed.transcript_path,
        summary_path=routed.summary_path,
    )


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
            entry["description"] = (
                "Upload bank statements, categorize expenses, and track budget history."
            )
        if str(entry.get("id", "")).strip() == "events":
            entry["slug"] = "events"
            entry["title"] = "Events"
            entry["description"] = "Track, review, and plan upcoming events."
            entry["category"] = "custom"
        existing_ids.add(str(entry.get("id", "")).strip())
        normalized.append(entry)
    next_order = max(
        [int(entry.get("order", 0)) for entry in normalized if isinstance(entry.get("order"), int)],
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
    repaired_items: list[ControlCenterRecord] = []
    for item in items:
        repaired_items.append(await _repair_podcast_record_if_needed(session=session, record=item))
    return ControlCenterRecordListResponse(
        items=[
            ControlCenterRecordRead.model_validate(item, from_attributes=True) for item in repaired_items
        ],
        total=len(repaired_items),
    )


def _event_candidate_to_scan_item(candidate: EventCandidate) -> EventScanItem:
    return EventScanItem(
        title=candidate.title,
        event_url=candidate.event_url,
        source_url=candidate.source_url,
        source_name=candidate.source_name,
        summary=candidate.summary,
        start_at=candidate.start_at,
        end_at=candidate.end_at,
        venue=candidate.venue,
        address=candidate.address,
        city=candidate.city,
        country=candidate.country,
        organizer=candidate.organizer,
        group_name=candidate.group_name,
        price=candidate.price,
        currency=candidate.currency,
        is_free=bool(candidate.is_free),
        image_url=candidate.image_url,
        event_type=candidate.event_type,
        status=candidate.status,
        cancelled=bool(candidate.cancelled),
        online_or_hybrid=candidate.online_or_hybrid,
        attendee_count=candidate.attendee_count,
        review_count=candidate.review_count,
        ticket_url=candidate.ticket_url,
        timezone=candidate.timezone,
    )


def _event_record_payload(
    *,
    candidate: EventCandidate,
    module_id: str,
    module_slug: str,
) -> dict[str, object]:
    summary_parts = [candidate.summary or ""]
    if candidate.event_url and candidate.event_url not in summary_parts[0]:
        summary_parts.append(f"Source: {candidate.event_url}")
    summary = "\n\n".join([part.strip() for part in summary_parts if part.strip()]) or None
    return {
        "module_id": module_id,
        "module_slug": module_slug,
        "module_category": "custom",
        "title": candidate.title,
        "summary": summary,
        "stage": "discovered",
        "data": {
            "kind": "event",
            "start_at": candidate.start_at,
            "end_at": candidate.end_at,
            "venue": candidate.venue,
            "address": candidate.address,
            "city": candidate.city,
            "country": candidate.country,
            "organizer": candidate.organizer,
            "group_name": candidate.group_name,
            "price": candidate.price,
            "currency": candidate.currency,
            "is_free": bool(candidate.is_free),
            "event_url": candidate.event_url,
            "source_url": candidate.source_url,
            "source_name": candidate.source_name,
            "image_url": candidate.image_url,
            "event_type": candidate.event_type,
            "status": candidate.status,
            "cancelled": bool(candidate.cancelled),
            "online_or_hybrid": candidate.online_or_hybrid,
            "attendee_count": candidate.attendee_count,
            "review_count": candidate.review_count,
            "ticket_url": candidate.ticket_url,
            "timezone": candidate.timezone,
        },
    }


def _merge_event_data(
    *,
    existing: dict[str, object] | None,
    incoming: dict[str, object],
) -> dict[str, object]:
    merged = dict(existing or {})
    for key, value in incoming.items():
        if value in (None, "", [], {}):
            continue
        merged[key] = value
    return merged


@router.post("/events/scan-week", response_model=EventScanResponse)
async def scan_events_week(
    payload: EventScanRequest,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> EventScanResponse:
    """Scan source URLs and persist newly discovered events for the selected week."""
    sources = [item.strip() for item in payload.sources if item.strip()]
    if not sources:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="At least one source URL is required."
        )

    try:
        scan = await scan_event_sources(sources=sources, week_start_value=payload.week_start)
    except UnsafeEventSourceError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    existing_statement = select(ControlCenterRecord).where(
        col(ControlCenterRecord.organization_id) == ctx.organization.id,
        col(ControlCenterRecord.module_id) == payload.module_id.strip(),
    )
    existing_records = list(await session.exec(existing_statement))
    existing_by_key = {
        record_key_from_payload(title=record.title, data=record.data or {}): record
        for record in existing_records
    }

    created_items: list[EventScanItem] = []
    duplicates = 0
    refreshed = 0
    for candidate in scan.events:
        candidate_payload = _event_record_payload(
            candidate=candidate,
            module_id=payload.module_id.strip(),
            module_slug=payload.module_slug.strip() or "events",
        )
        dedupe_key = record_key_from_payload(
            title=str(candidate_payload["title"]),
            data=candidate_payload["data"],  # type: ignore[arg-type]
        )
        existing_record = existing_by_key.get(dedupe_key)
        if existing_record is not None:
            next_data = _merge_event_data(
                existing=existing_record.data if isinstance(existing_record.data, dict) else {},
                incoming=candidate_payload["data"],  # type: ignore[arg-type]
            )
            patch_payload: dict[str, object] = {"data": next_data, "updated_at": utcnow()}
            incoming_summary = candidate_payload.get("summary")
            if (
                not existing_record.summary
                and isinstance(incoming_summary, str)
                and incoming_summary.strip()
            ):
                patch_payload["summary"] = incoming_summary
            if not existing_record.title and isinstance(candidate_payload.get("title"), str):
                patch_payload["title"] = candidate_payload["title"]
            await crud.patch(session, existing_record, patch_payload)
            duplicates += 1
            refreshed += 1
            continue
        created = await crud.create(
            session,
            ControlCenterRecord,
            organization_id=ctx.organization.id,
            **candidate_payload,
        )
        existing_by_key[dedupe_key] = created
        created_items.append(_event_candidate_to_scan_item(candidate))

    source_hosts = sorted(
        {
            urlparse(item.source_url).netloc.removeprefix("www.")
            for item in created_items
            if item.source_url
        },
    )
    message = (
        f"Imported {len(created_items)} new events from {len(source_hosts) or len(sources)} source"
        f"{'' if (len(source_hosts) or len(sources)) == 1 else 's'}"
        f"{f' and refreshed {refreshed} existing records' if refreshed else ''}."
    )
    skipped_total = scan.skipped + duplicates
    return EventScanResponse(
        imported=len(created_items),
        created=len(created_items),
        skipped=skipped_total,
        week_start=scan.week_start,
        week_end=scan.week_end,
        imported_count=len(created_items),
        skipped_duplicates=duplicates,
        message=message,
        events=created_items,
        diagnostics=[
            EventScanSourceDiagnostic(
                source_url=item.source_url,
                source_name=item.source_name,
                scanned_candidates=item.scanned_candidates,
                imported=item.imported,
                skipped=item.skipped,
                failure_reasons=item.failure_reasons,
            )
            for item in scan.diagnostics
        ],
        skipped_reasons={
            **scan.skipped_reasons,
            **(
                {"duplicates": scan.skipped_reasons.get("duplicates", 0) + duplicates}
                if duplicates
                else {}
            ),
        },
    )


@router.get("/events/geocode", response_model=EventGeocodeResponse)
async def geocode_events_location(
    query: str = Query(..., min_length=3),
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> EventGeocodeResponse:
    """Geocode a location string so the events UI can sort by nearest distance."""
    del ctx
    result = await geocode_query(query)
    if result is None:
        return EventGeocodeResponse(ok=False)
    return EventGeocodeResponse(
        ok=True,
        lat=result.lat,
        lon=result.lon,
        display_name=result.display_name,
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
        summary=(
            summary.strip()
            if isinstance(summary, str) and summary.strip()
            else "Podcast audio uploaded"
        ),
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

    audio_path = await save_uploaded_audio(
        record_id=record.id, filename=file.filename or "audio.bin", audio_bytes=audio_bytes
    )

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
            str(result["summary_path"]) if isinstance(result.get("summary_path"), str) else None
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

    record = await _repair_podcast_record_if_needed(session=session, record=record)
    return await _extract_podcast_actions_for_record(session=session, record=record, board=board)


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

    record = await _repair_podcast_record_if_needed(session=session, record=record)
    return await _classify_podcast_record(session=session, record=record)


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

    record = await _repair_podcast_record_if_needed(session=session, record=record)
    data = merge_pipeline_state(dict(record.data or {}))
    pipeline = data["pipeline"]
    retries = dict(pipeline.get("retries") or {})
    completed_stages: list[str] = []
    valid_stages = {"ingest", "transcribe", "summarize", "actions", "categorize"}

    if fail_stage is not None and fail_stage not in valid_stages:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid fail_stage value."
        )
    for stage in valid_stages:
        retries.setdefault(stage, 0)

    async def _mark_processing() -> None:
        pipeline["status"] = "processing"
        pipeline["retries"] = retries
        pipeline["max_retries"] = max_retries
        await crud.patch(session, record, {"data": data, "updated_at": utcnow()})

    failed_stage: str | None = None
    current_record = record
    data_state = dict(current_record.data or {})
    if _first_existing_path(data_state.get("audio_path"), data_state.get("source_path"), data_state.get("completed_audio_path")):
        completed_stages.append("ingest")

    stage_order = ["transcribe", "summarize", "actions", "categorize"]
    for stage in stage_order:
        data_state = dict(current_record.data or {})
        if stage == "transcribe" and _first_existing_path(data_state.get("transcript_path"), data_state.get("completed_transcript_path")):
            completed_stages.append(stage)
            continue
        if stage == "summarize" and _first_existing_path(data_state.get("summary_path"), data_state.get("completed_summary_path")):
            completed_stages.append(stage)
            continue
        if stage == "actions":
            existing_actions = _coerce_string_list(data_state.get("action_points"))
            existing_hashes = _coerce_string_list(data_state.get("action_hashes"))
            existing_tasks = _coerce_string_list(data_state.get("created_task_ids"))
            if existing_actions or existing_hashes or existing_tasks:
                completed_stages.append(stage)
                continue
        if stage == "categorize" and _first_existing_path(
            data_state.get("completed_audio_path"),
            data_state.get("completed_transcript_path"),
            data_state.get("completed_summary_path"),
        ):
            completed_stages.append(stage)
            continue

        if fail_stage == stage:
            retry_count = increment_retry(retries, stage)
            pipeline["status"] = "failed" if retry_count > max_retries else "processing"
            pipeline["last_error"] = (
                f"Retry cap exceeded for stage '{stage}' ({retry_count}>{max_retries})"
                if retry_count > max_retries
                else f"Transient failure in stage '{stage}'"
            )
            pipeline["completed_stages"] = completed_stages
            pipeline["retries"] = retries
            pipeline["max_retries"] = max_retries
            await crud.patch(session, current_record, {"data": data, "updated_at": utcnow()})
            failed_stage = stage
            break

        try:
            await _mark_processing()
            if stage == "transcribe":
                audio_path = _first_existing_path(
                    data_state.get("audio_path"),
                    data_state.get("source_path"),
                    data_state.get("completed_audio_path"),
                )
                if not audio_path:
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Audio file is required before transcription.")
                await process_transcription_for_record(
                    session=session,
                    record_id=current_record.id,
                    audio_path=audio_path,
                )
            elif stage == "summarize":
                await process_summary_for_record(session=session, record_id=current_record.id)
            elif stage == "actions":
                board = await Board.objects.by_id(UUID("5fc5d021-13fa-4258-bd0e-8c4d22e151b0")).first(session)
                if board is None or board.organization_id != ctx.organization.id:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tasks board not found.")
                await _extract_podcast_actions_for_record(session=session, record=current_record, board=board)
            elif stage == "categorize":
                await _classify_podcast_record(session=session, record=current_record)
            current_record = await _get_org_record_or_404(session=session, record_id=current_record.id, ctx=ctx)
            current_record = await _repair_podcast_record_if_needed(session=session, record=current_record)
            completed_stages.append(stage)
            pipeline["last_error"] = None
        except Exception as exc:
            retry_count = increment_retry(retries, stage)
            pipeline["status"] = "failed"
            pipeline["last_error"] = str(exc)
            pipeline["completed_stages"] = completed_stages
            pipeline["retries"] = retries
            pipeline["max_retries"] = max_retries
            await crud.patch(session, current_record, {"data": data, "updated_at": utcnow()})
            failed_stage = stage
            break

    if failed_stage is None:
        pipeline["status"] = "completed"
        pipeline["last_error"] = None

    pipeline["completed_stages"] = completed_stages
    pipeline["retries"] = retries
    pipeline["max_retries"] = max_retries
    current_record = await crud.patch(session, current_record, {"data": data, "updated_at": utcnow()})
    current_record = await _repair_podcast_record_if_needed(session=session, record=current_record)

    return PodcastPipelineRunResponse(
        record_id=current_record.id,
        pipeline_status=str(pipeline.get("status") or "processing"),
        completed_stages=completed_stages,
        retries={k: int(v) for k, v in retries.items()},
        failed_stage=failed_stage,
        max_retries=max_retries,
    )


@router.get("/records/{record_id}/view", response_model=PodcastRecordViewResponse)
async def get_podcast_record_view(
    record_id: UUID,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> PodcastRecordViewResponse:
    """Resolve transcript, summary, and extracted actions for a podcast record."""
    record = await _get_org_record_or_404(session=session, record_id=record_id, ctx=ctx)
    record = await _repair_podcast_record_if_needed(session=session, record=record)
    data = dict(record.data or {})

    transcript_path = _first_existing_path(data.get("transcript_path"), data.get("completed_transcript_path"))
    summary_path = _first_existing_path(data.get("summary_path"), data.get("completed_summary_path"))
    transcript_words_path = _first_existing_path(data.get("transcript_words_path"))
    transcript_vtt_path = _first_existing_path(data.get("transcript_vtt_path"))

    transcript_words: list[dict[str, object]] = []
    if transcript_words_path:
        try:
            payload = json.loads(Path(transcript_words_path).read_text(encoding="utf-8"))
            transcript_words = _coerce_words_payload(payload)
        except Exception:
            transcript_words = []

    summary_text = load_text_if_exists(summary_path)
    transcript_text = load_text_if_exists(transcript_path)
    summary_lines = [line.strip() for line in (summary_text or "").splitlines()]
    current_section: str | None = None
    section_map: dict[str, list[str]] = {"key_points": [], "decisions": [], "risks": []}
    for line in summary_lines:
        normalized = line.lower()
        if normalized == "key points":
            current_section = "key_points"
            continue
        if normalized == "decisions":
            current_section = "decisions"
            continue
        if normalized == "risks":
            current_section = "risks"
            continue
        if normalized == "action plan":
            current_section = None
            continue
        if current_section and line.startswith("- "):
            section_map[current_section].append(line[2:].strip())

    return PodcastRecordViewResponse(
        record_id=record.id,
        title=record.title,
        summary=record.summary,
        category=str(data.get("category") or "") or None,
        transcript_path=transcript_path,
        summary_path=summary_path,
        transcript_text=transcript_text,
        transcript_words=transcript_words,
        transcript_vtt_text=load_text_if_exists(transcript_vtt_path),
        summary_text=summary_text,
        action_points=_coerce_string_list(data.get("action_points")),
        key_points=section_map["key_points"],
        decisions=section_map["decisions"],
        risks=section_map["risks"],
    )


@router.get("/records/{record_id}/audio")
async def get_podcast_record_audio(
    record_id: UUID,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> FileResponse:
    """Stream the stored audio artifact for a podcast record."""
    record = await _get_org_record_or_404(session=session, record_id=record_id, ctx=ctx)
    record = await _repair_podcast_record_if_needed(session=session, record=record)
    data = dict(record.data or {})
    audio_path = _first_existing_path(data.get("audio_path"), data.get("completed_audio_path"), data.get("source_path"))
    if not audio_path:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Podcast audio not found.")
    media_type = mimetypes.guess_type(audio_path)[0] or "application/octet-stream"
    return FileResponse(audio_path, media_type=media_type, filename=Path(audio_path).name)


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
