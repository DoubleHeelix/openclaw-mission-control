"""Budget V2 API under control-center namespace."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlmodel import col, select

from app.api.deps import require_org_member
from app.db import crud
from app.db.session import get_session
from app.models.budget import (
    BudgetImportSession,
    BudgetLineItem,
    BudgetManualOverride,
    BudgetMerchantMemory,
    BudgetNormalizedTransaction,
    BudgetParsedStatement,
    BudgetParsedTransaction,
    BudgetRawFile,
    BudgetSnapshot,
    BudgetTransactionClassification,
)
from app.schemas.budget import (
    BudgetImportResponse,
    BudgetImportSummaryResponse,
    BudgetLinesResponse,
    BudgetMerchantMemoryItem,
    BudgetMerchantMemoryListResponse,
    BudgetMerchantMemoryUpsertRequest,
    BudgetNeedsReviewResponse,
    BudgetOverrideRequest,
    BudgetOverrideResponse,
    BudgetParserCapability,
    BudgetParserListResponse,
    BudgetResetResponse,
    BudgetSnapshotResponse,
    BudgetTransactionsResponse,
)
from app.services.budget_v2.engine import apply_overrides, list_latest_snapshot, recompute_snapshot, run_import
from app.services.budget_v2.identity import merchant_fingerprint
from app.services.budget_v2.parsers import PARSERS
from app.services.budget_v2.read_api import (
    group_review_rows,
    load_import_summary_response,
    load_lines_response,
    load_snapshot_response,
    load_transaction_review_rows,
)
from app.services.organizations import OrganizationContext

router = APIRouter(prefix="/control-center/budget", tags=["control-center", "budget"])
SESSION_DEP = Depends(get_session)
ORG_MEMBER_DEP = Depends(require_org_member)


@router.get("/parsers", response_model=BudgetParserListResponse)
async def list_parsers() -> BudgetParserListResponse:
    return BudgetParserListResponse(
        parsers=[
            BudgetParserCapability(name=parser.name, banks=list(parser.banks), formats=["pdf"])
            for parser in PARSERS
        ]
    )


@router.post("/imports", response_model=BudgetImportResponse)
async def import_statement(
    file: UploadFile = File(...),
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> BudgetImportResponse:
    if file.content_type and "pdf" not in file.content_type.lower():
        raise HTTPException(status_code=400, detail="Only PDF import is supported in this phase.")
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    imported = await run_import(
        session=session,
        organization_id=ctx.organization.id,
        filename=file.filename or "statement.pdf",
        content_type=file.content_type,
        raw_bytes=raw,
    )
    return BudgetImportResponse(
        import_id=imported.id,
        status=imported.status,
        source_bank=imported.source_bank,
        parser_name=imported.parser_name,
        parser_confidence=imported.parser_confidence,
        parser_warnings=imported.parser_warnings,
        transaction_count=imported.transaction_count,
    )


async def _get_import_or_404(*, session, organization_id: UUID, import_id: UUID) -> BudgetImportSession:
    row = await session.exec(
        select(BudgetImportSession)
        .where(col(BudgetImportSession.id) == import_id)
        .where(col(BudgetImportSession.organization_id) == organization_id)
    )
    obj = row.first()
    if obj is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    return obj


async def _get_latest_import(*, session, organization_id: UUID) -> BudgetImportSession | None:
    row = await session.exec(
        select(BudgetImportSession)
        .where(col(BudgetImportSession.organization_id) == organization_id)
        .order_by(col(BudgetImportSession.updated_at).desc(), col(BudgetImportSession.created_at).desc())
    )
    return row.first()


@router.get("/imports/latest", response_model=BudgetImportSummaryResponse)
async def get_latest_import_summary(
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> BudgetImportSummaryResponse:
    latest = await _get_latest_import(session=session, organization_id=ctx.organization.id)
    if latest is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    return await load_import_summary_response(session=session, organization_id=ctx.organization.id, import_row=latest)


@router.post("/reset", response_model=BudgetResetResponse)
async def reset_budget_workspace(
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> BudgetResetResponse:
    imports = list(
        await session.exec(
            select(BudgetImportSession.id).where(col(BudgetImportSession.organization_id) == ctx.organization.id)
        )
    )
    import_ids = [row for row in imports]

    if import_ids:
        await crud.delete_where(
            session,
            BudgetSnapshot,
            col(BudgetSnapshot.organization_id) == ctx.organization.id,
            col(BudgetSnapshot.import_session_id).in_(import_ids),
        )
        await crud.delete_where(
            session,
            BudgetLineItem,
            col(BudgetLineItem.organization_id) == ctx.organization.id,
            col(BudgetLineItem.import_session_id).in_(import_ids),
        )
        await crud.delete_where(
            session,
            BudgetManualOverride,
            col(BudgetManualOverride.organization_id) == ctx.organization.id,
            col(BudgetManualOverride.import_session_id).in_(import_ids),
        )
        await crud.delete_where(
            session,
            BudgetTransactionClassification,
            col(BudgetTransactionClassification.organization_id) == ctx.organization.id,
            col(BudgetTransactionClassification.import_session_id).in_(import_ids),
        )
        await crud.delete_where(
            session,
            BudgetNormalizedTransaction,
            col(BudgetNormalizedTransaction.organization_id) == ctx.organization.id,
            col(BudgetNormalizedTransaction.import_session_id).in_(import_ids),
        )
        await crud.delete_where(
            session,
            BudgetParsedTransaction,
            col(BudgetParsedTransaction.organization_id) == ctx.organization.id,
            col(BudgetParsedTransaction.import_session_id).in_(import_ids),
        )
        await crud.delete_where(
            session,
            BudgetParsedStatement,
            col(BudgetParsedStatement.organization_id) == ctx.organization.id,
            col(BudgetParsedStatement.import_session_id).in_(import_ids),
        )
        await crud.delete_where(
            session,
            BudgetRawFile,
            col(BudgetRawFile.organization_id) == ctx.organization.id,
            col(BudgetRawFile.import_session_id).in_(import_ids),
        )

    deleted_import_count = await crud.delete_where(
        session,
        BudgetImportSession,
        col(BudgetImportSession.organization_id) == ctx.organization.id,
        commit=True,
    )
    return BudgetResetResponse(reset=True, deleted_import_count=deleted_import_count)


@router.get("/imports/{import_id}", response_model=BudgetImportSummaryResponse)
async def get_import_summary(
    import_id: UUID,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> BudgetImportSummaryResponse:
    row = await _get_import_or_404(session=session, organization_id=ctx.organization.id, import_id=import_id)
    return await load_import_summary_response(session=session, organization_id=ctx.organization.id, import_row=row)


@router.get("/imports/{import_id}/transactions", response_model=BudgetTransactionsResponse)
async def get_transactions(
    import_id: UUID,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> BudgetTransactionsResponse:
    await _get_import_or_404(session=session, organization_id=ctx.organization.id, import_id=import_id)
    items = await load_transaction_review_rows(session=session, organization_id=ctx.organization.id, import_id=import_id)
    return BudgetTransactionsResponse(items=items, total=len(items))


@router.get("/imports/{import_id}/needs-review", response_model=BudgetNeedsReviewResponse)
async def get_needs_review(
    import_id: UUID,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> BudgetNeedsReviewResponse:
    await _get_import_or_404(session=session, organization_id=ctx.organization.id, import_id=import_id)
    items = await load_transaction_review_rows(session=session, organization_id=ctx.organization.id, import_id=import_id)
    filtered = group_review_rows([item for item in items if item.review_reasons])
    return BudgetNeedsReviewResponse(items=filtered, total=len(filtered))


@router.get("/imports/{import_id}/lines", response_model=BudgetLinesResponse)
async def get_lines(
    import_id: UUID,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> BudgetLinesResponse:
    row = await _get_import_or_404(session=session, organization_id=ctx.organization.id, import_id=import_id)
    return await load_lines_response(session=session, organization_id=ctx.organization.id, import_row=row)


@router.get("/imports/{import_id}/lines/{group_key:path}/transactions", response_model=BudgetTransactionsResponse)
async def get_line_transactions(
    import_id: UUID,
    group_key: str,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> BudgetTransactionsResponse:
    await _get_import_or_404(session=session, organization_id=ctx.organization.id, import_id=import_id)
    items = await load_transaction_review_rows(session=session, organization_id=ctx.organization.id, import_id=import_id)
    filtered = [item for item in items if item.group_key == group_key]
    return BudgetTransactionsResponse(items=filtered, total=len(filtered))


@router.patch("/imports/{import_id}/overrides", response_model=BudgetOverrideResponse)
async def patch_overrides(
    import_id: UUID,
    payload: BudgetOverrideRequest,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> BudgetOverrideResponse:
    await _get_import_or_404(session=session, organization_id=ctx.organization.id, import_id=import_id)
    applied = await apply_overrides(
        session=session,
        organization_id=ctx.organization.id,
        import_id=import_id,
        user_id=ctx.member.user_id,
        operations=payload.operations,
    )
    return BudgetOverrideResponse(applied=applied)


@router.post("/imports/{import_id}/recompute")
async def recompute_import(
    import_id: UUID,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> dict[str, str]:
    await _get_import_or_404(session=session, organization_id=ctx.organization.id, import_id=import_id)
    await recompute_snapshot(session=session, organization_id=ctx.organization.id, import_id=import_id)
    return {"status": "ok"}


@router.get("/imports/{import_id}/snapshot", response_model=BudgetSnapshotResponse)
async def get_snapshot(
    import_id: UUID,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> BudgetSnapshotResponse:
    await _get_import_or_404(session=session, organization_id=ctx.organization.id, import_id=import_id)
    snapshot = await list_latest_snapshot(session=session, organization_id=ctx.organization.id, import_id=import_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    return await load_snapshot_response(session=session, organization_id=ctx.organization.id, import_id=import_id, snapshot=snapshot)


@router.get("/merchant-memory", response_model=BudgetMerchantMemoryListResponse)
async def list_merchant_memory(
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> BudgetMerchantMemoryListResponse:
    rows = list(
        await session.exec(
            select(BudgetMerchantMemory)
            .where(col(BudgetMerchantMemory.organization_id) == ctx.organization.id)
            .order_by(col(BudgetMerchantMemory.updated_at).desc())
        )
    )
    usage_counts: dict[str, int] = {}
    if rows:
        fingerprints = [row.merchant_fingerprint for row in rows if row.merchant_fingerprint]
        if fingerprints:
            normalized_rows = list(
                await session.exec(
                    select(BudgetNormalizedTransaction)
                    .where(col(BudgetNormalizedTransaction.organization_id) == ctx.organization.id)
                    .where(col(BudgetNormalizedTransaction.merchant_candidate).is_not(None))
                )
            )
            for normalized in normalized_rows:
                meta = normalized.extra_data if isinstance(normalized.extra_data, dict) else {}
                fingerprint = str(meta.get("merchant_fingerprint") or "")
                if fingerprint in fingerprints:
                    usage_counts[fingerprint] = usage_counts.get(fingerprint, 0) + 1
    return BudgetMerchantMemoryListResponse(
        items=[
            BudgetMerchantMemoryItem(
                id=row.id,
                merchant_key=row.merchant_key,
                merchant_fingerprint=row.merchant_fingerprint,
                category=row.category,
                subcategory=row.subcategory,
                confidence=row.confidence,
                source=row.source,
                mapping_source="user_confirmed" if row.source == "manual_override" else "suggested",
                scope="organization",
                usage_count=usage_counts.get(row.merchant_fingerprint or "", 0),
                active=row.active,
            )
            for row in rows
        ]
    )


@router.post("/merchant-memory", response_model=BudgetMerchantMemoryItem)
async def upsert_merchant_memory(
    payload: BudgetMerchantMemoryUpsertRequest,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> BudgetMerchantMemoryItem:
    fingerprint = merchant_fingerprint(payload.merchant_key)
    existing = await session.exec(
        select(BudgetMerchantMemory)
        .where(col(BudgetMerchantMemory.organization_id) == ctx.organization.id)
        .where(col(BudgetMerchantMemory.merchant_fingerprint) == fingerprint)
    )
    row = existing.first()
    if row is None:
        row = await crud.create(
            session,
            BudgetMerchantMemory,
            organization_id=ctx.organization.id,
            merchant_key=payload.merchant_key,
            merchant_fingerprint=fingerprint,
            category=payload.category,
            subcategory=payload.subcategory,
            confidence=payload.confidence,
            source="manual_override",
            active=True,
        )
    else:
        row = await crud.patch(
            session,
            row,
            {
                "merchant_key": payload.merchant_key,
                "category": payload.category,
                "subcategory": payload.subcategory,
                "confidence": payload.confidence,
                "active": True,
            },
        )
    return BudgetMerchantMemoryItem(
        id=row.id,
        merchant_key=row.merchant_key,
        merchant_fingerprint=row.merchant_fingerprint,
        category=row.category,
        subcategory=row.subcategory,
        confidence=row.confidence,
        source=row.source,
        mapping_source="user_confirmed" if row.source == "manual_override" else "suggested",
        scope="organization",
        usage_count=0,
        active=row.active,
    )


@router.delete("/merchant-memory/{memory_id}")
async def forget_merchant_memory(
    memory_id: UUID,
    session=SESSION_DEP,
    ctx: OrganizationContext = ORG_MEMBER_DEP,
) -> dict[str, str]:
    row = await session.exec(
        select(BudgetMerchantMemory)
        .where(col(BudgetMerchantMemory.id) == memory_id)
        .where(col(BudgetMerchantMemory.organization_id) == ctx.organization.id)
    )
    item = row.first()
    if item is None:
        raise HTTPException(status_code=404, detail="Merchant mapping not found")
    await crud.patch(session, item, {"active": False})
    return {"status": "ok"}
