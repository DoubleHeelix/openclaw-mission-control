"""Read-side Budget V2 API assemblers.

These helpers keep FastAPI route handlers thin by centralizing data loading,
trust evaluation, and response shaping.
"""

from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal
from typing import Any
from uuid import UUID, NAMESPACE_URL, uuid5

from sqlmodel import col, select

from app.core.logging import get_logger
from app.models.budget import (
    BudgetImportSession,
    BudgetLineItem,
    BudgetManualOverride,
    BudgetNormalizedTransaction,
    BudgetParsedStatement,
    BudgetParsedTransaction,
    BudgetSnapshot,
    BudgetTransactionClassification,
)
from app.schemas.budget import (
    BudgetImportSummaryResponse,
    BudgetLineItemResponse,
    BudgetLinesResponse,
    BudgetModelTotalsResponse,
    BudgetSnapshotResponse,
    BudgetSnapshotSummary,
    BudgetStatementTruth,
    BudgetTransactionReviewRow,
    BudgetTrustResponse,
)
from app.services.budget_v2.assembly import build_lines
from app.services.budget_v2.config import CLASSIFICATION_VERSION, canonicalize_expense_taxonomy
from app.services.budget_v2.diagnostics import build_diagnostics
from app.services.budget_v2.engine import _apply_group_override_history, _historical_recurrence_map, _load_classified_transactions
from app.services.budget_v2.identity import canonical_group_key, confidence_label, review_priority
from app.services.budget_v2.aggregator import aggregate_sections
from app.services.budget_v2.totals import compute_budget_model_totals, compute_review_metrics, compute_statement_totals, compute_statement_window_days, observational_monthly_estimate
from app.services.budget_v2.trust import assess_import_trust, assess_line_trust, import_scope_warnings

RECURRING_CADENCES = {"weekly", "fortnightly", "monthly", "quarterly", "yearly"}
ALLOWED_CADENCES = RECURRING_CADENCES | {"irregular", "unknown"}
ALLOWED_IMPACTS = {"included", "excluded", "reserve_only"}
logger = get_logger(__name__)


PARSER_BLOCKING_REASONS = {
    "parser_anomaly",
    "suspected_leakage",
    "line_integrity_failure",
    "section_total_mismatch",
    "overlap_detected",
}

BUCKET_DECISION_REASONS = {
    "low_confidence",
    "single_occurrence_only",
    "likely_one_off",
    "likely_payroll_candidate",
    "salary_like_single_occurrence",
    "possible_recurring_insufficient_occurrences",
    "credit_requires_review",
    "large_debit_unclassified",
    "transfer_like_requires_review",
    "statement_shorter_than_cycle",
}

CADENCE_DECISION_REASONS = {
    "weak_cadence_evidence",
    "cadence_ambiguous_material",
    "possible_recurring_insufficient_occurrences",
    "statement_shorter_than_cycle",
}


def _decimal(value: object, default: Decimal | None = Decimal("0.00")) -> Decimal | None:
    if value is None:
        return default
    return Decimal(str(value))


def _decimal_or_none(value: object) -> Decimal | None:
    return _decimal(value, default=None)


def _int_map(value: object) -> dict[str, int]:
    if isinstance(value, dict):
        items = value.items()
    elif isinstance(value, list):
        try:
            items = dict(value).items()
        except Exception:
            return {}
    else:
        return {}
    output: dict[str, int] = {}
    for key, raw in items:
        try:
            output[str(key)] = int(raw)
        except Exception:
            continue
    return output


def _safe_cadence(value: object) -> str:
    cadence = str(value or "unknown").strip().lower()
    if cadence in ALLOWED_CADENCES:
        return cadence
    if cadence in {"observed_only", "observed only", "ad_hoc"}:
        return "unknown"
    return "unknown"


def _safe_impact(value: object, *, default: str = "included") -> str:
    impact = str(value or default).strip().lower()
    if impact in ALLOWED_IMPACTS:
        return impact
    return default


def _safe_str(value: object, *, default: str = "") -> str:
    cleaned = str(value or default)
    return cleaned if cleaned else default


def _line_meta(row: object) -> dict[str, object]:
    extra_data = getattr(row, "extra_data", None)
    if isinstance(extra_data, dict):
        return extra_data
    metadata = getattr(row, "metadata", None)
    if isinstance(metadata, dict):
        return metadata
    return {}


def _line_id(row: object) -> UUID:
    existing = getattr(row, "id", None)
    if isinstance(existing, UUID):
        return existing
    group_key = getattr(row, "group_key", None)
    if group_key:
        return uuid5(NAMESPACE_URL, f"budget-line:{group_key}")
    return uuid5(NAMESPACE_URL, f"budget-line:{id(row)}")


async def _load_effective_lines(*, session, organization_id: UUID, import_id: UUID) -> list[BudgetLineItem]:
    classified = await _load_classified_transactions(
        session=session,
        organization_id=organization_id,
        import_id=import_id,
    )
    if classified:
        parsed_statement = (
            await session.exec(
                select(BudgetParsedStatement)
                .where(col(BudgetParsedStatement.organization_id) == organization_id)
                .where(col(BudgetParsedStatement.import_session_id) == import_id)
            )
        ).first()
        historical_recurrence = await _historical_recurrence_map(
            session=session,
            organization_id=organization_id,
            import_id=import_id,
        )
        rebuilt_lines = build_lines(
            classified,
            historical_recurrence=historical_recurrence,
            statement_window_days=compute_statement_window_days(
                statement_start_date=parsed_statement.statement_start_date if parsed_statement else None,
                statement_end_date=parsed_statement.statement_end_date if parsed_statement else None,
            ),
        )
        group_overrides = list(
            await session.exec(
                select(BudgetManualOverride)
                .where(col(BudgetManualOverride.organization_id) == organization_id)
                .where(col(BudgetManualOverride.import_session_id) == import_id)
                .where(col(BudgetManualOverride.target_type) == "group")
            )
        )
        return _apply_group_override_history(rebuilt_lines, group_overrides)

    return list(
        await session.exec(
            select(BudgetLineItem)
            .where(col(BudgetLineItem.import_session_id) == import_id)
            .where(col(BudgetLineItem.organization_id) == organization_id)
        )
    )


def _resolved_like_from_rows(parsed_rows, norm_rows, class_rows):
    parsed_by_id = {row.id: row for row in parsed_rows}
    norm_by_id = {row.id: row for row in norm_rows}
    resolved = []
    for row in class_rows:
        norm = norm_by_id.get(row.normalized_transaction_id)
        parsed = parsed_by_id.get(norm.parsed_transaction_id) if norm else None
        if norm is None or parsed is None:
            continue
        meta = row.extra_data if isinstance(row.extra_data, dict) else {}
        resolved.append(
            type(
                "ResolvedLike",
                (),
                {
                    "row_index": parsed.row_index,
                    "amount": parsed.amount,
                    "direction": parsed.direction,
                    "confidence": row.confidence,
                    "final_bucket": str(meta.get("final_bucket", "uncategorized")),
                    "review_flags": list(meta.get("review_flags", []) or []),
                    "reasons": list(meta.get("reasons", []) or []),
                },
            )()
        )
    return resolved


def _resolved_like_from_lines(line_rows):
    resolved = []
    for row in line_rows:
        meta = _line_meta(row)
        raw_final_bucket = meta.get("final_bucket")
        final_bucket = str(raw_final_bucket) if raw_final_bucket not in {None, "", "None"} else "uncategorized"
        resolved.append(
            type(
                "ResolvedLike",
                (),
                {
                    "row_index": str(_line_id(row)),
                    "amount": _decimal(meta.get("observed_amount"), row.observed_window_total) or Decimal("0.00"),
                    "direction": "credit" if row.line_type == "income" else "debit",
                    "confidence": float(row.confidence),
                    "final_bucket": final_bucket,
                    "review_flags": list(meta.get("review_flags", []) or []),
                    "reasons": list(meta.get("reasons", []) or []),
                },
            )()
        )
    return resolved


async def load_import_summary_response(*, session, organization_id: UUID, import_row: BudgetImportSession) -> BudgetImportSummaryResponse:
    try:
        parsed_statement = (
            await session.exec(
                select(BudgetParsedStatement)
                .where(col(BudgetParsedStatement.import_session_id) == import_row.id)
                .where(col(BudgetParsedStatement.organization_id) == organization_id)
                .order_by(col(BudgetParsedStatement.created_at).desc())
            )
        ).first()
        line_rows = await _load_effective_lines(
            session=session,
            organization_id=organization_id,
            import_id=import_row.id,
        )
        parsed_rows = list(
            await session.exec(
                select(BudgetParsedTransaction)
                .where(col(BudgetParsedTransaction.import_session_id) == import_row.id)
                .where(col(BudgetParsedTransaction.organization_id) == organization_id)
            )
        )
        norm_rows = list(
            await session.exec(
                select(BudgetNormalizedTransaction)
                .where(col(BudgetNormalizedTransaction.import_session_id) == import_row.id)
                .where(col(BudgetNormalizedTransaction.organization_id) == organization_id)
            )
        )
        class_rows = list(
            await session.exec(
                select(BudgetTransactionClassification)
                .where(col(BudgetTransactionClassification.import_session_id) == import_row.id)
                .where(col(BudgetTransactionClassification.organization_id) == organization_id)
            )
        )
        parsed_meta = parsed_statement.extra_data if parsed_statement and isinstance(parsed_statement.extra_data, dict) else {}
        trust = assess_import_trust(import_row=import_row, parsed_meta=parsed_meta, lines=line_rows)
        statement_truth = compute_statement_totals(
            import_row=import_row,
            parsed_rows=parsed_rows,
            expected_closing_balance=(
                _decimal(parsed_meta.get("expected_closing_balance"), default=None)
                if parsed_meta.get("expected_closing_balance") is not None
                else None
            ),
            truth_trust_level=trust.truth_trust_level,
        )
        budget_model = compute_budget_model_totals(
            lines=line_rows,
            modeling_allowed=trust.modeling_allowed,
            modeling_restrictions=trust.modeling_restrictions,
            statement_window_days=compute_statement_window_days(
                statement_start_date=import_row.statement_start_date,
                statement_end_date=import_row.statement_end_date,
            ),
        )
        resolved = _resolved_like_from_lines(line_rows)
        aggregate = aggregate_sections(resolved)
        diagnostics = build_diagnostics(
            transactions=resolved,
            aggregate=aggregate,
            total_credits=statement_truth.total_credits,
            total_debits=statement_truth.total_debits,
        )
        budget_model.classification_reconciliation_status = diagnostics.classification_reconciliation_status
        budget_model.classification_reconciliation_difference = diagnostics.classification_reconciliation_difference
        budget_model.classified_section_totals = aggregate.totals
        budget_model.section_confidence = diagnostics.section_confidence
        budget_model.statement_model_confidence = diagnostics.statement_model_confidence
        review_metrics = compute_review_metrics(line_rows)
        parser_coverage_warnings = [
            warning
            for warning in (import_row.parser_warnings or [])
            if "Parser likely incomplete" in warning
            or "coverage" in warning.lower()
            or "multi-page" in warning.lower()
            or "reconciliation" in warning.lower()
        ]
        parsed_statement_coverage = list(parsed_meta.get("parser_coverage_warnings", [])) if isinstance(parsed_meta, dict) else []

        return BudgetImportSummaryResponse(
            import_id=import_row.id,
            status=import_row.status,
            source_bank=import_row.source_bank,
            source_format=import_row.source_format,
            parser_name=import_row.parser_name,
            parser_confidence=import_row.parser_confidence,
            parser_warnings=import_row.parser_warnings,
            statement_start_date=import_row.statement_start_date,
            statement_end_date=import_row.statement_end_date,
            transaction_count=import_row.transaction_count,
            extracted_debit_total=import_row.extracted_debit_total,
            extracted_credit_total=import_row.extracted_credit_total,
            parsed_debit_count=sum(1 for item in parsed_rows if item.direction == "debit"),
            parsed_credit_count=sum(1 for item in parsed_rows if item.direction == "credit"),
            parsed_debit_total=statement_truth.total_debits,
            parsed_credit_total=statement_truth.total_credits,
            opening_balance=import_row.opening_balance,
            closing_balance=import_row.closing_balance,
            statement_total_debits=_decimal(parsed_meta.get("statement_total_debits"), default=None) if parsed_meta.get("statement_total_debits") is not None else None,
            statement_total_credits=_decimal(parsed_meta.get("statement_total_credits"), default=None) if parsed_meta.get("statement_total_credits") is not None else None,
            expected_closing_balance=statement_truth.expected_closing_balance,
            reconciliation_status=import_row.reconciliation_status,
            reconciliation_reason=str(parsed_meta.get("reconciliation_reason")) if parsed_meta.get("reconciliation_reason") else None,
            reconciliation_difference=import_row.reconciliation_difference,
            warning_reasons=list(parsed_meta.get("warning_reasons", [])),
            document_type=str(parsed_meta.get("document_type")) if parsed_meta.get("document_type") else None,
            document_reconcilable=parsed_meta.get("document_reconcilable") if "document_reconcilable" in parsed_meta else None,
            document_warnings=list(parsed_meta.get("document_warnings", [])),
            text_extraction_length=int(parsed_meta.get("text_extraction_length", 0) or 0),
            table_detected=bool(parsed_meta.get("table_detected", False)),
            page_resume_count=int(parsed_meta.get("page_resume_count", 0) or 0),
            page_transaction_counts=_int_map(parsed_meta.get("page_transaction_counts")),
            coverage_estimate=float(parsed_meta.get("coverage_estimate")) if parsed_meta.get("coverage_estimate") is not None else None,
            suspected_missing_pages=bool(parsed_meta.get("suspected_missing_pages", False)),
            duplicate_rows_detected=int(parsed_meta.get("duplicate_rows_detected", 0) or 0),
            overlap_status=str(parsed_meta.get("overlap_status", "clear")),
            parser_failure_reason=str(parsed_meta.get("parser_failure_reason")) if parsed_meta.get("parser_failure_reason") else None,
            row_quality_counts=_int_map(parsed_meta.get("row_quality_counts")),
            direction_source_counts=_int_map(parsed_meta.get("direction_source_counts")),
            needs_review=import_row.needs_review,
            low_confidence_group_count=review_metrics.low_confidence_group_count,
            uncategorized_review_count=review_metrics.uncategorized_review_count,
            scope_warnings=import_scope_warnings(import_row, line_rows),
            parser_coverage_warnings=parser_coverage_warnings or parsed_statement_coverage,
            statement_truth=BudgetStatementTruth.model_validate(asdict(statement_truth)),
            budget_model=BudgetModelTotalsResponse.model_validate(asdict(budget_model)),
            trust=BudgetTrustResponse.model_validate(asdict(trust)),
            updated_at=import_row.updated_at,
        )
    except Exception:
        logger.exception(
            "budget_v2_read_summary_failed import_id=%s organization_id=%s",
            import_row.id,
            organization_id,
        )
        raise


def _override_operation_index(overrides: list[BudgetManualOverride]) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    transaction_ops: dict[str, set[str]] = {}
    group_ops: dict[str, set[str]] = {}
    for override in overrides:
        target = str(override.target_id)
        op = str(override.operation)
        if override.target_type == "transaction":
            transaction_ops.setdefault(target, set()).add(op)
        elif override.target_type == "group":
            group_ops.setdefault(target, set()).add(op)
    return transaction_ops, group_ops


def _resolved_review_reasons(
    *,
    review_reasons: list[str],
    effective_bucket_assignment: str,
    effective_cadence: str,
    transaction_ops: set[str],
    group_ops: set[str],
) -> list[str]:
    reasons = [reason for reason in review_reasons if reason != "unknown_merchant"]

    explicit_bucket_decision = bool(transaction_ops & {"mark_recurring", "mark_one_off", "set_bucket_assignment"} or group_ops & {"mark_recurring", "mark_one_off", "set_bucket_assignment"})
    explicit_cadence_decision = bool(transaction_ops & {"set_cadence"} or group_ops & {"set_cadence"})

    if explicit_bucket_decision:
        reasons = [reason for reason in reasons if reason in PARSER_BLOCKING_REASONS]

    if explicit_cadence_decision:
        reasons = [reason for reason in reasons if reason not in CADENCE_DECISION_REASONS]

    if effective_bucket_assignment == "income_recurring":
        reasons = [
            reason
            for reason in reasons
            if reason not in {"likely_payroll_candidate", "salary_like_single_occurrence", "credit_requires_review"}
        ]

    if effective_bucket_assignment in {"one_off_exceptional", "income_irregular"}:
        reasons = [
            reason
            for reason in reasons
            if reason
            not in {
                "likely_payroll_candidate",
                "salary_like_single_occurrence",
                "weak_cadence_evidence",
                "cadence_ambiguous_material",
                "single_occurrence_only",
                "likely_one_off",
                "possible_recurring_insufficient_occurrences",
                "credit_requires_review",
                "statement_shorter_than_cycle",
            }
        ]

    if effective_bucket_assignment == "recurring_baseline":
        reasons = [
            reason
            for reason in reasons
            if reason
            not in {
                "salary_like_single_occurrence",
                "possible_recurring_insufficient_occurrences",
                "single_occurrence_only",
                "likely_one_off",
            }
        ]

    if effective_bucket_assignment == "variable_discretionary":
        reasons = [
            reason
            for reason in reasons
            if reason not in {"possible_recurring_insufficient_occurrences", "single_occurrence_only", "likely_one_off"}
        ]

    if effective_cadence in RECURRING_CADENCES or effective_cadence == "irregular":
        reasons = [reason for reason in reasons if reason not in {"weak_cadence_evidence", "cadence_ambiguous_material", "statement_shorter_than_cycle"}]

    return sorted(set(reasons))


def group_review_rows(items: list[BudgetTransactionReviewRow]) -> list[BudgetTransactionReviewRow]:
    grouped: dict[str, list[BudgetTransactionReviewRow]] = {}
    for item in items:
        key = item.group_key or str(item.id)
        grouped.setdefault(key, []).append(item)

    collapsed: list[BudgetTransactionReviewRow] = []
    for rows in grouped.values():
        if len(rows) == 1:
            collapsed.append(rows[0])
            continue

        representative = max(rows, key=lambda row: float(row.review_priority or 0))
        total_amount = sum(Decimal(str(row.amount or 0)) for row in rows)
        total_signed_amount = sum(Decimal(str(row.signed_amount or 0)) for row in rows)
        merged_review_reasons = list(dict.fromkeys(reason for row in rows for reason in (row.review_reasons or [])))
        merged_review_flags = list(dict.fromkeys(flag for row in rows for flag in (row.review_flags or [])))
        merged_reasons = list(dict.fromkeys(reason for row in rows for reason in (row.reasons or [])))
        merged_merge_targets = list(
            dict.fromkeys(
                candidate if isinstance(candidate, str) else str(candidate)
                for row in rows
                for candidate in (row.likely_merge_targets or [])
            )
        )

        collapsed.append(
            representative.model_copy(
                update={
                    "amount": total_amount,
                    "signed_amount": total_signed_amount,
                    "review_reasons": merged_review_reasons,
                    "review_flags": merged_review_flags,
                    "reasons": merged_reasons,
                    "group_transaction_count": max(representative.group_transaction_count, len(rows)),
                    "review_priority": max(float(row.review_priority or 0) for row in rows),
                    "likely_merge_targets": merged_merge_targets,
                }
            )
        )

    return sorted(collapsed, key=lambda item: float(item.review_priority or 0), reverse=True)


async def load_transaction_review_rows(*, session, organization_id: UUID, import_id: UUID) -> list[BudgetTransactionReviewRow]:
    parsed_rows = list(
        await session.exec(
            select(BudgetParsedTransaction)
            .where(col(BudgetParsedTransaction.import_session_id) == import_id)
            .where(col(BudgetParsedTransaction.organization_id) == organization_id)
            .order_by(col(BudgetParsedTransaction.row_index))
        )
    )
    norm_rows = list(
        await session.exec(
            select(BudgetNormalizedTransaction)
            .where(col(BudgetNormalizedTransaction.import_session_id) == import_id)
            .where(col(BudgetNormalizedTransaction.organization_id) == organization_id)
        )
    )
    class_rows = list(
        await session.exec(
            select(BudgetTransactionClassification)
            .where(col(BudgetTransactionClassification.import_session_id) == import_id)
            .where(col(BudgetTransactionClassification.organization_id) == organization_id)
        )
    )
    line_rows = list(
        await session.exec(
            select(BudgetLineItem)
            .where(col(BudgetLineItem.import_session_id) == import_id)
            .where(col(BudgetLineItem.organization_id) == organization_id)
        )
    )
    overrides = list(
        await session.exec(
            select(BudgetManualOverride)
            .where(col(BudgetManualOverride.import_session_id) == import_id)
            .where(col(BudgetManualOverride.organization_id) == organization_id)
        )
    )

    transaction_override_ops, group_override_ops = _override_operation_index(overrides)

    norm_by_id = {row.id: row for row in norm_rows}
    norm_by_parsed = {row.parsed_transaction_id: row for row in norm_rows}
    class_by_norm = {row.normalized_transaction_id: row for row in class_rows}
    line_by_group = {row.group_key: row for row in line_rows}
    group_counts: dict[str, int] = {}
    for row in class_rows:
        normalized = norm_by_id.get(row.normalized_transaction_id)
        descriptor = normalized.merchant_candidate or normalized.normalized_description if normalized else None
        remapped_category, remapped_subcategory = canonicalize_expense_taxonomy(row.category, row.subcategory, descriptor)
        final_bucket = str(((row.extra_data or {}) if isinstance(row.extra_data, dict) else {}).get("final_bucket", "uncategorized"))
        resolved_group_key = f"{final_bucket}|" + canonical_group_key(remapped_category, remapped_subcategory, descriptor)
        group_counts[resolved_group_key] = group_counts.get(resolved_group_key, 0) + 1

    items: list[BudgetTransactionReviewRow] = []
    for parsed in parsed_rows:
        norm = norm_by_parsed.get(parsed.id)
        if norm is None:
            continue
        classification = class_by_norm.get(norm.id)
        if classification is None:
            continue
        normalized_meta = norm.extra_data if isinstance(norm.extra_data, dict) else {}
        classification_meta = classification.extra_data if isinstance(classification.extra_data, dict) else {}
        remapped_category, remapped_subcategory = canonicalize_expense_taxonomy(
            classification.category,
            classification.subcategory,
            norm.merchant_candidate or norm.normalized_description,
        )
        final_bucket = str(classification_meta.get("final_bucket", "uncategorized"))
        resolved_group_key = f"{final_bucket}|" + canonical_group_key(
            remapped_category,
            remapped_subcategory,
            norm.merchant_candidate or norm.normalized_description,
        )
        line = line_by_group.get(resolved_group_key)
        line_meta = line.extra_data if line and isinstance(line.extra_data, dict) else {}
        effective_bucket_assignment = str(
            line.bucket_assignment if line else classification_meta.get("bucket_assignment", "variable_discretionary")
        )
        effective_cadence = str(line.inferred_cadence if line and line.inferred_cadence else classification.inferred_cadence or "unknown")
        effective_cadence_reason = str(line.cadence_reason if line and line.cadence_reason else classification.cadence_reason or "")
        effective_observed_only = bool(line.observed_only if line else classification.observed_only)
        effective_impact_on_baseline = line.impact_on_baseline if line else classification.impact_on_baseline
        effective_included = bool(line.included if line else classification.included)
        review_reasons = _resolved_review_reasons(
            review_reasons=sorted(set(list(classification.review_reasons or []) + (list(line.review_reasons or []) if line else []))),
            effective_bucket_assignment=effective_bucket_assignment,
            effective_cadence=effective_cadence,
            transaction_ops=transaction_override_ops.get(str(norm.id), set()),
            group_ops=group_override_ops.get(resolved_group_key, set()),
        )
        cadence_conf = classification.cadence_confidence if classification.cadence_confidence is not None else (line.cadence_confidence if line else None)
        items.append(
            BudgetTransactionReviewRow(
                id=norm.id,
                transaction_date=parsed.transaction_date,
                effective_date=parsed.effective_date,
                amount=parsed.amount,
                signed_amount=_decimal(normalized_meta.get("signed_amount"), parsed.amount if parsed.direction == "credit" else parsed.amount * Decimal("-1")),
                direction=parsed.direction,
                movement_type=str(classification_meta.get("movement_type", normalized_meta.get("movement_type", "other_needs_review"))),
                raw_description=parsed.raw_description,
                normalized_description=norm.normalized_description,
                merchant_base_name=str(normalized_meta.get("merchant_base_name")) if normalized_meta.get("merchant_base_name") else None,
                merchant_fingerprint=str(normalized_meta.get("merchant_fingerprint")) if normalized_meta.get("merchant_fingerprint") else None,
                direction_source=str(normalized_meta.get("direction_source", (
                    "explicit_column"
                    if "direction_source_explicit_column" in (parsed.parser_flags or [])
                    else "balance_delta"
                    if "direction_source_balance_delta" in (parsed.parser_flags or [])
                    else "fallback"
                ))),
                parser_flags=list(parsed.parser_flags or []),
                row_quality_bucket=str(normalized_meta.get("row_quality_bucket")) if normalized_meta.get("row_quality_bucket") else None,
                row_shape_confidence=float(normalized_meta.get("row_shape_confidence")) if normalized_meta.get("row_shape_confidence") is not None else None,
                amount_parse_confidence=float(normalized_meta.get("amount_parse_confidence")) if normalized_meta.get("amount_parse_confidence") is not None else None,
                date_parse_confidence=float(normalized_meta.get("date_parse_confidence")) if normalized_meta.get("date_parse_confidence") is not None else None,
                section_context_confidence=float(normalized_meta.get("section_context_confidence")) if normalized_meta.get("section_context_confidence") is not None else None,
                amount_source=str(normalized_meta.get("amount_source")) if normalized_meta.get("amount_source") else None,
                date_source=str(normalized_meta.get("date_source")) if normalized_meta.get("date_source") else None,
                balance_source=str(normalized_meta.get("balance_source")) if normalized_meta.get("balance_source") else None,
                description_continuation_detected=bool(normalized_meta.get("description_continuation_detected", False)),
                classification_version=str(classification_meta.get("classification_version") or normalized_meta.get("classification_version") or CLASSIFICATION_VERSION),
                mapping_source=str(classification_meta.get("mapping_source")) if classification_meta.get("mapping_source") else None,
                transaction_classification_confidence=classification.confidence,
                final_bucket=str(line_meta.get("final_bucket") or classification_meta.get("final_bucket")) if (line_meta.get("final_bucket") or classification_meta.get("final_bucket")) else None,
                reasons=list(line_meta.get("reasons", []) or classification_meta.get("reasons", []) or []),
                review_flags=list(line_meta.get("review_flags", []) or classification_meta.get("review_flags", []) or []),
                payment_rail=norm.payment_rail,
                merchant_candidate=norm.merchant_candidate,
                interpretation_type=classification.interpretation_type,
                interpretation_confidence=classification.interpretation_confidence,
                interpretation_reason=classification.interpretation_reason,
                classification_type=classification.classification_type,
                category=remapped_category,
                subcategory=remapped_subcategory,
                confidence=classification.confidence,
                merchant_confidence=float(classification_meta.get("merchant_confidence", classification.confidence)),
                bucket_confidence=float(classification_meta.get("bucket_confidence", classification.confidence)),
                explanation=classification.explanation,
                evidence_source=classification.evidence_source,
                bucket_assignment=effective_bucket_assignment,
                confidence_label=str(line_meta.get("confidence_label", confidence_label(classification.confidence, review_reasons))),
                inferred_cadence=effective_cadence,
                cadence_confidence=cadence_conf,
                cadence_reason=effective_cadence_reason,
                impact_on_baseline=effective_impact_on_baseline,
                included=effective_included,
                observed_only=effective_observed_only,
                review_reasons=review_reasons,
                group_key=resolved_group_key,
                group_transaction_count=group_counts.get(resolved_group_key, 0),
                review_priority=review_priority(parsed.amount, review_reasons, cadence_conf),
                likely_merge_targets=list(line_meta.get("likely_merge_targets", [])),
                likely_payroll_candidate=bool(classification_meta.get("likely_payroll_candidate", False))
                and effective_bucket_assignment not in {"income_recurring", "income_irregular"},
            )
        )
    return items


async def load_lines_response(*, session, organization_id: UUID, import_row: BudgetImportSession) -> BudgetLinesResponse:
    lines = sorted(
        await _load_effective_lines(
            session=session,
            organization_id=organization_id,
            import_id=import_row.id,
        ),
        key=lambda item: (
            _safe_str(getattr(item, "category", ""), default=""),
            _safe_str(getattr(item, "subcategory", ""), default=""),
            _safe_str(getattr(item, "group_label", ""), default=""),
        ),
    )
    parsed_rows = list(
        await session.exec(
            select(BudgetParsedTransaction)
            .where(col(BudgetParsedTransaction.import_session_id) == import_row.id)
            .where(col(BudgetParsedTransaction.organization_id) == organization_id)
        )
    )
    parsed_statement = (
        await session.exec(
            select(BudgetParsedStatement)
            .where(col(BudgetParsedStatement.import_session_id) == import_row.id)
            .where(col(BudgetParsedStatement.organization_id) == organization_id)
            .order_by(col(BudgetParsedStatement.created_at).desc())
        )
    ).first()
    parsed_meta = parsed_statement.extra_data if parsed_statement and isinstance(parsed_statement.extra_data, dict) else {}
    trust = assess_import_trust(import_row=import_row, parsed_meta=parsed_meta, lines=lines)
    statement_window_days = compute_statement_window_days(
        statement_start_date=import_row.statement_start_date,
        statement_end_date=import_row.statement_end_date,
    )
    statement_truth = compute_statement_totals(
        import_row=import_row,
        parsed_rows=parsed_rows,
        expected_closing_balance=(
            _decimal(parsed_meta.get("expected_closing_balance"), default=None)
            if parsed_meta.get("expected_closing_balance") is not None
            else None
        ),
        truth_trust_level=trust.truth_trust_level,
    )
    budget_model = compute_budget_model_totals(
        lines=lines,
        modeling_allowed=trust.modeling_allowed,
        modeling_restrictions=trust.modeling_restrictions,
        statement_window_days=statement_window_days,
    )
    norm_rows = list(
        await session.exec(
            select(BudgetNormalizedTransaction)
            .where(col(BudgetNormalizedTransaction.import_session_id) == import_row.id)
            .where(col(BudgetNormalizedTransaction.organization_id) == organization_id)
        )
    )
    class_rows = list(
        await session.exec(
            select(BudgetTransactionClassification)
            .where(col(BudgetTransactionClassification.import_session_id) == import_row.id)
            .where(col(BudgetTransactionClassification.organization_id) == organization_id)
        )
    )
    resolved = _resolved_like_from_lines(lines)
    aggregate = aggregate_sections(resolved)
    diagnostics = build_diagnostics(
        transactions=resolved,
        aggregate=aggregate,
        total_credits=statement_truth.total_credits,
        total_debits=statement_truth.total_debits,
    )
    budget_model.classification_reconciliation_status = diagnostics.classification_reconciliation_status
    budget_model.classification_reconciliation_difference = diagnostics.classification_reconciliation_difference
    budget_model.classified_section_totals = aggregate.totals
    budget_model.section_confidence = diagnostics.section_confidence
    budget_model.statement_model_confidence = diagnostics.statement_model_confidence

    output = []
    for item in lines:
        item_meta = _line_meta(item)
        line_assessment = assess_line_trust(line=item, import_assessment=trust)
        remapped_category, remapped_subcategory = canonicalize_expense_taxonomy(
            item.category,
            item.subcategory,
            item.group_label,
        )
        output.append(BudgetLineItemResponse(
            id=_line_id(item),
            group_key=item.group_key,
            group_label=_safe_str(item.group_label, default=item.group_key),
            line_type=_safe_str(item.line_type, default="expense"),
            category=_safe_str(remapped_category, default="Discretionary"),
            subcategory=_safe_str(remapped_subcategory, default="Presents"),
            inferred_cadence=_safe_cadence(item.inferred_cadence),
            cadence_confidence=item.cadence_confidence,
            cadence_reason=_safe_str(item.cadence_reason, default="unknown"),
            observed_only=item.observed_only,
            bucket_assignment=item.bucket_assignment,
            bucket_suggestion=str(item_meta.get("bucket_suggestion", "suggested_discretionary")),
            modeling_status=str(item_meta.get("modeling_status", "observational_only")),
            recurrence_state=str(item_meta.get("recurrence_state", "unknown")),
            is_modeled=bool(item_meta.get("is_modeled", False)),
            modeled_by_default=bool(item_meta.get("modeled_by_default", False)),
            base_amount=item.base_amount,
            base_period=_safe_str(item.base_period, default="monthly"),
            authoritative_field=_safe_str(item.authoritative_field, default="monthly"),
            source_amount=item.source_amount,
            source_period=_safe_str(item.source_period, default="monthly"),
            observed_window_total=item.observed_window_total,
            normalized_weekly=item.normalized_weekly,
            normalized_fortnightly=item.normalized_fortnightly,
            normalized_monthly=item.normalized_monthly,
            normalized_yearly=item.normalized_yearly,
            reserve_monthly_equivalent=item.reserve_monthly_equivalent,
            impact_on_baseline=_safe_impact(item.impact_on_baseline),
            included=item.included,
            confidence=item.confidence,
            merchant_confidence=float(item_meta.get("merchant_confidence", item.confidence)),
            bucket_confidence=float(item_meta.get("bucket_confidence", item.confidence)),
            confidence_label=str(item_meta.get("confidence_label", confidence_label(item.confidence, list(item.review_reasons or [])))),
            explanation=_safe_str(item.explanation, default=""),
            baseline_decision_reason=item_meta.get("baseline_decision_reason"),
            notes=item.notes,
            review_reasons=list(item.review_reasons or []),
            transaction_count=item.transaction_count,
            observed_amount=_decimal(item_meta.get("observed_amount"), item.observed_window_total),
            observational_monthly_estimate=observational_monthly_estimate(
                observed_total=item.observed_window_total,
                statement_window_days=statement_window_days,
            ),
            observed_frequency_label=str(item_meta.get("observed_frequency_label", "")),
            line_trust_level=str(item_meta.get("line_trust_level", line_assessment.line_trust_level)),
            modeling_eligible=bool(item_meta.get("modeling_eligible", line_assessment.modeling_eligible)),
            modeling_block_reason=item_meta.get("modeling_block_reason") or line_assessment.modeling_block_reason,
            classification_version=item_meta.get("classification_version") or CLASSIFICATION_VERSION,
            mapping_source=item_meta.get("mapping_source"),
            line_integrity_status=str(item_meta.get("line_integrity_status", "verified")),
            final_bucket=item_meta.get("final_bucket"),
            section_confidence=diagnostics.section_confidence.get(str(item_meta.get("final_bucket", "uncategorized"))),
            reasons=list(item_meta.get("reasons", []) or []),
            review_flags=list(item_meta.get("review_flags", []) or []),
            duplicate_group_candidates=list(item_meta.get("duplicate_group_candidates", [])),
            merge_candidate_confidence=float(item_meta.get("merge_candidate_confidence", 0.0)),
            movement_type=str(item_meta.get("movement_type", "other_needs_review")),
            trust_level=trust.totals_trust_level,
            modeling_allowed=trust.modeling_allowed,
        ))

    totals = {
        "monthly_recurring_income": budget_model.recurring_income_monthly,
        "observed_irregular_income_total": budget_model.irregular_income_total,
        "monthly_recurring_baseline": budget_model.recurring_baseline_monthly,
        "monthly_variable_discretionary": budget_model.variable_discretionary_monthly,
        "observed_variable_discretionary_total": sum(
            (_decimal(_line_meta(item).get("observed_amount"), item.observed_window_total) for item in lines if item.included and item.bucket_assignment == "variable_discretionary"),
            Decimal("0.00"),
        ),
        "observed_one_off_exceptional_total": budget_model.observed_one_off_total,
        "observed_transfer_total": budget_model.observed_transfer_total,
        "monthly_income": budget_model.recurring_income_monthly,
        "monthly_expenses": budget_model.recurring_baseline_monthly + budget_model.variable_discretionary_monthly,
        "monthly_net": budget_model.observed_net,
        "net_recurring_monthly": budget_model.core_net,
        "net_observed_monthly": budget_model.observed_net,
    }

    return BudgetLinesResponse(
        items=output,
        totals=totals,
        statement_truth=BudgetStatementTruth.model_validate(asdict(statement_truth)),
        budget_model=BudgetModelTotalsResponse.model_validate(asdict(budget_model)),
        trust=BudgetTrustResponse.model_validate(asdict(trust)),
    )


async def load_snapshot_response(*, session, organization_id: UUID, import_id: UUID, snapshot: BudgetSnapshot) -> BudgetSnapshotResponse:
    summary_payload: dict[str, Any] = snapshot.summary if isinstance(snapshot.summary, dict) else {}
    statement_truth_payload = summary_payload.get("statement_truth", {})
    budget_model_payload = summary_payload.get("budget_model", {})
    trust_payload = summary_payload.get("trust", {})
    return BudgetSnapshotResponse(
        import_id=import_id,
        summary=BudgetSnapshotSummary.model_validate(summary_payload),
        statement_truth=BudgetStatementTruth.model_validate(statement_truth_payload),
        budget_model=BudgetModelTotalsResponse.model_validate(budget_model_payload),
        trust=BudgetTrustResponse.model_validate(trust_payload),
        generated_at=snapshot.updated_at,
    )
