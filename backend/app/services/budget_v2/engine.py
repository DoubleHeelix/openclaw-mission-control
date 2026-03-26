"""End-to-end Budget V2 import, recompute, and override pipeline."""

from __future__ import annotations

from dataclasses import asdict
import hashlib
import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from collections import defaultdict
from uuid import UUID

from sqlmodel import col, select

from app.core.time import utcnow
from app.db import crud
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
from app.schemas.budget import BudgetOverrideOperation
from app.services.budget_v2.aggregator import aggregate_sections
from app.services.budget_v2.assembly import apply_line_source, build_lines, build_snapshot_summary
from app.services.budget_v2.cadence import normalize_from_source
from app.services.budget_v2.classification import should_promote_memory
from app.services.budget_v2.constants import BucketAssignment, ModelingState, MovementType, ReconciliationStatus
from app.services.budget_v2.config import BALANCED_REVIEW_CONFIDENCE_THRESHOLD, canonicalize_expense_taxonomy
from app.services.budget_v2.diagnostics import build_diagnostics
from app.services.budget_v2.expense_classifier import classify_expense
from app.services.budget_v2.identity import canonical_group_key, merchant_fingerprint
from app.services.budget_v2.income_detector import assess_income_transactions
from app.services.budget_v2.merchant_memory import build_memory_hint
from app.services.budget_v2.merchant_classifier import canonicalize_merchant_descriptor
from app.services.budget_v2.normalization import normalize_transactions
from app.services.budget_v2.parsers import extract_pdf_pages, parser_scorecard
from app.services.budget_v2.recurrence import assess_recurrence
from app.services.budget_v2.resolver import resolve_transaction
from app.services.budget_v2.totals import (
    compute_budget_model_totals,
    compute_statement_totals,
    compute_statement_window_days,
)
from app.services.budget_v2.transfer_detector import assess_transfer
from app.services.budget_v2.trust import assess_import_trust
from app.services.budget_v2.types import BudgetLine, ClassifiedTransaction, ParsedTransaction

logger = logging.getLogger(__name__)

RECONCILIATION_TOLERANCE = Decimal("0.05")
STATEMENT_TOTAL_TOLERANCE = Decimal("1.00")


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _signed_amount_from_direction(*, amount: Decimal, direction: str) -> Decimal:
    absolute = abs(amount)
    return absolute if direction == "credit" else absolute * Decimal("-1")


def _strict_movement_type(
    *,
    normalized_description: str | None,
    merchant_candidate: str | None,
    existing: object | None,
    direction: str,
) -> str:
    if existing:
        return str(existing)

    text = " ".join(
        part.strip().lower()
        for part in [normalized_description or "", merchant_candidate or ""]
        if part and part.strip()
    )

    if any(token in text for token in ["refund", "reversal", "chargeback"]):
        return "refund"
    if any(token in text for token in ["credit card payment", "cc payment", "loan repayment", "mortgage repayment"]):
        return "debt_payment"
    if any(token in text for token in ["transfer", "payment to", "payment from", "internal", "to acct", "from acct"]):
        return "internal_transfer"
    if any(token in text for token in ["atm", "cash withdrawal"]):
        return "cash_withdrawal"
    if any(token in text for token in ["fee", "account fee", "monthly fee", "service fee", "overdraft fee"]):
        return "fee"
    if direction == "credit":
        return "income"
    if direction == "debit":
        return "expense"
    return MovementType.OTHER_NEEDS_REVIEW


def _ensure_normalized_metadata(
    *,
    parsed_row: BudgetParsedTransaction,
    normalized_item,
) -> dict[str, object]:
    metadata = dict(normalized_item.metadata or {})
    metadata["signed_amount"] = _signed_amount_from_direction(
        amount=normalized_item.amount,
        direction=normalized_item.direction,
    )
    metadata["merchant_fingerprint"] = normalized_item.merchant_fingerprint or merchant_fingerprint(
        normalized_item.merchant_candidate
    )
    metadata["merchant_base_name"] = metadata.get("merchant_base_name") or normalized_item.merchant_candidate
    metadata["movement_type"] = _strict_movement_type(
        normalized_description=normalized_item.normalized_description,
        merchant_candidate=normalized_item.merchant_candidate,
        existing=metadata.get("movement_type"),
        direction=normalized_item.direction,
    )
    metadata["row_index"] = normalized_item.row_index
    metadata["parsed_direction"] = parsed_row.direction
    metadata["absolute_amount"] = abs(normalized_item.amount)
    return metadata


def _extract_signed_amount(*, normalized_meta: dict[str, object], parsed: BudgetParsedTransaction) -> Decimal:
    signed_amount_raw = normalized_meta.get("signed_amount")
    if signed_amount_raw is not None:
        try:
            return Decimal(str(signed_amount_raw))
        except (InvalidOperation, ValueError, TypeError):
            pass
    return _signed_amount_from_direction(amount=parsed.amount, direction=parsed.direction)


def _statement_truth_inputs_from_parsed_rows(
    *,
    parsed_rows: list[BudgetParsedTransaction],
) -> tuple[Decimal, Decimal, int, int]:
    parsed_debit_total = Decimal("0.00")
    parsed_credit_total = Decimal("0.00")
    parsed_debit_count = 0
    parsed_credit_count = 0

    for row in parsed_rows:
        if row.direction == "credit":
            parsed_credit_total += abs(row.amount)
            parsed_credit_count += 1
        else:
            parsed_debit_total += abs(row.amount)
            parsed_debit_count += 1

    return (
        parsed_debit_total.quantize(Decimal("0.01")),
        parsed_credit_total.quantize(Decimal("0.01")),
        parsed_debit_count,
        parsed_credit_count,
    )


def _overlaps_dates(
    start_a: date | None,
    end_a: date | None,
    start_b: date | None,
    end_b: date | None,
) -> bool:
    if not all(isinstance(value, date) for value in (start_a, end_a, start_b, end_b)):
        return False
    return max(start_a, start_b) <= min(end_a, end_b)


async def _detect_overlap_status(
    *,
    session,
    organization_id: UUID,
    import_id: UUID,
    sha256: str,
    source_bank: str | None,
    account_ref_masked: str | None,
    statement_start_date: date | None,
    statement_end_date: date | None,
    row_hashes: set[str],
) -> dict[str, object]:
    raw_match_count = len(
        list(
            await session.exec(
                select(BudgetRawFile)
                .where(col(BudgetRawFile.organization_id) == organization_id)
                .where(col(BudgetRawFile.sha256) == sha256)
                .where(col(BudgetRawFile.import_session_id) != import_id)
            )
        )
    )
    previous_statements = list(
        await session.exec(
            select(BudgetParsedStatement)
            .where(col(BudgetParsedStatement.organization_id) == organization_id)
            .where(col(BudgetParsedStatement.import_session_id) != import_id)
        )
    )
    overlap_import_ids: set[UUID] = set()
    for statement in previous_statements:
        same_account = bool(account_ref_masked and statement.account_ref_masked and statement.account_ref_masked == account_ref_masked)
        same_bank = bool(source_bank and statement.bank_name and source_bank == statement.bank_name)
        if (same_account or same_bank) and _overlaps_dates(
            statement_start_date,
            statement_end_date,
            statement.statement_start_date,
            statement.statement_end_date,
        ):
            overlap_import_ids.add(statement.import_session_id)

    duplicate_row_hashes = 0
    if row_hashes:
        previous_norm_rows = list(
            await session.exec(
                select(BudgetNormalizedTransaction)
                .where(col(BudgetNormalizedTransaction.organization_id) == organization_id)
                .where(col(BudgetNormalizedTransaction.import_session_id) != import_id)
                .where(col(BudgetNormalizedTransaction.row_hash).in_(list(row_hashes)))
            )
        )
        duplicate_row_hashes = len({row.row_hash for row in previous_norm_rows})

    if raw_match_count > 0:
        status = "exact_duplicate"
    elif overlap_import_ids or duplicate_row_hashes:
        status = "overlap_detected"
    else:
        status = "clear"
    return {
        "overlap_status": status,
        "duplicate_import_count": raw_match_count,
        "overlap_import_ids": [str(item) for item in sorted(overlap_import_ids)],
        "duplicate_rows_detected": duplicate_row_hashes,
    }


async def _historical_recurrence_map(
    *,
    session,
    organization_id: UUID,
    import_id: UUID,
) -> dict[str, dict[str, object]]:
    previous_class_rows = list(
        await session.exec(
            select(BudgetTransactionClassification, BudgetNormalizedTransaction)
            .join(
                BudgetNormalizedTransaction,
                col(BudgetTransactionClassification.normalized_transaction_id) == col(BudgetNormalizedTransaction.id),
            )
            .where(col(BudgetTransactionClassification.organization_id) == organization_id)
            .where(col(BudgetTransactionClassification.import_session_id) != import_id)
            .where(col(BudgetNormalizedTransaction.organization_id) == organization_id)
        )
    )
    history: dict[str, dict[str, object]] = defaultdict(lambda: {"occurrence_count": 0, "amounts": [], "imports": set()})
    for classification, normalized in previous_class_rows:
        normalized_meta = normalized.extra_data if isinstance(normalized.extra_data, dict) else {}
        classification_meta = classification.extra_data if isinstance(classification.extra_data, dict) else {}
        merchant_fp = str(normalized_meta.get("merchant_fingerprint") or "")
        recurrence_key = str(
            classification_meta.get("recurrence_key")
            or "|".join(
                [
                    merchant_fp,
                    classification.category,
                    classification.subcategory,
                    str(classification_meta.get("movement_type", "")),
                ]
            )
        )
        signed_amount = normalized_meta.get("signed_amount")
        if not recurrence_key or signed_amount is None:
            continue
        for key in filter(None, {recurrence_key, merchant_fp.upper()}):
            entry = history[key]
            entry["occurrence_count"] = int(entry["occurrence_count"]) + 1
            entry["amounts"].append(abs(Decimal(str(signed_amount))))
            entry["imports"].add(classification.import_session_id)
    for value in history.values():
        value["import_count"] = len(value["imports"])
        value["imports"] = sorted(str(item) for item in value["imports"])
    return dict(history)


def _resolved_to_classified(resolved) -> ClassifiedTransaction:
    group_base = canonicalize_merchant_descriptor(resolved.merchant_candidate or resolved.normalized_description) or (
        resolved.merchant_candidate or resolved.normalized_description
    )
    group_key = canonical_group_key(resolved.category, resolved.subcategory, group_base)
    return ClassifiedTransaction(
        row_index=resolved.row_index,
        transaction_date=resolved.transaction_date,
        amount=resolved.amount,
        signed_amount=resolved.signed_amount,
        direction=resolved.direction,
        raw_description=resolved.raw_description,
        normalized_description=resolved.normalized_description,
        payment_rail=resolved.payment_rail,
        merchant_candidate=group_base,
        merchant_base_name=resolved.merchant_base_name,
        merchant_fingerprint=resolved.merchant_fingerprint,
        interpretation_type=resolved.interpretation_type,
        interpretation_confidence=resolved.interpretation_confidence,
        interpretation_reason=resolved.interpretation_reason,
        classification_type=resolved.classification_type,
        category=resolved.category,
        subcategory=resolved.subcategory,
        confidence=resolved.confidence,
        explanation="; ".join(resolved.reasons) if resolved.reasons else resolved.interpretation_reason,
        evidence_source=resolved.evidence_source,
        group_key=group_key,
        movement_type=resolved.movement_type,
        inferred_cadence=resolved.inferred_cadence,
        cadence_confidence=resolved.cadence_confidence,
        cadence_reason=resolved.cadence_reason,
        impact_on_baseline=resolved.impact_on_baseline,
        included=resolved.included,
        observed_only=resolved.observed_only,
        review_reasons=list(dict.fromkeys(resolved.review_flags)),
        metadata={
            "bucket_assignment": resolved.bucket_assignment,
            **(resolved.metadata or {}),
            "reasons": list(dict.fromkeys(resolved.reasons)),
            "review_flags": list(dict.fromkeys(resolved.review_flags)),
        },
    )


def _apply_line_bucket_state(line: BudgetLine, bucket_assignment: str) -> None:
    line.bucket_assignment = bucket_assignment
    line.metadata = {
        **(line.metadata or {}),
        "final_bucket": _final_bucket_for_bucket_assignment(bucket_assignment),
    }
    if bucket_assignment in {BucketAssignment.RECURRING_BASELINE, BucketAssignment.INCOME_RECURRING}:
        recurring_period = line.inferred_cadence if line.inferred_cadence in {"weekly", "fortnightly", "monthly", "quarterly", "yearly"} else (
            line.base_period
        )
        recurring_amount = line.base_amount
        if recurring_amount <= Decimal("0.00"):
            recurring_amount = line.observed_amount
        apply_line_source(line, amount=recurring_amount, period=recurring_period)
        line.observed_only = False
        line.is_modeled = True
        line.modeled_by_default = False
        line.modeling_status = ModelingState.USER_FORCED_RECURRING
        line.impact_on_baseline = "included"
        line.included = True
        return
    line.observed_only = True
    line.is_modeled = False
    line.modeled_by_default = False
    line.modeling_status = ModelingState.OBSERVATIONAL_ONLY
    line.impact_on_baseline = "excluded" if bucket_assignment == BucketAssignment.TRANSFER_MONEY_MOVEMENT else "included"
    line.included = True
    if bucket_assignment in {
        BucketAssignment.ONE_OFF_EXCEPTIONAL,
        BucketAssignment.TRANSFER_MONEY_MOVEMENT,
        BucketAssignment.INCOME_IRREGULAR,
    }:
        line.normalized_weekly = Decimal("0.00")
        line.normalized_fortnightly = Decimal("0.00")
        line.normalized_monthly = Decimal("0.00")
        line.normalized_yearly = Decimal("0.00")


def _classification_updates_for_bucket(*, bucket_assignment: str) -> dict[str, object]:
    updates: dict[str, object] = {"bucket_assignment": bucket_assignment, "included": True}
    if bucket_assignment in {BucketAssignment.RECURRING_BASELINE, BucketAssignment.INCOME_RECURRING}:
        updates["observed_only"] = False
        updates["impact_on_baseline"] = "included"
    elif bucket_assignment == BucketAssignment.TRANSFER_MONEY_MOVEMENT:
        updates["observed_only"] = True
        updates["impact_on_baseline"] = "excluded"
    else:
        updates["observed_only"] = True
        updates["impact_on_baseline"] = "included"
    return updates


def _manual_bucket_for_recurring(*, category: str, line_type: str | None = None) -> str:
    if category == "Income" or line_type == "income":
        return BucketAssignment.INCOME_RECURRING
    return BucketAssignment.RECURRING_BASELINE


def _manual_bucket_for_one_off(*, category: str, line_type: str | None = None) -> str:
    if category == "Income" or line_type == "income":
        return BucketAssignment.INCOME_IRREGULAR
    return BucketAssignment.ONE_OFF_EXCEPTIONAL


def _final_bucket_for_bucket_assignment(bucket_assignment: str) -> str:
    if bucket_assignment == BucketAssignment.RECURRING_BASELINE:
        return "recurring_baseline_expenses"
    if bucket_assignment in {BucketAssignment.INCOME_RECURRING, BucketAssignment.INCOME_IRREGULAR}:
        return "income"
    if bucket_assignment == BucketAssignment.ONE_OFF_EXCEPTIONAL:
        return "one_off_spending"
    if bucket_assignment == BucketAssignment.TRANSFER_MONEY_MOVEMENT:
        return "transfers"
    return "variable_spending"


def _to_json_safe(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _to_json_safe(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_to_json_safe(item) for item in value]
    if isinstance(value, list):
        return [_to_json_safe(item) for item in value]
    return value


def _compute_reconciliation(
    *,
    opening_balance: Decimal | None,
    closing_balance: Decimal | None,
    parsed_debit_total: Decimal,
    parsed_credit_total: Decimal,
) -> tuple[str, Decimal, Decimal | None]:
    if opening_balance is None or closing_balance is None:
        return ReconciliationStatus.UNKNOWN, Decimal("0.00"), None
    expected = opening_balance + parsed_credit_total - parsed_debit_total
    difference = (closing_balance - expected).quantize(Decimal("0.01"))
    if abs(difference) <= RECONCILIATION_TOLERANCE:
        return ReconciliationStatus.RECONCILED, difference, expected
    return ReconciliationStatus.FAILED_RECONCILIATION, difference, expected


def _classify_reconciliation(
    *,
    metadata: dict[str, object],
    opening_balance: Decimal | None,
    closing_balance: Decimal | None,
    parsed_debit_total: Decimal,
    parsed_credit_total: Decimal,
) -> tuple[str, Decimal, Decimal | None, str, list[str]]:
    base_status, difference, expected = _compute_reconciliation(
        opening_balance=opening_balance,
        closing_balance=closing_balance,
        parsed_debit_total=parsed_debit_total,
        parsed_credit_total=parsed_credit_total,
    )
    statement_total_debits = metadata.get("statement_total_debits")
    statement_total_credits = metadata.get("statement_total_credits")
    document_type = str(metadata.get("document_type") or "unknown")
    document_reconcilable = metadata.get("document_reconcilable")
    text_extraction_length = int(metadata.get("text_extraction_length", 0) or 0)

    warning_reasons: list[str] = []
    if text_extraction_length and text_extraction_length < 500 and parsed_debit_total == Decimal("0.00") and parsed_credit_total == Decimal("0.00"):
        return (
            ReconciliationStatus.EXTRACTION_DEGRADED,
            difference,
            expected,
            "Text extraction quality is too weak for reliable parsing. Imported totals may be incomplete and may require OCR or a different extraction path.",
            ["extraction_quality_low"],
        )

    totals_match = False
    totals_available = statement_total_debits is not None and statement_total_credits is not None
    if totals_available:
        debit_diff = abs(Decimal(str(statement_total_debits)) - parsed_debit_total)
        credit_diff = abs(Decimal(str(statement_total_credits)) - parsed_credit_total)
        totals_match = debit_diff <= STATEMENT_TOTAL_TOLERANCE and credit_diff <= STATEMENT_TOTAL_TOLERANCE
        if not totals_match:
            warning_reasons.append("statement_totals_mismatch")
            warning_reasons.append("parser_coverage_gap")
            return (
                ReconciliationStatus.PARSER_INCOMPLETE,
                difference,
                expected,
                (
                    "Imported transactions do not reconcile to the statement totals. "
                    f"Difference to closing balance: {abs(difference):.2f}. Some rows may be missing, duplicated, truncated, or misclassified."
                ),
                warning_reasons,
            )

    if base_status == ReconciliationStatus.RECONCILED:
        return ReconciliationStatus.RECONCILED, difference, expected, "Imported transactions reconcile to the statement closing balance.", []

    if base_status == ReconciliationStatus.UNKNOWN:
        return (
            ReconciliationStatus.UNKNOWN,
            difference,
            expected,
            "Reconciliation could not be completed because opening/closing balances or statement totals were unavailable.",
            ["reconciliation_inputs_missing"],
        )

    if totals_available and totals_match and document_reconcilable is False:
        warning_reasons.append("provisional_document_non_reconcilable")
        return (
            ReconciliationStatus.SOURCE_NON_RECONCILABLE,
            difference,
            expected,
            (
                "This import is based on a provisional transaction listing. Parsed totals align with the document totals, "
                f"but the document does not reconcile from opening to closing balance. Difference: {abs(difference):.2f}."
            ),
            warning_reasons,
        )

    if totals_available and totals_match:
        warning_reasons.append("balance_totals_disagree")
        return (
            ReconciliationStatus.SOURCE_NON_RECONCILABLE if document_type == "transaction_listing" else ReconciliationStatus.FAILED_RECONCILIATION,
            difference,
            expected,
            (
                "Parsed debit and credit totals align with the document summary, but the document does not reconcile from "
                f"opening to closing balance. Difference: {abs(difference):.2f}."
            ),
            warning_reasons,
        )

    return (
        ReconciliationStatus.FAILED_RECONCILIATION,
        difference,
        expected,
        "Imported transactions could not be fully reconciled because the statement summary totals were unavailable.",
        ["reconciliation_inputs_missing"],
    )


def _coverage_warnings(
    *,
    parser_name: str,
    metadata: dict[str, object],
    transactions: list[ParsedTransaction],
    parsed_debit_total: Decimal,
    parsed_credit_total: Decimal,
    parsed_credit_count: int,
    parsed_debit_count: int,
    reconciliation_status: str,
    reconciliation_reason: str,
) -> list[str]:
    warnings: list[str] = []
    statement_total_credits = metadata.get("statement_total_credits")
    statement_total_debits = metadata.get("statement_total_debits")
    table_header_count = int(metadata.get("table_header_count", 0) or 0)
    page_count = int(metadata.get("page_count", 1) or 1)
    table_detected = bool(metadata.get("table_detected"))
    parser_failure_reason = str(metadata.get("parser_failure_reason") or "")
    text_extraction_length = int(metadata.get("text_extraction_length", 0) or 0)
    parser_flags_raw = metadata.get("parser_flags", [])
    parser_flags = {str(flag) for flag in parser_flags_raw} if isinstance(parser_flags_raw, list) else set()
    page_row_counts_raw = metadata.get("page_row_counts", {})
    if isinstance(page_row_counts_raw, dict):
        populated_pages = sum(1 for value in page_row_counts_raw.values() if int(value or 0) > 0)
    else:
        populated_pages = 0

    statement_credit_matches = False
    statement_debit_matches = False

    if statement_total_credits is not None:
        statement_credit_value = Decimal(str(statement_total_credits))
        if statement_credit_value > Decimal("0") and parsed_credit_count == 0:
            warnings.append("Statement summary shows credits, but parsed credit rows are missing. Parser likely incomplete.")
        elif statement_credit_value > Decimal("0") and abs(statement_credit_value - parsed_credit_total) > STATEMENT_TOTAL_TOLERANCE:
            warnings.append("Parsed credit total does not align with statement summary credits. Review parser coverage.")
        else:
            statement_credit_matches = True

    if statement_total_debits is not None:
        statement_debit_value = Decimal(str(statement_total_debits))
        if statement_debit_value > Decimal("0") and abs(statement_debit_value - parsed_debit_total) > STATEMENT_TOTAL_TOLERANCE:
            warnings.append("Parsed debit total does not align with statement summary debits. Review parser coverage.")
        else:
            statement_debit_matches = True

    totals_verified = (
        statement_total_credits is not None
        and statement_total_debits is not None
        and statement_credit_matches
        and statement_debit_matches
    )
    coverage_verified = (
        reconciliation_status == ReconciliationStatus.RECONCILED
        and table_detected
        and not parser_failure_reason
        and populated_pages >= page_count
        and (totals_verified or table_header_count >= page_count or "compact_nz_layout_fallback" in parser_flags)
    )

    if page_count > 1 and table_header_count < page_count and not coverage_verified:
        warnings.append("Multi-page transaction table detected; one or more pages may not have been parsed fully.")

    if page_count > 1 and len(transactions) < 25:
        warnings.append("Parsed transaction count is unexpectedly low for a multi-page statement.")

    if parsed_debit_count == 0 and parsed_credit_count == 0:
        if text_extraction_length < 500:
            warnings.append("PDF text extraction produced very little usable text. This may require OCR or a different extraction path.")
        elif table_detected:
            warnings.append("Parser found the transaction table but failed to traverse transaction rows. Review parser flow diagnostics.")
        else:
            warnings.append(f"No transaction rows were parsed from {parser_name}.")

    if parser_failure_reason:
        warnings.append(f"Parser flow warning: {parser_failure_reason}.")

    if reconciliation_status in {
        ReconciliationStatus.PARSER_INCOMPLETE,
        ReconciliationStatus.SOURCE_NON_RECONCILABLE,
        ReconciliationStatus.UNKNOWN,
        ReconciliationStatus.EXTRACTION_DEGRADED,
    } and reconciliation_reason:
        warnings.append(reconciliation_reason)

    return warnings


async def run_import(*, session, organization_id: UUID, filename: str, content_type: str | None, raw_bytes: bytes) -> BudgetImportSession:
    import_session = await crud.create(
        session,
        BudgetImportSession,
        organization_id=organization_id,
        status="processing",
        source_format="pdf",
        parser_warnings=[],
        extracted_debit_total=Decimal("0"),
        extracted_credit_total=Decimal("0"),
        reconciliation_difference=Decimal("0"),
    )

    page_texts = extract_pdf_pages(raw_bytes)
    text = "\n".join(page_texts)
    digest = hashlib.sha256(raw_bytes).hexdigest()

    await crud.create(
        session,
        BudgetRawFile,
        organization_id=organization_id,
        import_session_id=import_session.id,
        filename=filename,
        content_type=content_type,
        byte_size=len(raw_bytes),
        sha256=digest,
        raw_text=text[:2_000_000],
    )

    scorecard = parser_scorecard(text, filename)
    parser = scorecard[0][0]
    parser_score = scorecard[0][1]
    parsed_statement = parser.parse(text, filename)

    parsed_statement.metadata = {
        **parsed_statement.metadata,
        "text_extraction_length": len(text),
        "page_text_lengths": [len(page_text) for page_text in page_texts],
        "empty_text_pages": [idx + 1 for idx, page_text in enumerate(page_texts) if not page_text.strip()],
        "low_text_pages": [idx + 1 for idx, page_text in enumerate(page_texts) if 0 < len(page_text.strip()) < 120],
        "parser_score": parser_score,
        "coverage_estimate": 0.9 if parsed_statement.parser_confidence >= 0.8 else 0.7 if parsed_statement.parser_confidence >= 0.6 else 0.45,
        "suspected_missing_pages": bool(any(not page_text.strip() for page_text in page_texts)),
        "parser_scorecard": [{"name": candidate.name, "score": score} for candidate, score in scorecard],
    }

    parsed_statement_row = await crud.create(
        session,
        BudgetParsedStatement,
        organization_id=organization_id,
        import_session_id=import_session.id,
        statement_id=parsed_statement.statement_id,
        bank_name=parsed_statement.bank_name,
        account_name=parsed_statement.account_name,
        account_ref_masked=parsed_statement.account_ref_masked,
        statement_start_date=parsed_statement.statement_start_date,
        statement_end_date=parsed_statement.statement_end_date,
        parser_name=parsed_statement.parser_name,
        parser_confidence=parsed_statement.parser_confidence,
        parser_flags=parsed_statement.parser_flags,
        extra_data=_to_json_safe(parsed_statement.metadata),
    )

    parsed_rows: list[BudgetParsedTransaction] = []
    for tx in parsed_statement.transactions:
        parsed_rows.append(
            await crud.create(
                session,
                BudgetParsedTransaction,
                organization_id=organization_id,
                import_session_id=import_session.id,
                parsed_statement_id=parsed_statement_row.id,
                row_index=tx.row_index,
                transaction_date=tx.transaction_date,
                effective_date=tx.effective_date,
                amount=abs(tx.amount),
                direction=tx.direction,
                raw_description=tx.raw_description,
                raw_reference=tx.raw_reference,
                balance_after=tx.balance_after,
                page_number=tx.page_number,
                source_line_refs=tx.source_line_refs,
                parser_flags=tx.parser_flags,
                parser_confidence=tx.parser_confidence,
            )
        )

    (
        debit_total,
        credit_total,
        parsed_debit_count,
        parsed_credit_count,
    ) = _statement_truth_inputs_from_parsed_rows(parsed_rows=parsed_rows)

    normalized = normalize_transactions(parsed_statement.transactions)

    overlap_status = await _detect_overlap_status(
        session=session,
        organization_id=organization_id,
        import_id=import_session.id,
        sha256=digest,
        source_bank=parsed_statement.bank_name,
        account_ref_masked=parsed_statement.account_ref_masked,
        statement_start_date=parsed_statement.statement_start_date,
        statement_end_date=parsed_statement.statement_end_date,
        row_hashes={item.row_hash for item in normalized},
    )

    parsed_statement.metadata = {
        **parsed_statement.metadata,
        **overlap_status,
    }
    await crud.patch(
        session,
        parsed_statement_row,
        {"extra_data": _to_json_safe(parsed_statement.metadata)},
    )

    row_to_normalized: dict[int, BudgetNormalizedTransaction] = {}
    normalized_by_row_index = {item.row_index: item for item in normalized}
    parsed_by_row_index = {row.row_index: row for row in parsed_rows}

    for item in normalized:
        source_row = parsed_by_row_index[item.row_index]
        strict_metadata = _ensure_normalized_metadata(
            parsed_row=source_row,
            normalized_item=item,
        )
        row = await crud.create(
            session,
            BudgetNormalizedTransaction,
            organization_id=organization_id,
            import_session_id=import_session.id,
            parsed_transaction_id=source_row.id,
            normalized_description=item.normalized_description,
            payment_rail=item.payment_rail,
            merchant_candidate=item.merchant_candidate,
            reference=item.reference,
            row_hash=item.row_hash,
            dedupe_rank=0,
            extra_data=_to_json_safe(strict_metadata),
        )
        row_to_normalized[item.row_index] = row

    memory_rows = list(
        await session.exec(
            select(BudgetMerchantMemory)
            .where(col(BudgetMerchantMemory.organization_id) == organization_id)
            .where(col(BudgetMerchantMemory.active) == True)  # noqa: E712
        )
    )
    memory_map: dict[str, BudgetMerchantMemory] = {}
    for mem in memory_rows:
        key = mem.merchant_fingerprint or mem.merchant_key.upper()
        memory_map[key] = mem

    historical_recurrence = await _historical_recurrence_map(
        session=session,
        organization_id=organization_id,
        import_id=import_session.id,
    )

    statement_window_days = compute_statement_window_days(
        statement_start_date=parsed_statement.statement_start_date,
        statement_end_date=parsed_statement.statement_end_date,
    )

    memory_hints: dict[int, object] = {}
    for item in normalized:
        fingerprint = item.merchant_fingerprint or merchant_fingerprint(item.merchant_candidate)
        mem = memory_map.get(fingerprint) if fingerprint else None
        if mem is not None:
            remapped_category, remapped_subcategory = canonicalize_expense_taxonomy(
                mem.category,
                mem.subcategory,
                item.merchant_candidate or item.normalized_description,
            )
            if (remapped_category, remapped_subcategory) != (mem.category, mem.subcategory):
                await crud.patch(
                    session,
                    mem,
                    {"category": remapped_category, "subcategory": remapped_subcategory, "updated_at": utcnow()},
                )
                mem.category = remapped_category
                mem.subcategory = remapped_subcategory
        memory_hints[item.row_index] = build_memory_hint(tx=item, memory_row=mem)

    transfer_assessments = {
        item.row_index: assess_transfer(tx=item, memory=memory_hints[item.row_index])
        for item in normalized
    }

    income_assessments = assess_income_transactions(
        transactions=normalized,
        transfer_assessments=transfer_assessments,
        memory_hints=memory_hints,
        statement_window_days=statement_window_days,
        historical_recurrence=historical_recurrence,
    )

    recurrence_assessments = assess_recurrence(
        transactions=normalized,
        memory_hints=memory_hints,
        statement_window_days=statement_window_days,
        historical_recurrence=historical_recurrence,
    )

    classified: list[ClassifiedTransaction] = []
    resolved_transactions = []

    for item in normalized:
        transfer = transfer_assessments[item.row_index]
        memory = memory_hints[item.row_index]
        income = income_assessments.get(item.row_index)
        recurrence = recurrence_assessments.get(item.row_index)

        expense = None
        if item.direction == "debit":
            expense = classify_expense(
                tx=item,
                transfer=transfer,
                recurrence=recurrence,
                memory=memory,
            )

        resolved = resolve_transaction(
            tx=item,
            memory=memory,
            transfer=transfer,
            income=income,
            recurrence=recurrence,
            expense=expense,
        )

        recurrence_key = "|".join(
            [
                str(item.merchant_fingerprint or ""),
                resolved.category,
                resolved.subcategory,
                resolved.movement_type,
            ]
        )

        resolved.metadata = {
            **(resolved.metadata or {}),
            "recurrence_key": recurrence_key,
            "merchant_confidence": resolved.confidence,
            "bucket_confidence": resolved.confidence,
            "movement_type": resolved.movement_type or _strict_movement_type(
                normalized_description=item.normalized_description,
                merchant_candidate=item.merchant_candidate,
                existing=None,
                direction=item.direction,
            ),
            "signed_amount": _signed_amount_from_direction(amount=item.amount, direction=item.direction),
        }

        resolved_transactions.append(resolved)
        classified.append(_resolved_to_classified(resolved))

    for classified_item in classified:
        item = normalized_by_row_index[classified_item.row_index]
        fingerprint = item.merchant_fingerprint or merchant_fingerprint(item.merchant_candidate)
        normalized_row = row_to_normalized[item.row_index]

        await crud.create(
            session,
            BudgetTransactionClassification,
            organization_id=organization_id,
            import_session_id=import_session.id,
            normalized_transaction_id=normalized_row.id,
            interpretation_type=classified_item.interpretation_type,
            interpretation_confidence=classified_item.interpretation_confidence,
            interpretation_reason=classified_item.interpretation_reason,
            classification_type=classified_item.classification_type,
            category=classified_item.category,
            subcategory=classified_item.subcategory,
            confidence=classified_item.confidence,
            explanation=classified_item.explanation,
            evidence_source=classified_item.evidence_source,
            group_key=classified_item.group_key,
            inferred_cadence=classified_item.inferred_cadence,
            cadence_confidence=classified_item.cadence_confidence,
            cadence_reason=classified_item.cadence_reason,
            impact_on_baseline=classified_item.impact_on_baseline,
            included=classified_item.included,
            observed_only=classified_item.observed_only,
            review_reasons=classified_item.review_reasons,
            extra_data=_to_json_safe(classified_item.metadata),
        )

        if (
            item.merchant_candidate
            and classified_item.interpretation_type == "merchant_expense"
            and should_promote_memory(classified_item.confidence, classified_item.evidence_source)
            and classified_item.category not in {"Transfer / Money Movement", "Cash Withdrawal"}
            and "unknown_merchant" not in classified_item.review_reasons
        ):
            if fingerprint and fingerprint not in memory_map:
                memory_map[fingerprint] = await crud.create(
                    session,
                    BudgetMerchantMemory,
                    organization_id=organization_id,
                    merchant_key=item.merchant_candidate,
                    merchant_fingerprint=fingerprint,
                    category=classified_item.category,
                    subcategory=classified_item.subcategory,
                    confidence=classified_item.confidence,
                    source="auto_high_confidence",
                    active=True,
                )

    lines = build_lines(
        classified,
        historical_recurrence=historical_recurrence,
        statement_window_days=statement_window_days,
    )

    for line in lines:
        await crud.create(
            session,
            BudgetLineItem,
            organization_id=organization_id,
            import_session_id=import_session.id,
            group_key=line.group_key,
            group_label=line.group_label,
            line_type=line.line_type,
            category=line.category,
            subcategory=line.subcategory,
            inferred_cadence=line.inferred_cadence,
            cadence_confidence=line.cadence_confidence,
            cadence_reason=line.cadence_reason,
            observed_only=line.observed_only,
            bucket_assignment=line.bucket_assignment,
            base_amount=line.base_amount,
            base_period=line.base_period,
            authoritative_field=line.authoritative_field,
            source_amount=line.source_amount,
            source_period=line.source_period,
            observed_window_total=line.observed_window_total,
            normalized_weekly=line.normalized_weekly,
            normalized_fortnightly=line.normalized_fortnightly,
            normalized_monthly=line.normalized_monthly,
            normalized_yearly=line.normalized_yearly,
            reserve_monthly_equivalent=line.reserve_monthly_equivalent,
            impact_on_baseline=line.impact_on_baseline,
            included=line.included,
            confidence=float(line.confidence),
            explanation=line.explanation,
            notes=line.notes,
            review_reasons=line.review_reasons,
            transaction_count=line.transaction_count,
            extra_data=_to_json_safe({"row_indexes": line.row_indexes, **line.metadata}),
        )

    metadata = parsed_statement.metadata if isinstance(parsed_statement.metadata, dict) else {}
    opening_balance = _decimal_or_none(metadata.get("opening_balance"))
    closing_balance = _decimal_or_none(metadata.get("closing_balance"))

    reconciliation_status, reconciliation_difference, expected_closing_balance, reconciliation_reason, warning_reasons = _classify_reconciliation(
        metadata=metadata,
        opening_balance=opening_balance,
        closing_balance=closing_balance,
        parsed_debit_total=debit_total,
        parsed_credit_total=credit_total,
    )

    logger.info(
        "budget_v2_reconciliation",
        extra={
            "import_session_id": str(import_session.id),
            "parser_name": parsed_statement.parser_name,
            "parser_score": parser_score,
            "document_type": metadata.get("document_type"),
            "document_reconcilable": metadata.get("document_reconcilable"),
            "text_extraction_length": metadata.get("text_extraction_length"),
            "table_detected": metadata.get("table_detected"),
            "page_resume_count": metadata.get("page_resume_count"),
            "first_stop_reason": metadata.get("first_stop_reason"),
            "first_stop_line": metadata.get("first_stop_line"),
            "opening_balance": str(opening_balance) if opening_balance is not None else None,
            "closing_balance": str(closing_balance) if closing_balance is not None else None,
            "statement_total_debits": str(metadata.get("statement_total_debits")) if metadata.get("statement_total_debits") is not None else None,
            "statement_total_credits": str(metadata.get("statement_total_credits")) if metadata.get("statement_total_credits") is not None else None,
            "parsed_debit_total": str(debit_total),
            "parsed_credit_total": str(credit_total),
            "expected_closing_balance": str(expected_closing_balance) if expected_closing_balance is not None else None,
            "reconciliation_status": reconciliation_status,
            "reconciliation_difference": str(reconciliation_difference),
            "warning_reasons": warning_reasons,
            "page_row_counts": metadata.get("page_row_counts"),
            "excluded_rows_count": len(metadata.get("excluded_rows", [])),
            "suspicious_rows_count": len(metadata.get("suspicious_rows", [])),
        },
    )

    coverage_warnings = _coverage_warnings(
        parser_name=parsed_statement.parser_name,
        metadata=metadata,
        transactions=parsed_statement.transactions,
        parsed_debit_total=debit_total,
        parsed_credit_total=credit_total,
        parsed_credit_count=parsed_credit_count,
        parsed_debit_count=parsed_debit_count,
        reconciliation_status=reconciliation_status,
        reconciliation_reason=reconciliation_reason,
    )

    parsed_statement_extra = dict(metadata)
    parsed_statement_extra.update(
        {
            "parsed_debit_count": parsed_debit_count,
            "parsed_credit_count": parsed_credit_count,
            "parsed_debit_total": debit_total,
            "parsed_credit_total": credit_total,
            "expected_closing_balance": expected_closing_balance,
            "reconciliation_status": reconciliation_status,
            "reconciliation_difference": reconciliation_difference,
            "reconciliation_reason": reconciliation_reason,
            "warning_reasons": warning_reasons,
            "parser_coverage_warnings": coverage_warnings,
        }
    )

    await crud.patch(
        session,
        parsed_statement_row,
        {
            "extra_data": _to_json_safe(parsed_statement_extra),
        },
    )

    aggregate = aggregate_sections(resolved_transactions)
    diagnostics = build_diagnostics(
        transactions=resolved_transactions,
        aggregate=aggregate,
        total_credits=credit_total,
        total_debits=debit_total,
    )

    low_confidence_count = len([item for item in resolved_transactions if item.confidence < BALANCED_REVIEW_CONFIDENCE_THRESHOLD])
    review_count = len([item for item in resolved_transactions if item.review_flags or item.final_bucket == "uncategorized"])

    import_session = await crud.patch(
        session,
        import_session,
        {
            "status": "completed",
            "source_bank": parsed_statement.bank_name,
            "parser_name": parsed_statement.parser_name,
            "parser_confidence": parsed_statement.parser_confidence,
            "parser_warnings": sorted(set([*parsed_statement.parser_flags, *coverage_warnings])),
            "statement_start_date": parsed_statement.statement_start_date,
            "statement_end_date": parsed_statement.statement_end_date,
            "transaction_count": len(parsed_statement.transactions),
            "extracted_debit_total": debit_total,
            "extracted_credit_total": credit_total,
            "opening_balance": opening_balance,
            "closing_balance": closing_balance,
            "reconciliation_status": reconciliation_status,
            "reconciliation_difference": reconciliation_difference,
            "needs_review": review_count > 0,
            "low_confidence_group_count": low_confidence_count,
            "uncategorized_review_count": review_count,
            "updated_at": utcnow(),
        },
    )

    trust_assessment = assess_import_trust(
        import_row=import_session,
        parsed_meta=parsed_statement_extra,
        lines=lines,
    )

    statement_truth = compute_statement_totals(
        import_row=import_session,
        parsed_rows=parsed_rows,
        expected_closing_balance=expected_closing_balance,
        truth_trust_level=trust_assessment.truth_trust_level,
    )

    budget_model = compute_budget_model_totals(
        lines=lines,
        modeling_allowed=trust_assessment.modeling_allowed,
        modeling_restrictions=trust_assessment.modeling_restrictions,
        statement_window_days=compute_statement_window_days(
            statement_start_date=import_session.statement_start_date,
            statement_end_date=import_session.statement_end_date,
        ),
    )

    budget_model.classification_reconciliation_status = diagnostics.classification_reconciliation_status
    budget_model.classification_reconciliation_difference = diagnostics.classification_reconciliation_difference
    budget_model.classified_section_totals = aggregate.totals
    budget_model.section_confidence = diagnostics.section_confidence
    budget_model.statement_model_confidence = diagnostics.statement_model_confidence

    summary = build_snapshot_summary(lines)
    summary.update(
        {
            "monthly_recurring_income": budget_model.recurring_income_monthly,
            "monthly_recurring_baseline_expenses": budget_model.recurring_baseline_monthly,
            "monthly_variable_discretionary": budget_model.variable_discretionary_monthly,
            "observed_one_off_exceptional_total": budget_model.observed_one_off_total,
            "observed_transfer_total": budget_model.observed_transfer_total,
            "observed_irregular_income_total": budget_model.irregular_income_total,
            "net_recurring_monthly": budget_model.core_net,
            "net_observed_total": budget_model.observed_net,
            "core_monthly_baseline": budget_model.recurring_baseline_monthly,
            "observed_discretionary_monthly": budget_model.variable_discretionary_monthly,
            "annual_infrequent_oneoff_spend": budget_model.observed_one_off_total,
            "transfer_money_movement_spend": budget_model.observed_transfer_total,
            "total_income_monthly": budget_model.recurring_income_monthly,
            "total_expenses_monthly": budget_model.recurring_baseline_monthly + budget_model.variable_discretionary_monthly,
            "net_monthly": budget_model.observed_net,
            "statement_truth": asdict(statement_truth),
            "budget_model": asdict(budget_model),
            "trust": asdict(trust_assessment),
            "modeling_allowed": trust_assessment.modeling_allowed,
            "totals_trust_level": trust_assessment.truth_trust_level,
            "modeling_restrictions": list(trust_assessment.modeling_restrictions or []),
            "classified_section_totals": {key: value for key, value in aggregate.totals.items()},
            "classification_reconciliation_status": diagnostics.classification_reconciliation_status,
            "classification_reconciliation_difference": diagnostics.classification_reconciliation_difference,
            "section_confidence": diagnostics.section_confidence,
            "statement_model_confidence": diagnostics.statement_model_confidence,
            "review_queue": diagnostics.review_queue,
        }
    )

    await crud.create(
        session,
        BudgetSnapshot,
        organization_id=organization_id,
        import_session_id=import_session.id,
        summary=_to_json_safe(summary),
    )

    return import_session


async def _load_classified_transactions(
    *,
    session,
    organization_id: UUID,
    import_id: UUID,
) -> list[ClassifiedTransaction]:
    parsed_rows = list(
        await session.exec(
            select(BudgetParsedTransaction)
            .where(col(BudgetParsedTransaction.organization_id) == organization_id)
            .where(col(BudgetParsedTransaction.import_session_id) == import_id)
        )
    )
    normalized_rows = list(
        await session.exec(
            select(BudgetNormalizedTransaction)
            .where(col(BudgetNormalizedTransaction.organization_id) == organization_id)
            .where(col(BudgetNormalizedTransaction.import_session_id) == import_id)
        )
    )
    class_rows = list(
        await session.exec(
            select(BudgetTransactionClassification)
            .where(col(BudgetTransactionClassification.organization_id) == organization_id)
            .where(col(BudgetTransactionClassification.import_session_id) == import_id)
        )
    )

    parsed_by_id = {row.id: row for row in parsed_rows}
    normalized_by_id = {row.id: row for row in normalized_rows}

    classified: list[ClassifiedTransaction] = []
    for row in class_rows:
        normalized = normalized_by_id.get(row.normalized_transaction_id)
        if normalized is None:
            continue
        parsed = parsed_by_id.get(normalized.parsed_transaction_id)
        if parsed is None:
            continue

        remapped_category, remapped_subcategory = canonicalize_expense_taxonomy(
            row.category,
            row.subcategory,
            normalized.merchant_candidate or normalized.normalized_description,
        )
        resolved_group_key = canonical_group_key(
            remapped_category,
            remapped_subcategory,
            normalized.merchant_candidate or normalized.normalized_description,
        )

        if (
            row.group_key != resolved_group_key
            or row.category != remapped_category
            or row.subcategory != remapped_subcategory
        ):
            await crud.patch(
                session,
                row,
                {
                    "category": remapped_category,
                    "subcategory": remapped_subcategory,
                    "group_key": resolved_group_key,
                    "updated_at": utcnow(),
                },
            )

        normalized_meta = normalized.extra_data if isinstance(normalized.extra_data, dict) else {}
        movement_type = str(
            (row.extra_data or {}).get(
                "movement_type",
                normalized_meta.get(
                    "movement_type",
                    _strict_movement_type(
                        normalized_description=normalized.normalized_description,
                        merchant_candidate=normalized.merchant_candidate,
                        existing=None,
                        direction=parsed.direction,
                    ),
                ),
            )
        )

        signed_amount = _extract_signed_amount(
            normalized_meta=normalized_meta,
            parsed=parsed,
        )

        recurrence_key = str(
            (row.extra_data or {}).get("recurrence_key")
            or "|".join(
                [
                    str(normalized_meta.get("merchant_fingerprint") or ""),
                    remapped_category,
                    remapped_subcategory,
                    movement_type,
                ]
            )
        )

        classified.append(
            ClassifiedTransaction(
                row_index=parsed.row_index,
                transaction_date=parsed.transaction_date,
                amount=parsed.amount,
                signed_amount=signed_amount,
                direction=parsed.direction,
                raw_description=parsed.raw_description,
                normalized_description=normalized.normalized_description,
                payment_rail=normalized.payment_rail,
                merchant_candidate=normalized.merchant_candidate,
                merchant_base_name=str(normalized_meta.get("merchant_base_name")) if normalized_meta.get("merchant_base_name") else None,
                merchant_fingerprint=str(normalized_meta.get("merchant_fingerprint")) if normalized_meta.get("merchant_fingerprint") else None,
                interpretation_type=row.interpretation_type,
                interpretation_confidence=row.interpretation_confidence,
                interpretation_reason=row.interpretation_reason,
                classification_type=row.classification_type,
                category=remapped_category,
                subcategory=remapped_subcategory,
                confidence=row.confidence,
                explanation=row.explanation,
                evidence_source=row.evidence_source,
                group_key=resolved_group_key,
                movement_type=movement_type,
                inferred_cadence=row.inferred_cadence,
                cadence_confidence=row.cadence_confidence,
                cadence_reason=row.cadence_reason,
                impact_on_baseline=row.impact_on_baseline,
                included=row.included,
                observed_only=row.observed_only,
                review_reasons=list(row.review_reasons or []),
                metadata={**(row.extra_data or {}), "recurrence_key": recurrence_key},
            )
        )
    return classified


def _apply_group_override_history(lines: list[BudgetLine], overrides: list[BudgetManualOverride]) -> list[BudgetLine]:
    by_group = {line.group_key: line for line in lines}
    for override in sorted(overrides, key=lambda item: item.created_at):
        line = by_group.get(override.target_id)
        if line is None:
            continue

        payload = override.payload if isinstance(override.payload, dict) else {}
        op = override.operation
        if op == "set_include":
            include = bool(payload.get("included", True))
            line.included = include
            line.impact_on_baseline = "included" if include else "excluded"
        elif op == "set_cadence":
            cadence = str(payload.get("cadence", line.inferred_cadence))
            line.inferred_cadence = cadence
            if cadence in {"weekly", "fortnightly", "monthly", "quarterly", "yearly"}:
                apply_line_source(line, amount=line.base_amount, period=cadence)
            else:
                line.observed_only = True
                line.is_modeled = False
                line.modeling_status = "observational_only"
                line.normalized_weekly = Decimal("0.00")
                line.normalized_fortnightly = Decimal("0.00")
                line.normalized_monthly = Decimal("0.00")
                line.normalized_yearly = Decimal("0.00")
        elif op == "set_normalized_monthly":
            monthly = Decimal(str(payload.get("normalized_monthly", line.normalized_monthly)))
            normalized = normalize_from_source(monthly, "monthly")
            line.normalized_monthly = normalized["monthly"]
            line.normalized_weekly = normalized["weekly"]
            line.normalized_fortnightly = normalized["fortnightly"]
            line.normalized_yearly = normalized["yearly"]
            line.authoritative_field = "monthly"
            line.source_amount = line.normalized_monthly
            line.source_period = "monthly"
        elif op == "set_category":
            line.category = str(payload.get("category", line.category))
        elif op == "set_subcategory":
            line.subcategory = str(payload.get("subcategory", line.subcategory))
        elif op == "set_bucket_assignment":
            _apply_line_bucket_state(line, str(payload.get("bucket_assignment", line.bucket_assignment)))
        elif op == "mark_recurring":
            _apply_line_bucket_state(line, _manual_bucket_for_recurring(category=line.category, line_type=line.line_type))
        elif op == "mark_one_off":
            _apply_line_bucket_state(line, _manual_bucket_for_one_off(category=line.category, line_type=line.line_type))
        elif op == "set_base_amount_period":
            amount = Decimal(str(payload.get("base_amount", line.base_amount)))
            period = str(payload.get("base_period", line.base_period))
            apply_line_source(line, amount=amount, period=period)
        elif op == "set_authoritative_period_values":
            authoritative_field = str(payload.get("authoritative_field", line.authoritative_field))
            value = Decimal(str(payload.get(authoritative_field, line.normalized_monthly)))
            normalized = normalize_from_source(value, authoritative_field)
            line.normalized_weekly = normalized["weekly"]
            line.normalized_fortnightly = normalized["fortnightly"]
            line.normalized_monthly = normalized["monthly"]
            line.normalized_yearly = normalized["yearly"]
            line.authoritative_field = authoritative_field
            line.source_amount = value
            line.source_period = authoritative_field
            line.base_amount = normalized["monthly"]
            line.base_period = "monthly"
        elif op == "set_notes":
            line.notes = str(payload.get("notes", "")) or None

    for line in by_group.values():
        line.metadata = {
            **(line.metadata or {}),
            "is_modeled": line.is_modeled,
            "modeled_by_default": line.modeled_by_default,
            "modeling_status": line.modeling_status,
            "merchant_confidence": line.merchant_confidence,
            "bucket_confidence": line.bucket_confidence,
            "observed_amount": line.observed_amount,
            "observed_frequency_label": line.observed_frequency_label,
            "duplicate_group_candidates": line.duplicate_group_candidates,
            "merge_candidate_confidence": line.merge_candidate_confidence,
        }

    return list(by_group.values())


async def _replace_line_items(
    *,
    session,
    organization_id: UUID,
    import_id: UUID,
    lines: list[BudgetLine],
) -> None:
    await crud.delete_where(
        session,
        BudgetLineItem,
        col(BudgetLineItem.organization_id) == organization_id,
        col(BudgetLineItem.import_session_id) == import_id,
    )
    for line in lines:
        await crud.create(
            session,
            BudgetLineItem,
            organization_id=organization_id,
            import_session_id=import_id,
            group_key=line.group_key,
            group_label=line.group_label,
            line_type=line.line_type,
            category=line.category,
            subcategory=line.subcategory,
            inferred_cadence=line.inferred_cadence,
            cadence_confidence=line.cadence_confidence,
            cadence_reason=line.cadence_reason,
            observed_only=line.observed_only,
            bucket_assignment=line.bucket_assignment,
            base_amount=line.base_amount,
            base_period=line.base_period,
            authoritative_field=line.authoritative_field,
            source_amount=line.source_amount,
            source_period=line.source_period,
            observed_window_total=line.observed_window_total,
            normalized_weekly=line.normalized_weekly,
            normalized_fortnightly=line.normalized_fortnightly,
            normalized_monthly=line.normalized_monthly,
            normalized_yearly=line.normalized_yearly,
            reserve_monthly_equivalent=line.reserve_monthly_equivalent,
            impact_on_baseline=line.impact_on_baseline,
            included=line.included,
            confidence=float(line.confidence),
            explanation=line.explanation,
            notes=line.notes,
            review_reasons=line.review_reasons,
            transaction_count=line.transaction_count,
            extra_data=_to_json_safe({"row_indexes": line.row_indexes, **line.metadata}),
        )


async def apply_overrides(
    *,
    session,
    organization_id: UUID,
    import_id: UUID,
    user_id: UUID | None,
    operations: list[BudgetOverrideOperation],
) -> int:
    applied = 0
    for op in operations:
        await crud.create(
            session,
            BudgetManualOverride,
            organization_id=organization_id,
            import_session_id=import_id,
            target_type=op.target_type,
            target_id=op.target_id,
            operation=op.operation,
            payload=op.payload,
            created_by_user_id=user_id,
        )
        applied += 1

        if op.target_type == "transaction":
            classification = await session.exec(
                select(BudgetTransactionClassification)
                .where(col(BudgetTransactionClassification.import_session_id) == import_id)
                .where(col(BudgetTransactionClassification.organization_id) == organization_id)
                .where(col(BudgetTransactionClassification.normalized_transaction_id) == UUID(op.target_id))
            )
            row = classification.first()
            if row is None:
                continue

            updates: dict[str, object] = {"evidence_source": "manual_override", "updated_at": utcnow()}
            metadata = dict(row.extra_data or {})

            if op.operation == "set_category":
                updates["category"] = str(op.payload.get("category", row.category))
            if op.operation == "set_subcategory":
                updates["subcategory"] = str(op.payload.get("subcategory", row.subcategory))
            if op.operation == "set_include":
                include = bool(op.payload.get("included", True))
                updates["included"] = include
                updates["impact_on_baseline"] = "included" if include else "excluded"
            if op.operation == "set_bucket_assignment":
                bucket_assignment = str(op.payload.get("bucket_assignment", metadata.get("bucket_assignment", row.bucket_assignment)))
                metadata["bucket_assignment"] = bucket_assignment
                metadata["final_bucket"] = _final_bucket_for_bucket_assignment(bucket_assignment)
                updates.update(_classification_updates_for_bucket(bucket_assignment=bucket_assignment))
                updates["extra_data"] = _to_json_safe(metadata)
            if op.operation == "set_cadence":
                updates["inferred_cadence"] = str(op.payload.get("cadence", row.inferred_cadence))
            if op.operation == "mark_recurring":
                bucket_assignment = _manual_bucket_for_recurring(category=row.category)
                metadata["bucket_assignment"] = bucket_assignment
                metadata["final_bucket"] = _final_bucket_for_bucket_assignment(bucket_assignment)
                updates.update(_classification_updates_for_bucket(bucket_assignment=bucket_assignment))
                updates["extra_data"] = _to_json_safe(metadata)
            if op.operation == "mark_one_off":
                bucket_assignment = _manual_bucket_for_one_off(category=row.category)
                metadata["bucket_assignment"] = bucket_assignment
                metadata["final_bucket"] = _final_bucket_for_bucket_assignment(bucket_assignment)
                updates.update(_classification_updates_for_bucket(bucket_assignment=bucket_assignment))
                updates["extra_data"] = _to_json_safe(metadata)

            await crud.patch(session, row, updates)

            if op.operation == "remember_mapping":
                norm_row = await session.exec(
                    select(BudgetNormalizedTransaction)
                    .where(col(BudgetNormalizedTransaction.id) == row.normalized_transaction_id)
                    .where(col(BudgetNormalizedTransaction.organization_id) == organization_id)
                )
                normalized = norm_row.first()
                if normalized and normalized.merchant_candidate:
                    fingerprint = merchant_fingerprint(normalized.merchant_candidate)
                    if fingerprint:
                        await crud.create(
                            session,
                            BudgetMerchantMemory,
                            organization_id=organization_id,
                            merchant_key=normalized.merchant_candidate,
                            merchant_fingerprint=fingerprint,
                            category=str(op.payload.get("category", row.category)),
                            subcategory=str(op.payload.get("subcategory", row.subcategory)),
                            confidence=0.95,
                            source="manual_override",
                            active=True,
                        )

        if op.target_type == "group":
            if op.operation == "merge_group":
                target_group = str(op.payload.get("target_group", "")).strip()
                if target_group and target_group != op.target_id:
                    merge_rows = list(
                        await session.exec(
                            select(BudgetTransactionClassification)
                            .where(col(BudgetTransactionClassification.import_session_id) == import_id)
                            .where(col(BudgetTransactionClassification.organization_id) == organization_id)
                            .where(col(BudgetTransactionClassification.group_key) == op.target_id)
                        )
                    )
                    for row in merge_rows:
                        await crud.patch(
                            session,
                            row,
                            {"group_key": target_group, "updated_at": utcnow(), "evidence_source": "manual_override"},
                        )

            if op.operation in {"split_group", "reassign_transactions"}:
                transaction_ids = [str(v) for v in (op.payload.get("transaction_ids") or []) if str(v).strip()]
                target_group = str(op.payload.get("target_group", "")).strip()
                if transaction_ids and target_group:
                    rows = list(
                        await session.exec(
                            select(BudgetTransactionClassification)
                            .where(col(BudgetTransactionClassification.import_session_id) == import_id)
                            .where(col(BudgetTransactionClassification.organization_id) == organization_id)
                            .where(col(BudgetTransactionClassification.group_key) == op.target_id)
                        )
                    )
                    allowed = {str(row.normalized_transaction_id): row for row in rows}
                    for tx_id in transaction_ids:
                        row = allowed.get(tx_id)
                        if row is None:
                            continue
                        await crud.patch(
                            session,
                            row,
                            {"group_key": target_group, "updated_at": utcnow(), "evidence_source": "manual_override"},
                        )

            rows = list(
                await session.exec(
                    select(BudgetLineItem)
                    .where(col(BudgetLineItem.import_session_id) == import_id)
                    .where(col(BudgetLineItem.organization_id) == organization_id)
                    .where(col(BudgetLineItem.group_key) == op.target_id)
                )
            )
            for row in rows:
                updates = {"updated_at": utcnow()}
                metadata = dict(row.extra_data or {})
                if op.operation == "set_include":
                    include = bool(op.payload.get("included", True))
                    updates["included"] = include
                    updates["impact_on_baseline"] = "included" if include else "excluded"
                if op.operation == "set_cadence":
                    updates["inferred_cadence"] = str(op.payload.get("cadence", row.inferred_cadence))
                if op.operation == "set_normalized_monthly":
                    monthly = Decimal(str(op.payload.get("normalized_monthly", row.normalized_monthly)))
                    normalized = normalize_from_source(monthly, "monthly")
                    updates["normalized_monthly"] = normalized["monthly"]
                    updates["normalized_weekly"] = normalized["weekly"]
                    updates["normalized_fortnightly"] = normalized["fortnightly"]
                    updates["normalized_yearly"] = normalized["yearly"]
                    updates["authoritative_field"] = "monthly"
                    updates["source_amount"] = normalized["monthly"]
                    updates["source_period"] = "monthly"
                if op.operation == "set_category":
                    updates["category"] = str(op.payload.get("category", row.category))
                if op.operation == "set_subcategory":
                    updates["subcategory"] = str(op.payload.get("subcategory", row.subcategory))
                if op.operation == "set_bucket_assignment":
                    updates.update(
                        _classification_updates_for_bucket(
                            bucket_assignment=str(op.payload.get("bucket_assignment", row.bucket_assignment))
                        )
                    )
                if op.operation == "mark_recurring":
                    updates.update(_classification_updates_for_bucket(bucket_assignment=_manual_bucket_for_recurring(category=row.category)))
                if op.operation == "mark_one_off":
                    updates.update(_classification_updates_for_bucket(bucket_assignment=_manual_bucket_for_one_off(category=row.category)))
                if op.operation == "set_base_amount_period":
                    amount = Decimal(str(op.payload.get("base_amount", row.base_amount)))
                    period = str(op.payload.get("base_period", row.base_period))
                    normalized = normalize_from_source(amount, period)
                    updates["base_amount"] = amount
                    updates["base_period"] = period
                    updates["source_amount"] = amount
                    updates["source_period"] = period
                    updates["authoritative_field"] = "base_amount"
                    updates["normalized_weekly"] = normalized["weekly"]
                    updates["normalized_fortnightly"] = normalized["fortnightly"]
                    updates["normalized_monthly"] = normalized["monthly"]
                    updates["normalized_yearly"] = normalized["yearly"]
                    updates["extra_data"] = _to_json_safe(metadata)
                if op.operation == "set_authoritative_period_values":
                    field = str(op.payload.get("authoritative_field", "monthly"))
                    amount = Decimal(str(op.payload.get(field, row.normalized_monthly)))
                    normalized = normalize_from_source(amount, field)
                    updates["authoritative_field"] = field
                    updates["source_amount"] = amount
                    updates["source_period"] = field
                    updates["normalized_weekly"] = normalized["weekly"]
                    updates["normalized_fortnightly"] = normalized["fortnightly"]
                    updates["normalized_monthly"] = normalized["monthly"]
                    updates["normalized_yearly"] = normalized["yearly"]
                    updates["extra_data"] = _to_json_safe(metadata)
                if op.operation == "set_notes":
                    updates["notes"] = str(op.payload.get("notes", "")) or None
                await crud.patch(session, row, updates)

    await recompute_snapshot(session=session, organization_id=organization_id, import_id=import_id)
    return applied


async def recompute_snapshot(*, session, organization_id: UUID, import_id: UUID) -> None:
    classified = await _load_classified_transactions(
        session=session,
        organization_id=organization_id,
        import_id=import_id,
    )

    historical_recurrence = await _historical_recurrence_map(
        session=session,
        organization_id=organization_id,
        import_id=import_id,
    )
    import_row = (
        await session.exec(
            select(BudgetImportSession)
            .where(col(BudgetImportSession.organization_id) == organization_id)
            .where(col(BudgetImportSession.id) == import_id)
        )
    ).first()
    statement_window_days = compute_statement_window_days(
        statement_start_date=import_row.statement_start_date if import_row else None,
        statement_end_date=import_row.statement_end_date if import_row else None,
    )

    lines = build_lines(
        classified,
        historical_recurrence=historical_recurrence,
        statement_window_days=statement_window_days,
    )

    group_overrides = list(
        await session.exec(
            select(BudgetManualOverride)
            .where(col(BudgetManualOverride.organization_id) == organization_id)
            .where(col(BudgetManualOverride.import_session_id) == import_id)
            .where(col(BudgetManualOverride.target_type) == "group")
        )
    )
    lines = _apply_group_override_history(lines, group_overrides)

    await _replace_line_items(
        session=session,
        organization_id=organization_id,
        import_id=import_id,
        lines=lines,
    )

    import_row = (
        await session.exec(
            select(BudgetImportSession)
            .where(col(BudgetImportSession.organization_id) == organization_id)
            .where(col(BudgetImportSession.id) == import_id)
        )
    ).first()

    parsed_statement = (
        await session.exec(
            select(BudgetParsedStatement)
            .where(col(BudgetParsedStatement.organization_id) == organization_id)
            .where(col(BudgetParsedStatement.import_session_id) == import_id)
            .order_by(col(BudgetParsedStatement.created_at).desc())
        )
    ).first()

    parsed_rows = list(
        await session.exec(
            select(BudgetParsedTransaction)
            .where(col(BudgetParsedTransaction.organization_id) == organization_id)
            .where(col(BudgetParsedTransaction.import_session_id) == import_id)
        )
    )

    parsed_meta = parsed_statement.extra_data if parsed_statement and isinstance(parsed_statement.extra_data, dict) else {}
    expected_closing_balance = (
        Decimal(str(parsed_meta.get("expected_closing_balance")))
        if parsed_meta.get("expected_closing_balance") is not None
        else None
    )

    trust_assessment = assess_import_trust(
        import_row=import_row,
        parsed_meta=parsed_meta,
        lines=lines,
    ) if import_row is not None else None

    statement_truth = compute_statement_totals(
        import_row=import_row,
        parsed_rows=parsed_rows,
        expected_closing_balance=expected_closing_balance,
        truth_trust_level=trust_assessment.truth_trust_level if trust_assessment else "needs_review",
    ) if import_row is not None else None

    budget_model = compute_budget_model_totals(
        lines=lines,
        modeling_allowed=trust_assessment.modeling_allowed if trust_assessment else False,
        modeling_restrictions=trust_assessment.modeling_restrictions if trust_assessment else ["Import missing for trust assessment."],
        statement_window_days=compute_statement_window_days(
            statement_start_date=import_row.statement_start_date if import_row else None,
            statement_end_date=import_row.statement_end_date if import_row else None,
        ),
    )

    summary = build_snapshot_summary(lines)
    summary.update(
        {
            "monthly_recurring_income": budget_model.recurring_income_monthly,
            "monthly_recurring_baseline_expenses": budget_model.recurring_baseline_monthly,
            "monthly_variable_discretionary": budget_model.variable_discretionary_monthly,
            "observed_one_off_exceptional_total": budget_model.observed_one_off_total,
            "observed_transfer_total": budget_model.observed_transfer_total,
            "observed_irregular_income_total": budget_model.irregular_income_total,
            "net_recurring_monthly": budget_model.core_net,
            "net_observed_total": budget_model.observed_net,
            "core_monthly_baseline": budget_model.recurring_baseline_monthly,
            "observed_discretionary_monthly": budget_model.variable_discretionary_monthly,
            "annual_infrequent_oneoff_spend": budget_model.observed_one_off_total,
            "transfer_money_movement_spend": budget_model.observed_transfer_total,
            "total_income_monthly": budget_model.recurring_income_monthly,
            "total_expenses_monthly": budget_model.recurring_baseline_monthly + budget_model.variable_discretionary_monthly,
            "net_monthly": budget_model.observed_net,
            "budget_model": asdict(budget_model),
            "trust": asdict(trust_assessment) if trust_assessment else {},
            "modeling_allowed": trust_assessment.modeling_allowed if trust_assessment else False,
            "totals_trust_level": trust_assessment.truth_trust_level if trust_assessment else "needs_review",
            "modeling_restrictions": list(trust_assessment.modeling_restrictions or []) if trust_assessment else ["Import missing for trust assessment."],
        }
    )

    if statement_truth is not None:
        summary["statement_truth"] = asdict(statement_truth)

    existing = await session.exec(
        select(BudgetSnapshot)
        .where(col(BudgetSnapshot.import_session_id) == import_id)
        .where(col(BudgetSnapshot.organization_id) == organization_id)
        .order_by(col(BudgetSnapshot.created_at).desc())
    )
    snapshot = existing.first()
    if snapshot is None:
        await crud.create(
            session,
            BudgetSnapshot,
            organization_id=organization_id,
            import_session_id=import_id,
            summary=_to_json_safe(summary),
        )
        return

    await crud.patch(
        session,
        snapshot,
        {
            "summary": _to_json_safe(summary),
            "updated_at": utcnow(),
        },
    )


async def list_latest_snapshot(*, session, organization_id: UUID, import_id: UUID) -> BudgetSnapshot | None:
    rows = await session.exec(
        select(BudgetSnapshot)
        .where(col(BudgetSnapshot.organization_id) == organization_id)
        .where(col(BudgetSnapshot.import_session_id) == import_id)
        .order_by(col(BudgetSnapshot.updated_at).desc())
    )
    return rows.first()
