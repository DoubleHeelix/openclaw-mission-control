"""Trust, eligibility, and import-level guardrails."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from app.models.budget import BudgetImportSession
from app.services.budget_v2.constants import ReconciliationStatus, TrustLevel


@dataclass
class TrustAssessment:
    reconciliation_status: str
    totals_trust_level: str
    truth_trust_level: str
    modeling_allowed: bool
    modeling_restrictions: list[str] = field(default_factory=list)
    trust_reasons: list[str] = field(default_factory=list)


@dataclass
class LineTrustAssessment:
    line_trust_level: str
    modeling_eligible: bool
    modeling_block_reason: str | None = None


def import_scope_warnings(row: BudgetImportSession, lines: list[object]) -> list[str]:
    warnings: list[str] = []
    monthly_income = sum(
        (
            getattr(item, "normalized_monthly", Decimal("0.00"))
            for item in lines
            if getattr(item, "line_type", None) == "income" and bool(getattr(item, "included", False))
        ),
        Decimal("0.00"),
    )
    parser_coverage_issue = any(
        "credits, but parsed credit rows are missing" in warning.lower()
        or "multi-page transaction table detected" in warning.lower()
        or "reconcile" in warning.lower()
        for warning in (row.parser_warnings or [])
    )
    if monthly_income <= Decimal("0.00") and not parser_coverage_issue:
        warnings.append("Income is zero in this import. Totals may reflect only a partial budget view.")

    date_span_days: int | None = None
    if isinstance(row.statement_start_date, date) and isinstance(row.statement_end_date, date):
        date_span_days = max((row.statement_end_date - row.statement_start_date).days, 0)

    if row.transaction_count < 15:
        warnings.append("Transaction count is low. Treat this import as a partial or low-signal budget view.")
    if date_span_days is not None and date_span_days < 21:
        warnings.append("Statement range is short. Recurring and baseline totals may be incomplete.")
    return warnings


def assess_import_trust(
    *,
    import_row: BudgetImportSession,
    parsed_meta: dict[str, object],
    lines: list[BudgetLineItem],
) -> TrustAssessment:
    reasons: list[str] = []
    restrictions: list[str] = []
    status = str(import_row.reconciliation_status or ReconciliationStatus.UNKNOWN)

    if import_row.parser_warnings:
        reasons.extend(str(item) for item in import_row.parser_warnings)
    if import_row.needs_review:
        reasons.append("Import still has unresolved review items.")

    text_extraction_length = int(parsed_meta.get("text_extraction_length", 0) or 0)
    table_detected = bool(parsed_meta.get("table_detected", False))
    document_reconcilable = parsed_meta.get("document_reconcilable")
    parser_failure_reason = parsed_meta.get("parser_failure_reason")
    overlap_status = str(parsed_meta.get("overlap_status") or "clear")

    statement_days: int | None = None
    if import_row.statement_start_date and import_row.statement_end_date:
        statement_days = max((import_row.statement_end_date - import_row.statement_start_date).days + 1, 0)

    if text_extraction_length and text_extraction_length < 500:
        restrictions.append("Text extraction quality is weak.")
    if not table_detected:
        restrictions.append("Transaction table was not detected confidently.")
    if parser_failure_reason:
        restrictions.append(str(parser_failure_reason))
    if statement_days is not None and statement_days < 21:
        restrictions.append("Statement range is too short for trusted recurrence modeling.")
    if overlap_status == "exact_duplicate":
        restrictions.append("This import appears to duplicate a previously imported statement.")
    elif overlap_status == "overlap_detected":
        restrictions.append("This import overlaps with a previous statement window.")

    catastrophic_restrictions: list[str] = []

    if status == ReconciliationStatus.RECONCILED:
        trust = TrustLevel.VERIFIED
    elif status == ReconciliationStatus.SOURCE_NON_RECONCILABLE:
        trust = TrustLevel.PROVISIONAL
        restrictions.append("Source document is non-reconcilable.")
        catastrophic_restrictions.append("Source document is non-reconcilable.")
    elif status in {ReconciliationStatus.PARSER_INCOMPLETE, ReconciliationStatus.EXTRACTION_DEGRADED}:
        trust = TrustLevel.PARTIAL
        restrictions.append("Parser coverage is incomplete.")
    elif status == ReconciliationStatus.FAILED_RECONCILIATION:
        trust = TrustLevel.FAILED_RECONCILIATION
        restrictions.append("Reconciliation failed beyond tolerance.")
        catastrophic_restrictions.append("Reconciliation failed beyond tolerance.")
    else:
        trust = TrustLevel.NEEDS_REVIEW
        restrictions.append("Trust state is unknown.")
        catastrophic_restrictions.append("Trust state is unknown.")

    if document_reconcilable is False and "Source document is non-reconcilable." not in restrictions:
        restrictions.append("Source document is marked provisional/non-reconcilable.")
        catastrophic_restrictions.append("Source document is marked provisional/non-reconcilable.")
    if overlap_status == "exact_duplicate":
        catastrophic_restrictions.append("This import appears to duplicate a previously imported statement.")

    modeling_allowed = not catastrophic_restrictions and not any("short" in item.lower() for item in restrictions)
    if not modeling_allowed and not restrictions:
        restrictions.append("Modeling blocked by trust policy.")

    return TrustAssessment(
        reconciliation_status=status,
        totals_trust_level=trust,
        truth_trust_level=trust,
        modeling_allowed=modeling_allowed,
        modeling_restrictions=restrictions,
        trust_reasons=reasons or restrictions,
    )


def assess_line_trust(*, line: object, import_assessment: TrustAssessment) -> LineTrustAssessment:
    confidence = float(getattr(line, "confidence", 0.0) or 0.0)
    cadence_confidence = float(getattr(line, "cadence_confidence", 0.0) or 0.0)
    bucket_assignment = str(getattr(line, "bucket_assignment", ""))
    line_type = str(getattr(line, "line_type", "expense"))
    review_reasons = list(getattr(line, "review_reasons", []) or [])
    transaction_count = int(getattr(line, "transaction_count", 0) or 0)
    inferred_cadence = str(getattr(line, "inferred_cadence", "unknown") or "unknown")

    if not import_assessment.modeling_allowed and bucket_assignment in {"recurring_baseline", "income_recurring"}:
        return LineTrustAssessment(
            line_trust_level=import_assessment.totals_trust_level,
            modeling_eligible=False,
            modeling_block_reason=", ".join(import_assessment.modeling_restrictions[:2]) or "Import trust blocks modeling.",
        )

    if bucket_assignment not in {"recurring_baseline", "income_recurring"}:
        return LineTrustAssessment(
            line_trust_level="observational",
            modeling_eligible=False,
            modeling_block_reason="Observed-only bucket.",
        )

    if "duplicate_group_candidate" in review_reasons:
        return LineTrustAssessment("needs_review", False, "Possible duplicate group.")
    if any(reason in review_reasons for reason in {"parser_anomaly", "suspected_leakage"}):
        return LineTrustAssessment("needs_review", False, "Parser integrity issue.")
    if inferred_cadence not in {"weekly", "fortnightly", "monthly", "quarterly", "yearly"}:
        return LineTrustAssessment("needs_review", False, "Cadence is not stable enough for modeling.")
    if transaction_count < (2 if line_type == "income" else 3):
        return LineTrustAssessment("provisional", False, "Not enough observations across statements.")
    if confidence < 0.68 or cadence_confidence < 0.68:
        return LineTrustAssessment("provisional", False, "Confidence is below modeling threshold.")
    if review_reasons:
        return LineTrustAssessment("provisional", False, "Unresolved review reasons still remain.")
    return LineTrustAssessment("verified", True, None)
