"""Deterministic final bucket resolution."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from app.services.budget_v2.expense_classifier import ExpenseAssessment
from app.services.budget_v2.income_detector import IncomeAssessment
from app.services.budget_v2.merchant_memory import MerchantMemoryHints
from app.services.budget_v2.recurrence import RecurrenceAssessment
from app.services.budget_v2.transfer_detector import TransferAssessment
from app.services.budget_v2.types import NormalizedTransaction


@dataclass
class ResolvedTransaction:
    row_index: int
    transaction_date: object
    amount: Decimal
    signed_amount: Decimal
    direction: str
    raw_description: str
    normalized_description: str
    payment_rail: str | None
    merchant_candidate: str | None
    merchant_base_name: str | None
    merchant_fingerprint: str | None
    movement_type: str
    final_bucket: str
    interpretation_type: str
    interpretation_confidence: float
    interpretation_reason: str
    classification_type: str
    category: str
    subcategory: str
    confidence: float
    reasons: list[str] = field(default_factory=list)
    review_flags: list[str] = field(default_factory=list)
    evidence_source: str = "rule"
    mapping_source: str | None = None
    bucket_assignment: str = "variable_discretionary"
    included: bool = True
    observed_only: bool = True
    impact_on_baseline: str = "included"
    inferred_cadence: str | None = None
    cadence_confidence: float | None = None
    cadence_reason: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


FINAL_BUCKET_TO_LEGACY = {
    "income": "income_irregular",
    "recurring_baseline_expenses": "recurring_baseline",
    "variable_spending": "variable_discretionary",
    "one_off_spending": "one_off_exceptional",
    "transfers": "transfer_money_movement",
    "fees": "variable_discretionary",
    "uncategorized": "variable_discretionary",
}


def resolve_transaction(
    *,
    tx: NormalizedTransaction,
    memory: MerchantMemoryHints,
    transfer: TransferAssessment,
    income: IncomeAssessment | None,
    recurrence: RecurrenceAssessment | None,
    expense: ExpenseAssessment | None,
) -> ResolvedTransaction:
    reasons: list[str] = []
    review_flags: list[str] = []
    mapping_source = memory.mapping_source if memory.mapping_source != "none" else None

    final_bucket = "uncategorized"
    movement_type = tx.movement_type
    interpretation_type = "merchant_expense"
    interpretation_confidence = 0.5
    interpretation_reason = "Fallback interpretation."
    classification_type = "uncategorized_review"
    category = "Needs Review"
    subcategory = "Needs Review"
    confidence = 0.25
    observed_only = True
    impact_on_baseline = "included"

    # Transfers and debt-like money movement first.
    if transfer.state == "transfer_confirmed_internal":
        final_bucket = "transfers"
        movement_type = "internal_transfer"
        interpretation_type = transfer.state
        interpretation_confidence = transfer.confidence
        interpretation_reason = "Confirmed internal transfer semantics."
        classification_type = "transfer_or_money_movement"
        category = "Transfer / Money Movement"
        subcategory = "Transfer / Money Movement"
        confidence = transfer.confidence
        reasons.extend(transfer.reasons)
        impact_on_baseline = "excluded"

    elif transfer.state == "transfer_confirmed_debt_payment":
        final_bucket = "transfers"
        movement_type = "debt_payment"
        interpretation_type = transfer.state
        interpretation_confidence = transfer.confidence
        interpretation_reason = "Debt payment detected and excluded from spend."
        classification_type = "transfer_or_money_movement"
        category = "Transfer / Money Movement"
        subcategory = "Debt Payment"
        confidence = transfer.confidence
        reasons.extend(transfer.reasons)
        impact_on_baseline = "excluded"

    elif income and income.state in {"reimbursement", "refund", "transfer_in"}:
        final_bucket = "transfers"
        movement_type = "refund" if income.state == "refund" else "internal_transfer"
        interpretation_type = income.state
        interpretation_confidence = income.confidence
        interpretation_reason = "; ".join(income.reasons) or income.state
        classification_type = income.state
        category = income.category
        subcategory = income.subcategory
        confidence = income.confidence
        reasons.extend(income.reasons)
        impact_on_baseline = "excluded"

    elif transfer.state == "transfer_confirmed_external":
        final_bucket = "transfers"
        movement_type = "internal_transfer"
        interpretation_type = transfer.state
        interpretation_confidence = transfer.confidence
        interpretation_reason = "Confirmed outbound transfer semantics."
        classification_type = "transfer_or_money_movement"
        category = "Transfer / Money Movement"
        subcategory = "Transfer / Money Movement"
        confidence = transfer.confidence
        reasons.extend(transfer.reasons)
        impact_on_baseline = "excluded"

    elif transfer.state == "transfer_likely_inbound" and tx.direction == "credit":
        final_bucket = "transfers"
        movement_type = "internal_transfer"
        interpretation_type = transfer.state
        interpretation_confidence = transfer.confidence
        interpretation_reason = "Inbound transfer-like credit."
        classification_type = "transfer_or_money_movement"
        category = "Transfer / Money Movement"
        subcategory = "Transfer / Money Movement"
        confidence = transfer.confidence
        reasons.extend(transfer.reasons)
        impact_on_baseline = "excluded"
        review_flags.append("transfer_like_requires_review")

    # Income next.
    elif income and income.state in {
        "salary_recurring_confirmed",
        "salary_recurring_likely",
        "income_irregular",
        "credit_uncategorized",
    }:
        final_bucket = "income" if income.state != "credit_uncategorized" else "uncategorized"
        movement_type = "income"
        interpretation_type = income.state
        interpretation_confidence = income.confidence
        interpretation_reason = "; ".join(income.reasons) or income.state
        classification_type = income.state
        category = income.category
        subcategory = income.subcategory
        confidence = income.confidence
        reasons.extend(income.reasons)

        if income.state == "salary_recurring_confirmed":
            observed_only = False
        if income.state == "salary_recurring_likely":
            review_flags.append("salary_like_single_occurrence")
        if income.state == "credit_uncategorized":
            review_flags.append("credit_requires_review")

    # Fees
    elif expense and expense.proposed_bucket == "fees":
        final_bucket = "fees"
        movement_type = "fee"
        interpretation_type = "merchant_fee"
        interpretation_confidence = expense.confidence
        interpretation_reason = "; ".join(expense.reasons) or "Fee semantics."
        classification_type = "bank_fee"
        category = expense.category
        subcategory = expense.subcategory
        confidence = expense.confidence
        reasons.extend(expense.reasons)

    # Recurring baseline
    elif expense and expense.proposed_bucket == "recurring_baseline_expenses":
        final_bucket = "recurring_baseline_expenses"
        movement_type = "expense"
        interpretation_type = recurrence.state if recurrence else "merchant_expense"
        interpretation_confidence = expense.confidence
        interpretation_reason = "; ".join(expense.reasons) or "Recurring baseline expense."
        classification_type = "baseline_recurring"
        category = expense.category
        subcategory = expense.subcategory
        confidence = expense.confidence
        reasons.extend(expense.reasons)
        review_flags.extend(expense.review_flags)
        observed_only = False if recurrence and recurrence.state == "recurring_confirmed" else True

    # Variable
    elif expense and expense.proposed_bucket == "variable_spending":
        final_bucket = "variable_spending"
        movement_type = "expense"
        interpretation_type = "merchant_expense"
        interpretation_confidence = expense.confidence
        interpretation_reason = "; ".join(expense.reasons) or "Routine variable spend."
        classification_type = "variable_spend"
        category = expense.category
        subcategory = expense.subcategory
        confidence = expense.confidence
        reasons.extend(expense.reasons)
        review_flags.extend(expense.review_flags)

    # One-off
    elif expense and expense.proposed_bucket == "one_off_spending":
        final_bucket = "one_off_spending"
        movement_type = "expense"
        interpretation_type = "merchant_expense"
        interpretation_confidence = expense.confidence
        interpretation_reason = "; ".join(expense.reasons) or "One-off spend."
        classification_type = "one_off_spend"
        category = expense.category
        subcategory = expense.subcategory
        confidence = expense.confidence
        reasons.extend(expense.reasons)
        review_flags.extend(expense.review_flags)

    elif expense:
        final_bucket = "uncategorized"
        movement_type = "other_needs_review"
        interpretation_type = "merchant_expense"
        interpretation_confidence = expense.confidence
        interpretation_reason = "; ".join(expense.reasons) or "Expense unresolved."
        classification_type = "uncategorized_review"
        category = expense.category
        subcategory = expense.subcategory
        confidence = expense.confidence
        reasons.extend(expense.reasons)
        review_flags.extend(expense.review_flags)

    # Cross-cutting review rules
    if recurrence and recurrence.state == "recurring_likely" and final_bucket in {"uncategorized", "variable_spending"}:
        review_flags.append("possible_recurring_insufficient_occurrences")

    if transfer.state in {"transfer_likely", "transfer_likely_inbound"} and final_bucket not in {"transfers", "income"}:
        review_flags.append("transfer_like_requires_review")

    if recurrence and recurrence.state == "recurring_likely" and tx.amount >= Decimal("200.00"):
        review_flags.append("statement_shorter_than_cycle")

    # Legacy bucket assignment mapping
    bucket_assignment = FINAL_BUCKET_TO_LEGACY[final_bucket]

    if final_bucket == "income" and income and income.state in {"salary_recurring_confirmed", "salary_recurring_likely"}:
        bucket_assignment = "income_recurring"
        observed_only = False
    elif final_bucket == "income":
        bucket_assignment = "income_irregular"
    elif final_bucket == "transfers":
        observed_only = True
        impact_on_baseline = "excluded"
    elif final_bucket == "fees":
        observed_only = True
    elif final_bucket == "one_off_spending":
        observed_only = True
    elif final_bucket == "recurring_baseline_expenses" and recurrence and recurrence.state == "recurring_confirmed":
        observed_only = False

    return ResolvedTransaction(
        row_index=tx.row_index,
        transaction_date=tx.transaction_date,
        amount=tx.amount,
        signed_amount=tx.signed_amount,
        direction=tx.direction,
        raw_description=tx.raw_description,
        normalized_description=tx.normalized_description,
        payment_rail=tx.payment_rail,
        merchant_candidate=tx.merchant_candidate,
        merchant_base_name=tx.merchant_base_name,
        merchant_fingerprint=tx.merchant_fingerprint,
        movement_type=movement_type,
        final_bucket=final_bucket,
        interpretation_type=interpretation_type,
        interpretation_confidence=interpretation_confidence,
        interpretation_reason=interpretation_reason,
        classification_type=classification_type,
        category=category,
        subcategory=subcategory,
        confidence=min(max(confidence + memory.confidence_adjustment, 0.0), 0.99),
        reasons=list(dict.fromkeys(reasons)),
        review_flags=list(dict.fromkeys(review_flags)),
        evidence_source="merchant_memory" if mapping_source else "deterministic_rule",
        mapping_source=mapping_source,
        bucket_assignment=bucket_assignment,
        included=True,
        observed_only=observed_only,
        impact_on_baseline=impact_on_baseline,
        inferred_cadence=recurrence.cadence if recurrence else None,
        cadence_confidence=recurrence.confidence if recurrence else None,
        cadence_reason=("; ".join(recurrence.reasons) if recurrence and recurrence.reasons else None),
        metadata={
            "final_bucket": final_bucket,
            "bucket_assignment": bucket_assignment,
            "classification_version": "budget_v2_deterministic_v3",
            "mapping_source": mapping_source,
            "reasons": list(dict.fromkeys(reasons)),
            "review_flags": list(dict.fromkeys(review_flags)),
            "movement_type": movement_type,
            "transfer_score": transfer.confidence,
            "income_score": income.confidence if income else 0.0,
            "recurrence_score": recurrence.confidence if recurrence else 0.0,
            "merchant_semantic_score": expense.confidence if expense else 0.0,
            "cadence_score": recurrence.confidence if recurrence else 0.0,
            "baseline_inclusion_reason": impact_on_baseline,
        },
    )
