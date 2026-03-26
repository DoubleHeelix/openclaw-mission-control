"""Debit expense classification after transfer and recurrence assessment."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from app.services.budget_v2.config import FEE_KEYWORDS
from app.services.budget_v2.merchant_classifier import classify_merchant_descriptor
from app.services.budget_v2.merchant_memory import MerchantMemoryHints
from app.services.budget_v2.recurrence import RecurrenceAssessment
from app.services.budget_v2.transfer_detector import TransferAssessment
from app.services.budget_v2.types import NormalizedTransaction

ONE_OFF_TERMS = (
    "MAC MINI",
    "JB HI FI",
    "JB HIFI",
    "ELECTRONICS",
    "BONDE",
    "BOND",
    "DEPOSIT",
    "EVENT",
    "TICKET",
    "GOVERNMENT",
    "VICROADS",
    "ATO",
)
VARIABLE_TERMS = (
    "GROCERY",
    "ALDI",
    "COLES",
    "WOOLWORTHS",
    "DINING",
    "CAFE",
    "UBER EATS",
    "MYKI",
    "PHARMACY",
    "CHEMIST",
    "FUEL",
    "PETROL",
    "AMPOL",
    "SHELL",
)
BASELINE_TERMS = (
    "RENT",
    "POWER",
    "INTERNET",
    "PHONE",
    "INSURANCE",
    "SUBSCRIPTION",
    "GYM",
    "WATER",
)


@dataclass
class ExpenseAssessment:
    proposed_bucket: str
    confidence: float
    category: str
    subcategory: str
    reasons: list[str] = field(default_factory=list)
    review_flags: list[str] = field(default_factory=list)
    bucket_lean: str = "discretionary"
    baseline_eligible: bool = False
    ancillary: bool = False


def classify_expense(
    *,
    tx: NormalizedTransaction,
    transfer: TransferAssessment,
    recurrence: RecurrenceAssessment | None,
    memory: MerchantMemoryHints,
) -> ExpenseAssessment:
    desc = (tx.normalized_description or "").upper()

    if any(token in desc for token in FEE_KEYWORDS):
        return ExpenseAssessment(
            proposed_bucket="fees",
            confidence=0.93,
            category="Discretionary",
            subcategory="Presents",
            reasons=["fee_keywords"],
            ancillary=True,
        )

    if transfer.state == "transfer_confirmed_internal":
        return ExpenseAssessment(
            proposed_bucket="transfers",
            confidence=transfer.confidence,
            category="Transfer / Money Movement",
            subcategory="Transfer / Money Movement",
            reasons=list(transfer.reasons),
        )

    merchant = classify_merchant_descriptor(tx.merchant_candidate or tx.normalized_description or "")
    category = merchant.category if merchant else (memory.category_hint or "Discretionary")
    subcategory = merchant.subcategory if merchant else (memory.subcategory_hint or "Presents")
    merchant_confidence = merchant.confidence if merchant else (0.45 + memory.confidence_adjustment)
    baseline_eligible = bool(merchant.baseline_eligible) if merchant else memory.baseline_tendency >= 0.5

    if recurrence and recurrence.state == "recurring_confirmed" and (baseline_eligible or any(token in desc for token in BASELINE_TERMS)):
        return ExpenseAssessment(
            proposed_bucket="recurring_baseline_expenses",
            confidence=max(0.82, min(0.95, recurrence.confidence + 0.04)),
            category=category,
            subcategory=subcategory,
            reasons=[*recurrence.reasons, "baseline_semantics"],
            bucket_lean="baseline",
            baseline_eligible=True,
        )

    if recurrence and recurrence.state == "recurring_likely":
        if baseline_eligible or any(token in desc for token in BASELINE_TERMS):
            return ExpenseAssessment(
                proposed_bucket="recurring_baseline_expenses",
                confidence=max(0.68, recurrence.confidence),
                category=category,
                subcategory=subcategory,
                reasons=[*recurrence.reasons, "baseline_semantics"],
                review_flags=["possible_recurring_insufficient_occurrences"],
                bucket_lean="baseline",
                baseline_eligible=True,
            )
        return ExpenseAssessment(
            proposed_bucket="variable_spending",
            confidence=max(0.58, merchant_confidence),
            category=category,
            subcategory=subcategory,
            reasons=[*recurrence.reasons, "routine_spend_without_strong_baseline_evidence"],
            review_flags=["possible_recurring_insufficient_occurrences"],
        )

    large_unusual = tx.amount >= Decimal("300.00")
    if large_unusual and any(token in desc for token in ONE_OFF_TERMS):
        return ExpenseAssessment(
            proposed_bucket="one_off_spending",
            confidence=0.8,
            category=category,
            subcategory=subcategory,
            reasons=["large_unusual_purchase", "one_off_semantics"],
        )

    if any(token in desc for token in VARIABLE_TERMS) or (merchant and merchant.bucket_lean == "discretionary" and merchant_confidence >= 0.65):
        return ExpenseAssessment(
            proposed_bucket="variable_spending",
            confidence=max(0.68, merchant_confidence),
            category=category,
            subcategory=subcategory,
            reasons=["routine_lifestyle_semantics"],
        )

    if merchant is None and memory.category_hint is None:
        if tx.amount < Decimal("200.00") and transfer.state == "not_transfer":
            return ExpenseAssessment(
                proposed_bucket="variable_spending",
                confidence=0.52,
                category="Discretionary",
                subcategory="Presents",
                reasons=["merchant_unresolved", "ordinary_debit_default_variable"],
                review_flags=["low_confidence"],
            )
        return ExpenseAssessment(
            proposed_bucket="uncategorized",
            confidence=0.38,
            category="Discretionary",
            subcategory="Presents",
            reasons=["merchant_unresolved"],
            review_flags=["large_debit_unclassified"] if tx.amount >= Decimal("200.00") else ["low_confidence"],
        )

    if tx.amount < Decimal("200.00") and transfer.state == "not_transfer":
        return ExpenseAssessment(
            proposed_bucket="variable_spending",
            confidence=max(0.55, merchant_confidence),
            category=category,
            subcategory=subcategory,
            reasons=["insufficient_evidence_for_recurring_or_one_off", "ordinary_debit_default_variable"],
            review_flags=["low_confidence"],
        )

    return ExpenseAssessment(
        proposed_bucket="uncategorized",
        confidence=max(0.45, merchant_confidence),
        category=category,
        subcategory=subcategory,
        reasons=["insufficient_evidence_for_final_expense_bucket"],
        review_flags=["large_debit_unclassified"] if tx.amount >= Decimal("200.00") else ["low_confidence"],
    )
