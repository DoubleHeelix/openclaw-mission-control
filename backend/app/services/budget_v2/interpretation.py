"""Deterministic transaction interpretation rules (stage before merchant classification)."""

from __future__ import annotations

from collections import defaultdict
import re
from decimal import Decimal

from app.services.budget_v2.config import (
    CADENCE_WINDOWS,
    CASH_KEYWORDS,
    DEBT_PAYMENT_KEYWORDS,
    FEE_KEYWORDS,
    PAYROLL_KEYWORDS,
    PERSON_TRANSFER_HINTS,
    REFUND_KEYWORDS,
    TRANSFER_KEYWORDS,
)
from app.services.budget_v2.types import ClassifiedTransaction, InterpretedTransaction, NormalizedTransaction

PERSON_NAME_PATTERN = re.compile(r"\b(TO|FROM)\s+[A-Z]{2,}(?:\s+[A-Z]{2,}){0,4}\b")
EMPLOYERISH_PATTERN = re.compile(r"\b(?:PTY|LTD|LIMITED|GROUP|HOLDINGS|SERVICES|AUSTRA|AUSTRALIA|PAYMENTS?)\b")


def _has_parser_anomaly(tx: NormalizedTransaction) -> bool:
    flags = tx.metadata.get("parser_flags") if isinstance(tx.metadata, dict) else []
    if not isinstance(flags, list):
        return False
    anomaly_flags = {
        "suspected_footer_merge",
        "unsupported_section_encountered",
        "truncated_row",
        "malformed_amount",
        "malformed_date",
        "missing_amount",
    }
    return any(str(flag) in anomaly_flags for flag in flags)


def interpret_transaction(tx: NormalizedTransaction) -> InterpretedTransaction:
    desc = tx.normalized_description

    if _has_parser_anomaly(tx):
        return InterpretedTransaction(
            interpretation_type="parser_anomaly",
            interpretation_confidence=0.95,
            interpretation_reason="Parser anomaly flags detected on transaction row.",
        )

    if any(word in desc for word in REFUND_KEYWORDS):
        return InterpretedTransaction(
            interpretation_type="refund_reversal",
            interpretation_confidence=0.92,
            interpretation_reason="Descriptor contains refund/reversal marker.",
        )

    if any(word in desc for word in CASH_KEYWORDS):
        return InterpretedTransaction(
            interpretation_type="cash_withdrawal",
            interpretation_confidence=0.9,
            interpretation_reason="Descriptor contains ATM/cash marker.",
        )

    if any(word in desc for word in DEBT_PAYMENT_KEYWORDS):
        return InterpretedTransaction(
            interpretation_type="debt_payment",
            interpretation_confidence=0.88,
            interpretation_reason="Descriptor contains debt payment semantics.",
        )

    if any(word in desc for word in TRANSFER_KEYWORDS) or tx.payment_rail in {"NPP", "OSKO", "PAYID", "WISE"}:
        if PERSON_NAME_PATTERN.search(desc) or any(hint in desc for hint in PERSON_TRANSFER_HINTS):
            return InterpretedTransaction(
                interpretation_type="p2p_transfer_reimbursement",
                interpretation_confidence=0.86,
                interpretation_reason="Transfer semantics with person-like counterparty pattern.",
            )
        return InterpretedTransaction(
            interpretation_type="internal_transfer",
            interpretation_confidence=0.83,
            interpretation_reason="Transfer/payment-rail semantics found.",
        )

    if tx.direction == "debit" and any(word in desc for word in FEE_KEYWORDS):
        return InterpretedTransaction(
            interpretation_type="merchant_fee",
            interpretation_confidence=0.84,
            interpretation_reason="Descriptor contains fee/charge semantics.",
        )

    if tx.direction == "credit" and any(word in desc for word in PAYROLL_KEYWORDS):
        return InterpretedTransaction(
            interpretation_type="income_payroll",
            interpretation_confidence=0.9,
            interpretation_reason="Credit with payroll semantics.",
        )

    if tx.direction == "credit":
        if EMPLOYERISH_PATTERN.search(desc):
            return InterpretedTransaction(
                interpretation_type="income_other",
                interpretation_confidence=0.74,
                interpretation_reason="Credit from employer-like entity; recurring evidence required to confirm payroll.",
            )
        return InterpretedTransaction(
            interpretation_type="income_other",
            interpretation_confidence=0.66,
            interpretation_reason="Credit detected without transfer, refund, or payroll certainty.",
        )

    return InterpretedTransaction(
        interpretation_type="merchant_expense",
        interpretation_confidence=0.74,
        interpretation_reason="Default debit consumer spend interpretation.",
    )


def promote_income_series(items: list[ClassifiedTransaction]) -> list[ClassifiedTransaction]:
    groups: dict[str, list[ClassifiedTransaction]] = defaultdict(list)
    for item in items:
        if item.direction != "credit":
            continue
        if item.interpretation_type not in {"income_other", "income_payroll"}:
            continue
        if item.category != "Income":
            continue
        key = (item.merchant_candidate or item.normalized_description or "").upper().strip()
        if not key:
            continue
        groups[key].append(item)

    for group_key, group_items in groups.items():
        if len(group_items) < 2:
            continue
        dates = [item.transaction_date for item in group_items if item.transaction_date is not None]
        if len(dates) < 2:
            continue
        sorted_dates = sorted(dates)
        intervals = [(sorted_dates[idx] - sorted_dates[idx - 1]).days for idx in range(1, len(sorted_dates))]
        if not intervals:
            continue
        avg_interval = sum(intervals) / len(intervals)
        cadence_match = None
        for cadence, window in CADENCE_WINDOWS.items():
            if window.min_days <= avg_interval <= window.max_days:
                cadence_match = cadence
                break
        if cadence_match not in {"weekly", "fortnightly", "monthly"}:
            continue

        amounts = [item.amount for item in group_items]
        avg_amount = sum(amounts, Decimal("0")) / Decimal(str(len(amounts)))
        stable_amounts = all(abs(amount - avg_amount) <= max(Decimal("5.00"), avg_amount * Decimal("0.08")) for amount in amounts)
        descriptor = group_key
        employerish = bool(EMPLOYERISH_PATTERN.search(descriptor))
        transferish = any(token in descriptor for token in TRANSFER_KEYWORDS) or PERSON_NAME_PATTERN.search(descriptor)
        if not stable_amounts or transferish:
            continue
        if not employerish and len(group_items) < 3:
            continue

        confidence = 0.88 if employerish else 0.8
        reason = f"Recurring credit series matched {cadence_match} cadence with stable amounts."
        for item in group_items:
            item.interpretation_type = "income_payroll"
            item.interpretation_confidence = max(item.interpretation_confidence, confidence)
            item.interpretation_reason = reason
            item.subcategory = "Salary / Wages"
            item.confidence = max(item.confidence, confidence)
            item.explanation = f"{reason} {item.explanation}".strip()
            item.metadata = {
                **(item.metadata or {}),
                "income_signal_strength": "high" if confidence >= 0.85 else "medium",
                "payroll_likelihood": confidence,
            }
    return items
