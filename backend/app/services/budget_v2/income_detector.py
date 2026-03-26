"""Income assessment stage for normalized credit transactions."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
import re

from app.services.budget_v2.config import CADENCE_WINDOWS, PAYROLL_KEYWORDS, REFUND_KEYWORDS, TRANSFER_KEYWORDS
from app.services.budget_v2.merchant_memory import MerchantMemoryHints
from app.services.budget_v2.transfer_detector import TransferAssessment
from app.services.budget_v2.types import NormalizedTransaction

EMPLOYERISH_PATTERN = re.compile(r"\b(?:PTY|LTD|LIMITED|GROUP|HOLDINGS|SERVICES|PAYROLL|PAYMENTS?|AUSTRA|AUSTRALIA)\b")
REIMBURSEMENT_PATTERN = re.compile(r"\b(?:REIMB|REIMBURSE|EXPENSE CLAIM)\b")


@dataclass
class IncomeAssessment:
    state: str
    confidence: float
    reasons: list[str] = field(default_factory=list)
    category: str = "Income"
    subcategory: str = "Other Income"


def _intervals(dates: list[date]) -> list[int]:
    ordered = sorted(dates)
    return [(ordered[idx] - ordered[idx - 1]).days for idx in range(1, len(ordered))]


def _cadence_from_intervals(intervals: list[int]) -> tuple[str | None, float]:
    if not intervals:
        return None, 0.0
    avg = sum(intervals) / len(intervals)
    for cadence, window in CADENCE_WINDOWS.items():
        if window.min_days <= avg <= window.max_days:
            spread = max(intervals) - min(intervals)
            return cadence, max(0.4, 1.0 - (spread / max(window.max_days, 1)))
    return None, 0.0


def assess_income_transactions(
    *,
    transactions: list[NormalizedTransaction],
    transfer_assessments: dict[int, TransferAssessment],
    memory_hints: dict[int, MerchantMemoryHints],
    statement_window_days: int | None,
    historical_recurrence: dict[str, dict[str, object]],
) -> dict[int, IncomeAssessment]:
    assessments: dict[int, IncomeAssessment] = {}
    grouped: dict[str, list[NormalizedTransaction]] = defaultdict(list)

    for tx in transactions:
        if tx.direction != "credit":
            continue
        grouped[(tx.merchant_fingerprint or tx.merchant_candidate or tx.normalized_description or "").upper()].append(tx)

    credit_amounts = [tx.amount for tx in transactions if tx.direction == "credit"]
    median_credit = sorted(credit_amounts)[len(credit_amounts) // 2] if credit_amounts else Decimal("0.00")

    for key, items in grouped.items():
        dates = [item.transaction_date for item in items if isinstance(item.transaction_date, date)]
        intervals = _intervals(dates)
        cadence, cadence_score = _cadence_from_intervals(intervals)
        amounts = [item.amount for item in items]
        avg_amount = sum(amounts, Decimal("0.00")) / Decimal(str(max(len(amounts), 1)))
        stable_amounts = all(abs(amount - avg_amount) <= max(Decimal("5.00"), avg_amount * Decimal("0.05")) for amount in amounts)
        historical = historical_recurrence.get(key, {})
        historical_occurrence_count = int(historical.get("occurrence_count", 0) or 0)

        for item in items:
            desc = (item.normalized_description or "").upper()
            transfer = transfer_assessments.get(item.row_index)
            memory = memory_hints.get(item.row_index, MerchantMemoryHints())
            reasons: list[str] = []

            if any(token in desc for token in REFUND_KEYWORDS):
                assessments[item.row_index] = IncomeAssessment("refund", 0.93, ["refund_keyword"], "Income", "Refund / Reversal")
                continue
            if REIMBURSEMENT_PATTERN.search(desc):
                assessments[item.row_index] = IncomeAssessment("reimbursement", 0.82, ["reimbursement_keyword"], "Income", "Reimbursement")
                continue
            if transfer and transfer.state == "transfer_confirmed_internal":
                assessments[item.row_index] = IncomeAssessment("transfer_in", transfer.confidence, list(transfer.reasons), "Income", "Transfer In")
                continue

            payroll_semantics = any(token in desc for token in PAYROLL_KEYWORDS) or bool(EMPLOYERISH_PATTERN.search(desc))
            large_credit = item.amount >= max(median_credit, Decimal("1000.00"))
            transferish = transfer is not None and transfer.state in {"transfer_confirmed_external", "transfer_likely"}
            repeated = len(items) + historical_occurrence_count
            confirmed = False
            likely = False
            confidence = 0.45

            if cadence in {"weekly", "fortnightly"} and stable_amounts and repeated >= 2 and not transferish:
                confirmed = True
                reasons.extend(["cadence_support", f"cadence:{cadence}", "stable_amounts"])
                confidence = 0.9 if payroll_semantics else 0.82
            elif cadence == "monthly" and stable_amounts and repeated >= (2 if (statement_window_days or 0) >= 45 else 3) and not transferish:
                confirmed = True
                reasons.extend(["cadence_support", "cadence:monthly", "stable_amounts"])
                confidence = 0.88 if payroll_semantics else 0.8
            elif payroll_semantics and large_credit and not transferish:
                likely = True
                reasons.extend(["payroll_semantics", "large_credit"])
                confidence = 0.78
                if (statement_window_days or 0) < 14:
                    reasons.append("statement_shorter_than_cycle")
            elif repeated >= 2 and stable_amounts and not transferish and (memory.income_tendency > 0.4 or payroll_semantics):
                likely = True
                reasons.extend(["historical_income_pattern", "stable_amounts"])
                confidence = 0.74

            if confirmed:
                assessments[item.row_index] = IncomeAssessment("salary_recurring_confirmed", confidence, reasons, "Income", "Salary / Wages")
            elif likely:
                assessments[item.row_index] = IncomeAssessment("salary_recurring_likely", confidence, reasons, "Income", "Salary / Wages")
            elif transferish:
                assessments[item.row_index] = IncomeAssessment("credit_uncategorized", 0.4, ["transfer_like_requires_review", *(transfer.reasons if transfer else [])], "Income", "Other Income")
            else:
                reason = "credit_repeated" if len(items) > 1 else "credit_single_occurrence"
                assessments[item.row_index] = IncomeAssessment("income_irregular", 0.62 + memory.confidence_adjustment, [reason], "Income", "Other Income")
    return assessments
