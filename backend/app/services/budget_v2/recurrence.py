"""Recurrence scoring for normalized transactions."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from app.services.budget_v2.config import CADENCE_WINDOWS
from app.services.budget_v2.merchant_memory import MerchantMemoryHints
from app.services.budget_v2.types import NormalizedTransaction


@dataclass
class RecurrenceAssessment:
    state: str
    confidence: float
    cadence: str | None = None
    reasons: list[str] = field(default_factory=list)
    occurrence_count: int = 0
    historical_occurrence_count: int = 0
    median_amount: Decimal = Decimal("0.00")


def _median(values: list[Decimal]) -> Decimal:
    ordered = sorted(values)
    if not ordered:
        return Decimal("0.00")
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / Decimal("2")


def _intervals(dates: list[date]) -> list[int]:
    ordered = sorted(dates)
    return [(ordered[idx] - ordered[idx - 1]).days for idx in range(1, len(ordered))]


def _cadence(intervals: list[int]) -> tuple[str | None, float]:
    if not intervals:
        return None, 0.0
    avg = sum(intervals) / len(intervals)
    for cadence, window in CADENCE_WINDOWS.items():
        if cadence == "yearly":
            continue
        if window.min_days <= avg <= window.max_days:
            spread = max(intervals) - min(intervals)
            return cadence, max(0.45, 1.0 - (spread / max(window.max_days, 1)))
    return None, 0.0


def assess_recurrence(
    *,
    transactions: list[NormalizedTransaction],
    memory_hints: dict[int, MerchantMemoryHints],
    statement_window_days: int | None,
    historical_recurrence: dict[str, dict[str, object]],
) -> dict[int, RecurrenceAssessment]:
    grouped: dict[str, list[NormalizedTransaction]] = defaultdict(list)
    for tx in transactions:
        if tx.direction != "debit":
            continue
        key = (tx.merchant_fingerprint or tx.merchant_candidate or tx.normalized_description or "").upper()
        grouped[key].append(tx)

    out: dict[int, RecurrenceAssessment] = {}
    for key, items in grouped.items():
        dates = [item.transaction_date for item in items if isinstance(item.transaction_date, date)]
        intervals = _intervals(dates)
        cadence, cadence_conf = _cadence(intervals)
        amounts = [item.amount for item in items]
        median_amount = _median(amounts)
        stable_amounts = all(abs(amount - median_amount) <= max(Decimal("5.00"), median_amount * Decimal("0.05")) for amount in amounts) if median_amount > 0 else False
        historical = historical_recurrence.get(key, {})
        historical_count = int(historical.get("occurrence_count", 0) or 0)
        occurrence_count = len(items)

        for item in items:
            memory = memory_hints.get(item.row_index, MerchantMemoryHints())
            reasons: list[str] = []
            confidence = 0.3
            state = "non_recurring"

            if cadence and stable_amounts:
                reasons.extend([f"cadence:{cadence}", "stable_amounts"])
                total_occurrences = occurrence_count + historical_count
                if (statement_window_days or 0) < 14:
                    state = "recurring_likely"
                    reasons.append("statement_shorter_than_cycle")
                    confidence = 0.68
                elif 14 <= (statement_window_days or 0) < 45:
                    if cadence in {"weekly", "fortnightly"} and total_occurrences >= 2:
                        state = "recurring_confirmed"
                        confidence = 0.84
                    else:
                        state = "recurring_likely"
                        confidence = 0.71
                else:
                    if total_occurrences >= 2:
                        state = "recurring_confirmed"
                        confidence = 0.86
                    else:
                        state = "recurring_likely"
                        confidence = 0.7
            elif memory.baseline_tendency > 0.5 and (occurrence_count + historical_count) >= 2:
                state = "recurring_likely"
                confidence = 0.66
                reasons.extend(["merchant_memory_baseline_hint", *memory.reasons])

            out[item.row_index] = RecurrenceAssessment(
                state=state,
                confidence=confidence,
                cadence=cadence,
                reasons=reasons,
                occurrence_count=occurrence_count,
                historical_occurrence_count=historical_count,
                median_amount=median_amount,
            )
    return out
