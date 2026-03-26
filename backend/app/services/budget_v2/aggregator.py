"""Aggregation of resolved transactions into exact section totals."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from app.services.budget_v2.resolver import ResolvedTransaction

FINAL_BUCKETS = (
    "income",
    "recurring_baseline_expenses",
    "variable_spending",
    "one_off_spending",
    "transfers",
    "fees",
    "uncategorized",
)


@dataclass
class SectionAggregate:
    totals: dict[str, Decimal] = field(default_factory=lambda: {bucket: Decimal("0.00") for bucket in FINAL_BUCKETS})
    credit_totals: dict[str, Decimal] = field(default_factory=lambda: {bucket: Decimal("0.00") for bucket in FINAL_BUCKETS})
    debit_totals: dict[str, Decimal] = field(default_factory=lambda: {bucket: Decimal("0.00") for bucket in FINAL_BUCKETS})
    transaction_count_by_bucket: dict[str, int] = field(default_factory=lambda: {bucket: 0 for bucket in FINAL_BUCKETS})


def aggregate_sections(transactions: list[ResolvedTransaction]) -> SectionAggregate:
    aggregate = SectionAggregate()
    for tx in transactions:
        amount = tx.amount
        bucket = tx.final_bucket
        aggregate.totals[bucket] += amount
        aggregate.transaction_count_by_bucket[bucket] += 1
        if tx.direction == "credit":
            aggregate.credit_totals[bucket] += amount
        else:
            aggregate.debit_totals[bucket] += amount
    return aggregate
