"""Diagnostics, reconciliation, and confidence scoring for classified transactions."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from app.services.budget_v2.aggregator import SectionAggregate
from app.services.budget_v2.resolver import ResolvedTransaction


@dataclass
class StatementDiagnostics:
    classification_reconciliation_status: str
    classification_reconciliation_difference: Decimal
    section_confidence: dict[str, float] = field(default_factory=dict)
    statement_model_confidence: float = 0.0
    review_queue: list[dict[str, object]] = field(default_factory=list)
    hard_warnings: list[str] = field(default_factory=list)


def build_diagnostics(*, transactions: list[ResolvedTransaction], aggregate: SectionAggregate, total_credits: Decimal, total_debits: Decimal) -> StatementDiagnostics:
    classified_credits = sum((tx.amount for tx in transactions if tx.direction == "credit"), Decimal("0.00"))
    classified_debits = sum((tx.amount for tx in transactions if tx.direction == "debit"), Decimal("0.00"))
    credit_diff = classified_credits - total_credits
    debit_diff = classified_debits - total_debits
    classification_diff = abs(credit_diff) + abs(debit_diff)
    status = "reconciled" if classification_diff == Decimal("0.00") else "mismatch"

    section_confidence: dict[str, float] = {}
    total_amount = total_credits + total_debits
    uncategorized_total = aggregate.totals.get("uncategorized", Decimal("0.00"))
    for bucket, amount in aggregate.totals.items():
        if amount <= Decimal("0.00"):
            section_confidence[bucket] = 1.0
            continue
        bucket_items = [tx for tx in transactions if tx.final_bucket == bucket]
        avg_conf = sum((tx.confidence for tx in bucket_items), 0.0) / max(len(bucket_items), 1)
        review_penalty = min(len([flag for tx in bucket_items for flag in tx.review_flags]) * 0.03, 0.25)
        section_confidence[bucket] = round(max(0.0, min(1.0, avg_conf - review_penalty)), 2)

    review_queue = [
        {
            "transaction_id": f"row_{tx.row_index}",
            "review_flags": list(tx.review_flags),
            "transaction_classification_confidence": round(tx.confidence, 2),
        }
        for tx in transactions
        if tx.review_flags or tx.final_bucket == "uncategorized"
    ]
    statement_model_confidence = max(0.0, min(1.0, 1.0 - float((uncategorized_total / total_amount) if total_amount > 0 else Decimal("0.00")) - (0.03 * len(review_queue))))
    warnings: list[str] = []
    if status != "reconciled":
        warnings.append("classification_total_mismatch")

    return StatementDiagnostics(
        classification_reconciliation_status=status,
        classification_reconciliation_difference=classification_diff.quantize(Decimal("0.01")),
        section_confidence=section_confidence,
        statement_model_confidence=round(statement_model_confidence, 2),
        review_queue=review_queue,
        hard_warnings=warnings,
    )
