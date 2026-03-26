from decimal import Decimal
from types import SimpleNamespace

from app.services.budget_v2.aggregator import aggregate_sections
from app.services.budget_v2.diagnostics import build_diagnostics


def test_diagnostics_reconciles_exactly() -> None:
    txs = [
        SimpleNamespace(final_bucket="income", amount=Decimal("5000.00"), direction="credit", confidence=0.9, review_flags=[]),
        SimpleNamespace(final_bucket="variable_spending", amount=Decimal("100.00"), direction="debit", confidence=0.8, review_flags=[]),
    ]
    aggregate = aggregate_sections(txs)
    result = build_diagnostics(
        transactions=txs,
        aggregate=aggregate,
        total_credits=Decimal("5000.00"),
        total_debits=Decimal("100.00"),
    )
    assert result.classification_reconciliation_status == "reconciled"
    assert result.classification_reconciliation_difference == Decimal("0.00")
