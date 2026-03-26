from decimal import Decimal
from types import SimpleNamespace

from app.services.budget_v2.aggregator import aggregate_sections


def test_aggregator_sums_each_final_bucket_once() -> None:
    txs = [
        SimpleNamespace(final_bucket="income", amount=Decimal("5000.00"), direction="credit"),
        SimpleNamespace(final_bucket="variable_spending", amount=Decimal("100.00"), direction="debit"),
        SimpleNamespace(final_bucket="one_off_spending", amount=Decimal("900.00"), direction="debit"),
    ]
    result = aggregate_sections(txs)
    assert result.totals["income"] == Decimal("5000.00")
    assert result.totals["variable_spending"] == Decimal("100.00")
    assert result.totals["one_off_spending"] == Decimal("900.00")
