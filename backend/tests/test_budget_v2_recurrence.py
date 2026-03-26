from datetime import date
from decimal import Decimal

from app.services.budget_v2.merchant_memory import MerchantMemoryHints
from app.services.budget_v2.recurrence import assess_recurrence
from app.services.budget_v2.types import NormalizedTransaction


def _debit(row_index: int, desc: str, amount: str, d: date) -> NormalizedTransaction:
    return NormalizedTransaction(
        row_index=row_index,
        transaction_date=d,
        effective_date=None,
        amount=Decimal(amount),
        direction="debit",
        raw_description=desc,
        normalized_description=desc.upper(),
        payment_rail=None,
        merchant_candidate=desc.upper(),
        reference=None,
        balance_after=None,
        parser_confidence=0.9,
        row_hash=str(row_index),
        signed_amount=Decimal(amount) * Decimal("-1"),
        merchant_fingerprint=desc.upper(),
        metadata={},
    )


def test_monthly_repeat_is_confirmed_on_longer_window() -> None:
    tx1 = _debit(1, "Goodlife Gym", "19.99", date(2026, 1, 2))
    tx2 = _debit(2, "Goodlife Gym", "19.99", date(2026, 2, 2))
    result = assess_recurrence(
        transactions=[tx1, tx2],
        memory_hints={1: MerchantMemoryHints(), 2: MerchantMemoryHints()},
        statement_window_days=60,
        historical_recurrence={},
    )
    assert result[1].state == "recurring_confirmed"


def test_short_window_only_yields_likely() -> None:
    tx1 = _debit(1, "Goodlife Gym", "19.99", date(2026, 2, 1))
    tx2 = _debit(2, "Goodlife Gym", "19.99", date(2026, 2, 8))
    result = assess_recurrence(
        transactions=[tx1, tx2],
        memory_hints={1: MerchantMemoryHints(), 2: MerchantMemoryHints()},
        statement_window_days=8,
        historical_recurrence={},
    )
    assert result[1].state == "recurring_likely"
