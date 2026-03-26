from datetime import date
from decimal import Decimal

from app.services.budget_v2.income_detector import assess_income_transactions
from app.services.budget_v2.merchant_memory import MerchantMemoryHints
from app.services.budget_v2.transfer_detector import TransferAssessment
from app.services.budget_v2.types import NormalizedTransaction


def _tx(row_index: int, desc: str, amount: str, d: date) -> NormalizedTransaction:
    return NormalizedTransaction(
        row_index=row_index,
        transaction_date=d,
        effective_date=None,
        amount=Decimal(amount),
        direction="credit",
        raw_description=desc,
        normalized_description=desc.upper(),
        payment_rail=None,
        merchant_candidate=desc.upper(),
        reference=None,
        balance_after=None,
        parser_confidence=0.9,
        row_hash=str(row_index),
        signed_amount=Decimal(amount),
        merchant_fingerprint=desc.upper(),
        metadata={},
    )


def test_salary_single_occurrence_can_be_likely() -> None:
    tx = _tx(1, "Accenture Australia Pty Ltd", "5314.81", date(2026, 2, 12))
    result = assess_income_transactions(
        transactions=[tx],
        transfer_assessments={1: TransferAssessment("not_transfer", 0.1, [])},
        memory_hints={1: MerchantMemoryHints()},
        statement_window_days=10,
        historical_recurrence={},
    )
    assert result[1].state == "salary_recurring_likely"


def test_repeated_payroll_becomes_confirmed() -> None:
    tx1 = _tx(1, "Accenture Australia Pty Ltd", "5314.81", date(2026, 2, 12))
    tx2 = _tx(2, "Accenture Australia Pty Ltd", "5314.81", date(2026, 2, 26))
    result = assess_income_transactions(
        transactions=[tx1, tx2],
        transfer_assessments={1: TransferAssessment("not_transfer", 0.1, []), 2: TransferAssessment("not_transfer", 0.1, [])},
        memory_hints={1: MerchantMemoryHints(), 2: MerchantMemoryHints()},
        statement_window_days=30,
        historical_recurrence={},
    )
    assert result[1].state == "salary_recurring_confirmed"
    assert result[2].state == "salary_recurring_confirmed"
