from decimal import Decimal

from app.services.budget_v2.expense_classifier import classify_expense
from app.services.budget_v2.merchant_memory import MerchantMemoryHints
from app.services.budget_v2.recurrence import RecurrenceAssessment
from app.services.budget_v2.transfer_detector import TransferAssessment
from app.services.budget_v2.types import NormalizedTransaction


def _tx(desc: str, amount: str = "42.00") -> NormalizedTransaction:
    return NormalizedTransaction(
        row_index=1,
        transaction_date=None,
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
        row_hash="h",
        signed_amount=Decimal(amount) * Decimal("-1"),
        metadata={},
    )


def test_unknown_does_not_default_to_one_off() -> None:
    result = classify_expense(
        tx=_tx("Random ambiguous descriptor", "180.00"),
        transfer=TransferAssessment("not_transfer", 0.0, []),
        recurrence=None,
        memory=MerchantMemoryHints(),
    )
    assert result.proposed_bucket == "variable_spending"
    assert "ordinary_debit_default_variable" in result.reasons


def test_large_unknown_debit_stays_uncategorized() -> None:
    result = classify_expense(
        tx=_tx("Random ambiguous descriptor", "280.00"),
        transfer=TransferAssessment("not_transfer", 0.0, []),
        recurrence=None,
        memory=MerchantMemoryHints(),
    )
    assert result.proposed_bucket == "uncategorized"
    assert "large_debit_unclassified" in result.review_flags


def test_fee_classifies_as_fee() -> None:
    result = classify_expense(
        tx=_tx("International transaction fee", "12.00"),
        transfer=TransferAssessment("not_transfer", 0.0, []),
        recurrence=None,
        memory=MerchantMemoryHints(),
    )
    assert result.proposed_bucket == "fees"


def test_large_mac_mini_purchase_is_one_off() -> None:
    result = classify_expense(
        tx=_tx("Apple Mac Mini JB HI FI", "1899.00"),
        transfer=TransferAssessment("not_transfer", 0.0, []),
        recurrence=RecurrenceAssessment("non_recurring", 0.2),
        memory=MerchantMemoryHints(),
    )
    assert result.proposed_bucket == "one_off_spending"


def test_recurring_likely_without_baseline_semantics_defaults_to_variable() -> None:
    result = classify_expense(
        tx=_tx("WW METRO SOUTHBANK", "13.29"),
        transfer=TransferAssessment("not_transfer", 0.0, []),
        recurrence=RecurrenceAssessment("recurring_likely", 0.61, cadence="weekly", reasons=["intervals_soft_match"]),
        memory=MerchantMemoryHints(),
    )
    assert result.proposed_bucket == "variable_spending"
    assert "possible_recurring_insufficient_occurrences" in result.review_flags
