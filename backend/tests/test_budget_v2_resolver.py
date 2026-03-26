from decimal import Decimal

from app.services.budget_v2.expense_classifier import ExpenseAssessment
from app.services.budget_v2.income_detector import IncomeAssessment
from app.services.budget_v2.merchant_memory import MerchantMemoryHints
from app.services.budget_v2.resolver import resolve_transaction
from app.services.budget_v2.transfer_detector import TransferAssessment
from app.services.budget_v2.types import NormalizedTransaction


def _tx(direction: str = "debit") -> NormalizedTransaction:
    return NormalizedTransaction(
        row_index=1,
        transaction_date=None,
        effective_date=None,
        amount=Decimal("100.00"),
        direction=direction,
        raw_description="desc",
        normalized_description="DESC",
        payment_rail=None,
        merchant_candidate="DESC",
        reference=None,
        balance_after=None,
        parser_confidence=0.9,
        row_hash="h",
        signed_amount=Decimal("100.00") if direction == "credit" else Decimal("-100.00"),
        metadata={},
    )


def test_transfer_has_priority_over_expense() -> None:
    resolved = resolve_transaction(
        tx=_tx(),
        memory=MerchantMemoryHints(),
        transfer=TransferAssessment("transfer_confirmed_internal", 0.9, ["self_transfer_hint"]),
        income=None,
        recurrence=None,
        expense=ExpenseAssessment("variable_spending", 0.9, "General / Home", "Grocery Shopping"),
    )
    assert resolved.final_bucket == "transfers"


def test_uncategorized_survives_without_positive_evidence() -> None:
    resolved = resolve_transaction(
        tx=_tx(),
        memory=MerchantMemoryHints(),
        transfer=TransferAssessment("not_transfer", 0.1, []),
        income=None,
        recurrence=None,
        expense=ExpenseAssessment("uncategorized", 0.4, "Discretionary", "Presents", review_flags=["low_confidence"]),
    )
    assert resolved.final_bucket == "uncategorized"


def test_salary_recurring_likely_maps_to_recurring_income_bucket_assignment() -> None:
    resolved = resolve_transaction(
        tx=_tx(direction="credit"),
        memory=MerchantMemoryHints(),
        transfer=TransferAssessment("not_transfer", 0.1, []),
        income=IncomeAssessment("salary_recurring_likely", 0.78, ["payroll_semantics", "large_credit"], "Income", "Salary / Wages"),
        recurrence=None,
        expense=None,
    )

    assert resolved.final_bucket == "income"
    assert resolved.bucket_assignment == "income_recurring"
    assert "salary_like_single_occurrence" in resolved.review_flags
