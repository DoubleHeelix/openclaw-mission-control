from decimal import Decimal

from app.services.budget_v2.classification import classify_transaction
from app.services.budget_v2.config import canonicalize_expense_taxonomy
from app.services.budget_v2.types import InterpretedTransaction, NormalizedTransaction


def _tx(desc: str, direction: str = "debit") -> NormalizedTransaction:
    return NormalizedTransaction(
        row_index=1,
        transaction_date=None,
        effective_date=None,
        amount=Decimal("42.00"),
        direction=direction,
        raw_description=desc,
        normalized_description=desc.upper(),
        payment_rail=None,
        merchant_candidate=desc.upper(),
        reference=None,
        balance_after=None,
        parser_confidence=0.9,
        row_hash="hash",
        metadata={"parser_flags": []},
    )


def _interp(kind: str, confidence: float = 0.7) -> InterpretedTransaction:
    return InterpretedTransaction(
        interpretation_type=kind,
        interpretation_confidence=confidence,
        interpretation_reason="test",
    )


def test_merchant_spend_classification_uses_family_rules() -> None:
    result = classify_transaction(_tx("Cranbourne Pharmacy"), _interp("merchant_expense", 0.7))
    assert result.category == "Health & Fitness"
    assert result.subcategory == "Supplements"
    assert result.impact_on_baseline == "included"


def test_unknown_merchant_routes_to_review_reasons() -> None:
    result = classify_transaction(_tx("Random ambiguous descriptor"), _interp("merchant_expense", 0.4))
    assert result.classification_type == "uncategorized_review"
    assert "unknown_merchant" in result.review_reasons


def test_intl_txn_fee_classifies_as_fee_and_not_unknown() -> None:
    result = classify_transaction(_tx("INTL TXN FEE"), _interp("merchant_expense", 0.8))
    assert result.category == "Discretionary"
    assert result.subcategory == "Presents"
    assert result.metadata is not None
    assert result.metadata["ancillary"] is True


def test_transfer_classification_not_merchant() -> None:
    result = classify_transaction(_tx("PAYMENT TO SOMEONE"), _interp("internal_transfer", 0.9))
    assert result.category == "Transfer / Money Movement"
    assert result.bucket_assignment == "transfer_money_movement"


def test_employer_like_credit_becomes_payroll_candidate_not_generic_income() -> None:
    result = classify_transaction(_tx("ACCENTURE AUSTRA", "credit"), _interp("income_other", 0.74))
    assert result.category == "Income"
    assert result.subcategory == "Salary / Wages candidate"
    assert result.bucket_assignment == "income_irregular"
    assert "likely_payroll_candidate" in result.review_reasons


def test_legacy_utilities_taxonomy_is_remapped_to_power() -> None:
    category, subcategory = canonicalize_expense_taxonomy("Expenses", "Utilities / Telecom", "AGL ENERGY")
    assert category == "General / Home"
    assert subcategory == "Power"


def test_legacy_digital_services_taxonomy_is_remapped_to_chatgpt() -> None:
    category, subcategory = canonicalize_expense_taxonomy("Expenses", "Digital Services / Software", "OPENAI CHATGPT SUBSCRIPTION")
    assert category == "Discretionary"
    assert subcategory == "Chatgpt"


def test_legacy_grocery_taxonomy_is_remapped() -> None:
    category, subcategory = canonicalize_expense_taxonomy("Expenses", "Groceries", "ALDI")
    assert category == "General / Home"
    assert subcategory == "Grocery Shopping"


def test_broad_fallback_matches_pluralized_everyday_spend_tokens() -> None:
    result = classify_transaction(_tx("LOCAL GROCERIES MARKET"), _interp("merchant_expense", 0.58))
    assert result.category == "General / Home"
    assert result.subcategory == "Grocery Shopping"
    assert result.classification_type != "uncategorized_review"
