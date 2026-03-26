from decimal import Decimal

from datetime import date

from app.services.budget_v2.interpretation import interpret_transaction, promote_income_series
from app.services.budget_v2.types import ClassifiedTransaction, NormalizedTransaction


def _tx(desc: str, direction: str = "credit", *, flags: list[str] | None = None) -> NormalizedTransaction:
    return NormalizedTransaction(
        row_index=1,
        transaction_date=None,
        effective_date=None,
        amount=Decimal("100.00"),
        direction=direction,
        raw_description=desc,
        normalized_description=desc.upper(),
        payment_rail=None,
        merchant_candidate=None,
        reference=None,
        balance_after=None,
        parser_confidence=0.9,
        row_hash="h",
        metadata={"parser_flags": flags or []},
    )


def test_positive_refund_not_defaulted_to_payroll() -> None:
    result = interpret_transaction(_tx("merchant refund credited", "credit"))
    assert result.interpretation_type == "refund_reversal"
    assert result.interpretation_confidence > 0.8


def test_transfer_to_person_detected_as_p2p_not_expense() -> None:
    result = interpret_transaction(_tx("payment to john smith transfer", "debit"))
    assert result.interpretation_type == "p2p_transfer_reimbursement"


def test_payroll_requires_semantic_evidence() -> None:
    result = interpret_transaction(_tx("salary payroll acme pty ltd", "credit"))
    assert result.interpretation_type == "income_payroll"


def test_parser_anomaly_short_circuits_interpretation() -> None:
    result = interpret_transaction(_tx("foo", "debit", flags=["suspected_footer_merge"]))
    assert result.interpretation_type == "parser_anomaly"


def test_repeating_credit_series_can_promote_to_payroll_without_keyword() -> None:
    items = [
        ClassifiedTransaction(
            row_index=1,
            transaction_date=date(2026, 2, 12),
            amount=Decimal("5314.81"),
            direction="credit",
            raw_description="140516401 ACCENTURE AUSTRA",
            normalized_description="140516401 ACCENTURE AUSTRA",
            payment_rail=None,
            merchant_candidate="ACCENTURE AUSTRA",
            interpretation_type="income_other",
            interpretation_confidence=0.66,
            interpretation_reason="credit detected",
            classification_type="grouped_merchant_spend",
            category="Income",
            subcategory="Other Income",
            confidence=0.66,
            explanation="credit detected",
            evidence_source="deterministic_rule",
            group_key="income|accenture",
            inferred_cadence=None,
            cadence_confidence=None,
            cadence_reason=None,
            impact_on_baseline="included",
            included=True,
            observed_only=False,
            review_reasons=[],
            metadata={},
        ),
        ClassifiedTransaction(
            row_index=2,
            transaction_date=date(2026, 2, 26),
            amount=Decimal("5314.81"),
            direction="credit",
            raw_description="140516401 ACCENTURE AUSTRA",
            normalized_description="140516401 ACCENTURE AUSTRA",
            payment_rail=None,
            merchant_candidate="ACCENTURE AUSTRA",
            interpretation_type="income_other",
            interpretation_confidence=0.66,
            interpretation_reason="credit detected",
            classification_type="grouped_merchant_spend",
            category="Income",
            subcategory="Other Income",
            confidence=0.66,
            explanation="credit detected",
            evidence_source="deterministic_rule",
            group_key="income|accenture",
            inferred_cadence=None,
            cadence_confidence=None,
            cadence_reason=None,
            impact_on_baseline="included",
            included=True,
            observed_only=False,
            review_reasons=[],
            metadata={},
        ),
    ]
    promoted = promote_income_series(items)
    assert all(item.interpretation_type == "income_payroll" for item in promoted)
    assert all(item.subcategory == "Salary / Wages" for item in promoted)


def test_transfer_like_credit_series_does_not_promote_to_payroll() -> None:
    items = [
        ClassifiedTransaction(
            row_index=1,
            transaction_date=date(2026, 2, 12),
            amount=Decimal("102.00"),
            direction="credit",
            raw_description="NPP-ANZBAU3LXXX123",
            normalized_description="NPP ANZBAU3LXXX123",
            payment_rail="NPP",
            merchant_candidate="NPP ANZBAU3LXXX123",
            interpretation_type="income_other",
            interpretation_confidence=0.66,
            interpretation_reason="credit detected",
            classification_type="grouped_merchant_spend",
            category="Income",
            subcategory="Other Income",
            confidence=0.66,
            explanation="credit detected",
            evidence_source="deterministic_rule",
            group_key="income|npp",
            inferred_cadence=None,
            cadence_confidence=None,
            cadence_reason=None,
            impact_on_baseline="included",
            included=True,
            observed_only=False,
            review_reasons=[],
            metadata={},
        ),
        ClassifiedTransaction(
            row_index=2,
            transaction_date=date(2026, 2, 26),
            amount=Decimal("101.00"),
            direction="credit",
            raw_description="NPP-ANZBAU3LXXX123",
            normalized_description="NPP ANZBAU3LXXX123",
            payment_rail="NPP",
            merchant_candidate="NPP ANZBAU3LXXX123",
            interpretation_type="income_other",
            interpretation_confidence=0.66,
            interpretation_reason="credit detected",
            classification_type="grouped_merchant_spend",
            category="Income",
            subcategory="Other Income",
            confidence=0.66,
            explanation="credit detected",
            evidence_source="deterministic_rule",
            group_key="income|npp",
            inferred_cadence=None,
            cadence_confidence=None,
            cadence_reason=None,
            impact_on_baseline="included",
            included=True,
            observed_only=False,
            review_reasons=[],
            metadata={},
        ),
    ]
    promoted = promote_income_series(items)
    assert all(item.interpretation_type == "income_other" for item in promoted)
