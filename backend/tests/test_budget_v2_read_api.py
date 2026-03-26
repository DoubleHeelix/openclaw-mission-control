from decimal import Decimal

from app.schemas.budget import BudgetTransactionReviewRow
from app.services.budget_v2.read_api import group_review_rows


def _review_row(*, row_id: str, group_key: str, amount: str, signed_amount: str, priority: float) -> BudgetTransactionReviewRow:
    return BudgetTransactionReviewRow(
        id=row_id,
        transaction_date="2026-01-01",
        effective_date=None,
        amount=Decimal(amount),
        signed_amount=Decimal(signed_amount),
        direction="debit",
        movement_type="other_needs_review",
        raw_description="WISE AUSTRALIA PTY P62729256",
        normalized_description="WISE AUSTRALIA PTY P62729256",
        merchant_base_name="AUSTRALIA",
        merchant_fingerprint="fingerprint",
        direction_source="fallback",
        parser_flags=[],
        row_quality_bucket="high",
        row_shape_confidence=0.93,
        amount_parse_confidence=0.92,
        date_parse_confidence=0.92,
        section_context_confidence=0.9,
        amount_source="parsed_amount_absolute",
        date_source="transaction_date",
        balance_source="balance_after",
        description_continuation_detected=False,
        classification_version="budget_v2_deterministic_v3",
        mapping_source=None,
        transaction_classification_confidence=0.38,
        final_bucket="uncategorized",
        reasons=["merchant_unresolved"],
        review_flags=["large_debit_unclassified", "transfer_like_requires_review"],
        payment_rail="WISE",
        merchant_candidate="AUSTRALIA",
        interpretation_type="merchant_expense",
        interpretation_confidence=0.38,
        interpretation_reason="merchant_unresolved",
        classification_type="uncategorized_review",
        category="Discretionary",
        subcategory="Presents",
        confidence=0.38,
        merchant_confidence=0.38,
        bucket_confidence=0.38,
        explanation="merchant_unresolved",
        evidence_source="deterministic_rule",
        bucket_assignment="variable_discretionary",
        confidence_label="Needs review",
        inferred_cadence="irregular",
        cadence_confidence=0.3,
        cadence_reason="intervals_inconsistent",
        impact_on_baseline="included",
        included=True,
        observed_only=True,
        review_reasons=["large_debit_unclassified", "low_confidence", "transfer_like_requires_review"],
        group_key=group_key,
        group_transaction_count=8,
        review_priority=priority,
        likely_merge_targets=[],
        likely_payroll_candidate=False,
    )


def test_group_review_rows_collapses_duplicate_group_cards() -> None:
    rows = [
        _review_row(row_id="1f5cf5c9-b3e5-40ad-ae2e-9fcb7fe2d111", group_key="uncategorized|Discretionary|Presents|AUSTRALIA", amount="100.00", signed_amount="-100.00", priority=200),
        _review_row(row_id="2f5cf5c9-b3e5-40ad-ae2e-9fcb7fe2d222", group_key="uncategorized|Discretionary|Presents|AUSTRALIA", amount="100.00", signed_amount="-100.00", priority=200),
        _review_row(row_id="3f5cf5c9-b3e5-40ad-ae2e-9fcb7fe2d333", group_key="uncategorized|Discretionary|Presents|AUSTRALIA", amount="1165.68", signed_amount="-1165.68", priority=1265.68),
    ]

    grouped = group_review_rows(rows)

    assert len(grouped) == 1
    assert grouped[0].group_key == "uncategorized|Discretionary|Presents|AUSTRALIA"
    assert grouped[0].group_transaction_count == 8
    assert grouped[0].amount == Decimal("1365.68")
    assert grouped[0].signed_amount == Decimal("-1365.68")
    assert grouped[0].review_priority == 1265.68
    assert grouped[0].id == rows[2].id
