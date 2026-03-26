from datetime import date
from decimal import Decimal

from app.services.budget_v2.assembly import build_lines, build_snapshot_summary
from app.services.budget_v2.types import ClassifiedTransaction


def _tx(
    row: int,
    tx_date: date,
    amount: str,
    *,
    desc: str = "GOOGLE YOUTUBEPREMIUM",
    merchant: str = "YOUTUBE PREMIUM",
    category: str = "Entertainment",
    subcategory: str = "Movies / Activities",
    group_key: str = "expenses|youtube",
    direction: str = "debit",
    cadence_confidence: float | None = None,
    metadata: dict | None = None,
) -> ClassifiedTransaction:
    return ClassifiedTransaction(
        row_index=row,
        transaction_date=tx_date,
        amount=Decimal(amount),
        direction=direction,
        raw_description=desc,
        normalized_description=desc,
        payment_rail="VISA DEBIT PURCHASE",
        merchant_candidate=merchant,
        interpretation_type="income_other" if category == "Income" else "merchant_expense",
        interpretation_confidence=0.85,
        interpretation_reason="test",
        classification_type="grouped_merchant_spend",
        category=category,
        subcategory=subcategory,
        confidence=0.9,
        explanation="test",
        evidence_source="deterministic_rule",
        group_key=group_key,
        inferred_cadence=None,
        cadence_confidence=cadence_confidence,
        cadence_reason=None,
        impact_on_baseline="included",
        included=True,
        observed_only=True,
        review_reasons=[],
        metadata=metadata or {"bucket_lean": "baseline", "baseline_eligible": True, "ancillary": False},
    )


def test_single_occurrence_transaction_stays_observational() -> None:
    lines = build_lines([_tx(1, date(2026, 3, 13), "25.99")])
    assert len(lines) == 1
    line = lines[0]
    assert line.is_modeled is False
    assert line.modeling_status == "observational_only"
    assert line.recurrence_state == "one_off_candidate"
    assert line.normalized_monthly == Decimal("0.00")
    assert line.bucket_assignment == "one_off_exceptional"
    assert "single_occurrence_only" in line.review_reasons


def test_two_occurrences_require_stronger_evidence_before_modeling() -> None:
    txs = [
        _tx(1, date(2026, 2, 13), "25.99"),
        _tx(2, date(2026, 3, 13), "25.99"),
    ]
    lines = build_lines(txs)
    line = lines[0]
    assert line.inferred_cadence == "monthly"
    assert line.is_modeled is False
    assert line.modeling_status == "observational_only"
    assert line.bucket_assignment == "variable_discretionary"
    assert "weak_cadence_evidence" in line.review_reasons
    assert line.base_amount == Decimal("25.99")


def test_three_stable_baseline_occurrences_can_model_recurring() -> None:
    txs = [
        _tx(1, date(2026, 1, 13), "25.99"),
        _tx(2, date(2026, 2, 13), "25.99"),
        _tx(3, date(2026, 3, 13), "25.99"),
    ]
    lines = build_lines(txs)
    line = lines[0]
    assert line.inferred_cadence == "monthly"
    assert line.is_modeled is True
    assert line.modeling_status == "modeled_recurring"
    assert line.bucket_assignment == "recurring_baseline"
    assert line.normalized_monthly > Decimal("0.00")
    assert line.base_amount == Decimal("25.99")


def test_unknown_cadence_line_excluded_from_recurring_headline_totals() -> None:
    summary = build_snapshot_summary([build_lines([_tx(1, date(2026, 3, 13), "25.99")])[0]])
    assert summary["monthly_recurring_baseline_expenses"] == Decimal("0.00")
    assert summary["observed_one_off_exceptional_total"] == Decimal("25.99")
    assert summary["net_recurring_monthly"] == Decimal("0.00")


def test_high_merchant_low_cadence_does_not_show_high_confidence() -> None:
    txs = [
        _tx(1, date(2026, 2, 13), "25.99"),
        _tx(2, date(2026, 3, 13), "25.99"),
    ]
    line = build_lines(txs)[0]
    assert line.merchant_confidence >= 0.8
    assert line.is_modeled is False
    assert line.metadata["confidence_label"] != "High confidence"


def test_similar_housing_aliases_collapse_into_one_group() -> None:
    txs = [
        _tx(
            1,
            date(2026, 2, 1),
            "520.00",
            desc="UPM TRUST 123",
            merchant="UPM TRUST 123",
            category="General / Home",
            subcategory="Rent",
            group_key="expenses|upm-trust-123",
        ),
        _tx(
            2,
            date(2026, 3, 1),
            "520.00",
            desc="UPM TRUST 123",
            merchant="UPM TRUST 123",
            category="General / Home",
            subcategory="Rent",
            group_key="expenses|upm-trust-123",
        ),
        _tx(
            3,
            date(2026, 2, 2),
            "521.00",
            desc="UPM TRUST LTD",
            merchant="UPM TRUST LTD",
            category="General / Home",
            subcategory="Rent",
            group_key="expenses|upm-trust-ltd",
        ),
        _tx(
            4,
            date(2026, 3, 2),
            "521.00",
            desc="UPM TRUST LTD",
            merchant="UPM TRUST LTD",
            category="General / Home",
            subcategory="Rent",
            group_key="expenses|upm-trust-ltd",
        ),
    ]
    lines = build_lines(txs)
    assert len(lines) == 1
    assert lines[0].group_label == "UPM TRUST"
    assert lines[0].transaction_count == 4


def test_one_off_large_purchase_does_not_become_baseline() -> None:
    tx = _tx(
        1,
        date(2026, 3, 8),
        "1299.00",
        desc="JB HI FI",
        merchant="JB HI FI",
        category="Discretionary",
        subcategory="Presents",
        group_key="expenses|jbhifi",
        metadata={"bucket_lean": "discretionary", "baseline_eligible": False, "ancillary": False},
    )
    line = build_lines([tx])[0]
    assert line.bucket_assignment == "one_off_exceptional"
    assert line.is_modeled is False
    assert line.normalized_monthly == Decimal("0.00")


def test_code_prefixed_goodlife_rows_group_together_into_one_line() -> None:
    txs = [
        _tx(
            1,
            date(2026, 2, 1),
            "15.29",
            desc="A00LEQ2007CT GOODLIFE FOUNTAI SHAHEEL KUMAR",
            merchant="GOODLIFE",
            category="Health & Fitness",
            subcategory="Gym membership",
            group_key="Health & Fitness|Gym membership|A00LEQ2007CT GOODLIFE FOUNTAI SHAHEEL KUMAR",
        ),
        _tx(
            2,
            date(2026, 2, 8),
            "15.29",
            desc="A00LGSH307RB GOODLIFE FOUNTAI SHAHEEL KUMAR",
            merchant="GOODLIFE",
            category="Health & Fitness",
            subcategory="Gym membership",
            group_key="Health & Fitness|Gym membership|A00LGSH307RB GOODLIFE FOUNTAI SHAHEEL KUMAR",
        ),
        _tx(
            3,
            date(2026, 2, 15),
            "15.29",
            desc="A00LGSH307RC GOODLIFE FOUNTAI SHAHEEL KUMAR",
            merchant="GOODLIFE",
            category="Health & Fitness",
            subcategory="Gym membership",
            group_key="Health & Fitness|Gym membership|A00LGSH307RC GOODLIFE FOUNTAI SHAHEEL KUMAR",
        ),
    ]
    lines = build_lines(txs)
    assert len(lines) == 1
    assert lines[0].group_label == "GOODLIFE"
    assert lines[0].transaction_count == 3
    assert lines[0].bucket_assignment == "recurring_baseline"
