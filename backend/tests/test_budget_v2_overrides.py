from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.services.budget_v2.constants import BucketAssignment
from app.services.budget_v2.engine import _apply_group_override_history
from app.services.budget_v2.totals import compute_budget_model_totals, observational_monthly_estimate
from app.services.budget_v2.types import BudgetLine

STATEMENT_WINDOW_DAYS = 61


def _line(*, line_type: str = "expense", category: str = "Entertainment", bucket_assignment: str = "recurring_baseline") -> BudgetLine:
    return BudgetLine(
        group_key=f"{category}|Movies / Activities|STREAMING",
        group_label="Streaming" if line_type == "expense" else "Salary",
        line_type=line_type,
        category=category,
        subcategory="Movies / Activities" if line_type == "expense" else "Payroll",
        inferred_cadence="monthly",
        cadence_confidence=0.8,
        cadence_reason="intervals_match_monthly",
        observed_only=False,
        bucket_assignment=bucket_assignment,
        modeling_status="modeled_recurring",
        recurrence_state="recurring_candidate",
        is_modeled=True,
        modeled_by_default=True,
        base_amount=Decimal("25.99"),
        base_period="monthly",
        authoritative_field="base_amount",
        source_amount=Decimal("25.99"),
        source_period="monthly",
        observed_window_total=Decimal("51.98"),
        normalized_weekly=Decimal("6.00"),
        normalized_fortnightly=Decimal("12.00"),
        normalized_monthly=Decimal("25.99"),
        normalized_yearly=Decimal("311.88"),
        reserve_monthly_equivalent=Decimal("0"),
        impact_on_baseline="included",
        included=True,
        confidence=0.8,
        merchant_confidence=0.85,
        bucket_confidence=0.8,
        explanation="base",
        notes=None,
        transaction_count=2,
        observed_amount=Decimal("25.99"),
        observed_frequency_label="2 occurrences in statement window",
        row_indexes=[1, 2],
        review_reasons=[],
    )


def _override(operation: str, payload: dict[str, object], minute: int) -> SimpleNamespace:
    return SimpleNamespace(
        target_id="Entertainment|Movies / Activities|STREAMING",
        operation=operation,
        payload=payload,
        created_at=datetime(2026, 3, 18, 10, minute, tzinfo=timezone.utc),
    )


def test_group_override_history_updates_include_cadence_and_amount() -> None:
    line = _line()
    overrides = [
        SimpleNamespace(
            target_id=line.group_key,
            operation="set_include",
            payload={"included": False},
            created_at=datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc),
        ),
        SimpleNamespace(
            target_id=line.group_key,
            operation="set_base_amount_period",
            payload={"base_amount": 30, "base_period": "monthly"},
            created_at=datetime(2026, 3, 18, 10, 1, tzinfo=timezone.utc),
        ),
        SimpleNamespace(
            target_id=line.group_key,
            operation="set_notes",
            payload={"notes": "one-off reviewed"},
            created_at=datetime(2026, 3, 18, 10, 2, tzinfo=timezone.utc),
        ),
    ]
    result = _apply_group_override_history([line], overrides)
    out = result[0]
    assert out.included is False
    assert out.impact_on_baseline == "excluded"
    assert out.normalized_monthly == Decimal("30.00")
    assert out.notes == "one-off reviewed"


def test_group_override_history_saved_variable_amount_drives_variable_totals() -> None:
    line = _line(bucket_assignment=BucketAssignment.VARIABLE_DISCRETIONARY)
    overrides = [
        SimpleNamespace(
            target_id=line.group_key,
            operation="set_base_amount_period",
            payload={"base_amount": 100, "base_period": "weekly"},
            created_at=datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc),
        ),
    ]

    result = _apply_group_override_history([line], overrides)
    out = result[0]
    totals = compute_budget_model_totals(
        lines=result,
        modeling_allowed=True,
        modeling_restrictions=[],
        statement_window_days=STATEMENT_WINDOW_DAYS,
    )

    assert out.bucket_assignment == BucketAssignment.VARIABLE_DISCRETIONARY
    assert out.base_amount == Decimal("100.00")
    assert out.base_period == "weekly"
    assert out.normalized_monthly == Decimal("433.33")
    assert totals.variable_discretionary_monthly == Decimal("433.33")


def test_group_override_history_mark_one_off_moves_line_to_one_off_bucket_and_excludes_it_from_nets() -> None:
    line = _line()
    overrides = [
        SimpleNamespace(
            target_id=line.group_key,
            operation="mark_one_off",
            payload={},
            created_at=datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc),
        ),
    ]

    result = _apply_group_override_history([line], overrides)
    out = result[0]

    assert out.bucket_assignment == BucketAssignment.ONE_OFF_EXCEPTIONAL
    assert out.observed_only is True
    assert out.is_modeled is False
    assert out.normalized_monthly == Decimal("0.00")

    totals = compute_budget_model_totals(
        lines=result,
        modeling_allowed=True,
        modeling_restrictions=[],
        statement_window_days=STATEMENT_WINDOW_DAYS,
    )
    assert totals.observed_one_off_total == Decimal("51.98")
    assert totals.core_net == Decimal("0.00")
    assert totals.observed_net == Decimal("0.00")


@pytest.mark.parametrize(
    ("overrides", "expected_bucket", "expected_cadence", "expected_section_total_field", "expected_section_total", "expected_core_net", "expected_observed_net"),
    [
        ([_override("mark_recurring", {}, 0)], BucketAssignment.RECURRING_BASELINE, "monthly", "recurring_baseline_monthly", Decimal("25.99"), Decimal("-25.99"), Decimal("-25.99")),
        ([_override("set_bucket_assignment", {"bucket_assignment": "variable_discretionary"}, 0)], BucketAssignment.VARIABLE_DISCRETIONARY, "monthly", "variable_discretionary_monthly", Decimal("25.99"), Decimal("0.00"), Decimal("-25.99")),
        ([_override("mark_one_off", {}, 0)], BucketAssignment.ONE_OFF_EXCEPTIONAL, "monthly", "observed_one_off_total", Decimal("51.98"), Decimal("0.00"), Decimal("0.00")),
        ([_override("mark_recurring", {}, 0), _override("mark_one_off", {}, 1)], BucketAssignment.ONE_OFF_EXCEPTIONAL, "monthly", "observed_one_off_total", Decimal("51.98"), Decimal("0.00"), Decimal("0.00")),
        ([_override("mark_one_off", {}, 0), _override("mark_recurring", {}, 1)], BucketAssignment.RECURRING_BASELINE, "monthly", "recurring_baseline_monthly", Decimal("25.99"), Decimal("-25.99"), Decimal("-25.99")),
        ([_override("set_bucket_assignment", {"bucket_assignment": "variable_discretionary"}, 0), _override("mark_one_off", {}, 1)], BucketAssignment.ONE_OFF_EXCEPTIONAL, "monthly", "observed_one_off_total", Decimal("51.98"), Decimal("0.00"), Decimal("0.00")),
        ([_override("set_cadence", {"cadence": "quarterly"}, 0), _override("set_bucket_assignment", {"bucket_assignment": "recurring_baseline"}, 1)], BucketAssignment.RECURRING_BASELINE, "quarterly", "recurring_baseline_monthly", Decimal("8.66"), Decimal("-8.66"), Decimal("-8.66")),
        ([_override("mark_recurring", {}, 0), _override("set_cadence", {"cadence": "irregular"}, 1)], BucketAssignment.RECURRING_BASELINE, "irregular", "recurring_baseline_monthly", Decimal("0.00"), Decimal("0.00"), Decimal("0.00")),
        ([_override("set_cadence", {"cadence": "monthly"}, 0), _override("set_bucket_assignment", {"bucket_assignment": "recurring_baseline"}, 1)], BucketAssignment.RECURRING_BASELINE, "monthly", "recurring_baseline_monthly", Decimal("25.99"), Decimal("-25.99"), Decimal("-25.99")),
    ],
)
def test_group_override_history_scenario_matrix(
    overrides: list[SimpleNamespace],
    expected_bucket: str,
    expected_cadence: str,
    expected_section_total_field: str,
    expected_section_total: Decimal,
    expected_core_net: Decimal,
    expected_observed_net: Decimal,
) -> None:
    line = _line()
    for item in overrides:
        item.target_id = line.group_key

    result = _apply_group_override_history([line], overrides)
    out = result[0]
    totals = compute_budget_model_totals(
        lines=result,
        modeling_allowed=True,
        modeling_restrictions=[],
        statement_window_days=STATEMENT_WINDOW_DAYS,
    )

    assert out.bucket_assignment == expected_bucket
    assert out.inferred_cadence == expected_cadence
    assert totals.core_net == expected_core_net
    assert totals.observed_net == expected_observed_net
    assert getattr(totals, expected_section_total_field) == expected_section_total


def test_group_override_history_income_mark_recurring_maps_to_recurring_income() -> None:
    line = _line(line_type="income", category="Income", bucket_assignment="income_irregular")
    overrides = [
        SimpleNamespace(
            target_id=line.group_key,
            operation="mark_recurring",
            payload={},
            created_at=datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc),
        ),
    ]

    result = _apply_group_override_history([line], overrides)
    out = result[0]
    totals = compute_budget_model_totals(
        lines=result,
        modeling_allowed=True,
        modeling_restrictions=[],
        statement_window_days=STATEMENT_WINDOW_DAYS,
    )

    assert out.bucket_assignment == BucketAssignment.INCOME_RECURRING
    assert out.observed_only is False
    assert totals.recurring_income_monthly == Decimal("25.99")
    assert totals.core_net == Decimal("25.99")
    assert totals.observed_net == Decimal("25.99")
