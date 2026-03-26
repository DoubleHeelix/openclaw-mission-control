from datetime import date
from decimal import Decimal
from types import SimpleNamespace

from app.services.budget_v2.constants import BucketAssignment
from app.services.budget_v2.totals import (
    compute_budget_model_totals,
    compute_statement_window_days,
    observational_monthly_estimate,
)


def _line(*, bucket_assignment: str, normalized_monthly: str = "0.00", observed_window_total: str = "0.00", included: bool = True):
    return SimpleNamespace(
        bucket_assignment=bucket_assignment,
        normalized_monthly=Decimal(normalized_monthly),
        base_amount=Decimal(normalized_monthly),
        base_period="monthly",
        observed_window_total=Decimal(observed_window_total),
        included=included,
    )


def test_statement_window_days_is_inclusive() -> None:
    assert compute_statement_window_days(statement_start_date=date(2026, 1, 1), statement_end_date=date(2026, 1, 31)) == 31


def test_observational_monthly_estimate_requires_at_least_seven_days() -> None:
    assert observational_monthly_estimate(observed_total=Decimal("120.00"), statement_window_days=6) is None


def test_budget_model_uses_observational_monthly_estimate_for_variable_discretionary() -> None:
    lines = [
        _line(bucket_assignment=BucketAssignment.INCOME_RECURRING, normalized_monthly="4000.00"),
        _line(bucket_assignment=BucketAssignment.RECURRING_BASELINE, normalized_monthly="1500.00"),
        _line(bucket_assignment=BucketAssignment.VARIABLE_DISCRETIONARY, normalized_monthly="0.00", observed_window_total="310.00"),
        _line(bucket_assignment=BucketAssignment.ONE_OFF_EXCEPTIONAL, observed_window_total="220.00"),
        _line(bucket_assignment=BucketAssignment.TRANSFER_MONEY_MOVEMENT, observed_window_total="500.00"),
        _line(bucket_assignment=BucketAssignment.INCOME_IRREGULAR, observed_window_total="850.00"),
    ]

    totals = compute_budget_model_totals(
        lines=lines,
        modeling_allowed=True,
        modeling_restrictions=[],
        statement_window_days=31,
    )

    assert totals.recurring_income_monthly == Decimal("4000.00")
    assert totals.recurring_baseline_monthly == Decimal("1500.00")
    assert totals.variable_discretionary_monthly == Decimal("304.40")
    assert totals.observed_one_off_total == Decimal("220.00")
    assert totals.observed_transfer_total == Decimal("500.00")
    assert totals.irregular_income_total == Decimal("850.00")
    assert totals.core_net == Decimal("2500.00")
    assert totals.observed_net == Decimal("2195.60")


def test_budget_model_uses_saved_variable_budget_amount_when_present() -> None:
    lines = [
        _line(bucket_assignment=BucketAssignment.INCOME_RECURRING, normalized_monthly="4000.00"),
        _line(bucket_assignment=BucketAssignment.RECURRING_BASELINE, normalized_monthly="1500.00"),
        _line(bucket_assignment=BucketAssignment.VARIABLE_DISCRETIONARY, normalized_monthly="433.00", observed_window_total="310.00"),
    ]

    totals = compute_budget_model_totals(
        lines=lines,
        modeling_allowed=True,
        modeling_restrictions=[],
        statement_window_days=31,
    )

    assert totals.variable_discretionary_monthly == Decimal("433.00")
    assert totals.observed_net == Decimal("2067.00")


def test_budget_model_keeps_provisional_monthly_totals_when_modeling_blocked() -> None:
    lines = [
        _line(bucket_assignment=BucketAssignment.INCOME_RECURRING, normalized_monthly="4000.00"),
        _line(bucket_assignment=BucketAssignment.RECURRING_BASELINE, normalized_monthly="1500.00"),
        _line(bucket_assignment=BucketAssignment.VARIABLE_DISCRETIONARY, observed_window_total="310.00"),
        _line(bucket_assignment=BucketAssignment.ONE_OFF_EXCEPTIONAL, observed_window_total="220.00"),
    ]

    totals = compute_budget_model_totals(
        lines=lines,
        modeling_allowed=False,
        modeling_restrictions=["Statement range is too short for trusted recurrence modeling."],
        statement_window_days=31,
    )

    assert totals.recurring_income_monthly == Decimal("4000.00")
    assert totals.recurring_baseline_monthly == Decimal("1500.00")
    assert totals.variable_discretionary_monthly == Decimal("304.40")
    assert totals.observed_one_off_total == Decimal("220.00")
    assert totals.core_net == Decimal("2500.00")
    assert totals.observed_net == Decimal("2195.60")


def test_budget_model_uses_budget_amount_for_provisional_recurring_income_when_monthly_normalization_is_zero() -> None:
    lines = [
        SimpleNamespace(
            bucket_assignment=BucketAssignment.INCOME_RECURRING,
            normalized_monthly=Decimal("0.00"),
            base_amount=Decimal("5314.81"),
            base_period="monthly",
            observed_window_total=Decimal("5314.81"),
            included=True,
            metadata={"final_bucket": "income"},
        ),
        SimpleNamespace(
            bucket_assignment=BucketAssignment.RECURRING_BASELINE,
            normalized_monthly=Decimal("3277.33"),
            base_amount=Decimal("3277.33"),
            base_period="monthly",
            observed_window_total=Decimal("3277.33"),
            included=True,
            metadata={"final_bucket": "recurring_baseline_expenses"},
        ),
    ]

    totals = compute_budget_model_totals(
        lines=lines,
        modeling_allowed=False,
        modeling_restrictions=["Source document is non-reconcilable."],
        statement_window_days=30,
    )

    assert totals.recurring_income_monthly == Decimal("5314.81")
    assert totals.recurring_baseline_monthly == Decimal("3277.33")
    assert totals.core_net == Decimal("2037.48")
