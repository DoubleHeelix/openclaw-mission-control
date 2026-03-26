"""Single-source statement truth, budget model totals, and review metrics."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

from app.models.budget import BudgetImportSession, BudgetParsedTransaction
from app.services.budget_v2.cadence import normalize_from_source
from app.services.budget_v2.constants import BucketAssignment

MONTHLY_DAYS = Decimal("30.44")
CENT = Decimal("0.01")


def _meta(item: object) -> dict[str, object]:
    metadata = getattr(item, "metadata", None)
    if isinstance(metadata, dict):
        return metadata
    extra_data = getattr(item, "extra_data", None)
    if isinstance(extra_data, dict):
        return extra_data
    return {}


def _final_bucket(item: object) -> str:
    return str(_meta(item).get("final_bucket", ""))


def _provisional_monthly(item: object) -> Decimal:
    normalized_monthly = Decimal(str(getattr(item, "normalized_monthly", Decimal("0.00"))))
    if normalized_monthly > Decimal("0.00"):
        return normalized_monthly
    base_amount = Decimal(str(getattr(item, "base_amount", Decimal("0.00"))))
    if base_amount <= Decimal("0.00"):
        return Decimal("0.00")
    base_period = str(getattr(item, "base_period", "monthly") or "monthly")
    return normalize_from_source(base_amount, base_period)["monthly"]


def _variable_monthly_value(item: object, *, statement_window_days: int | None) -> Decimal:
    modeled_monthly = _provisional_monthly(item)
    if modeled_monthly > Decimal("0.00"):
        return modeled_monthly
    return (
        observational_monthly_estimate(
            observed_total=getattr(item, "observed_window_total"),
            statement_window_days=statement_window_days,
        )
        or Decimal("0.00")
    )


@dataclass
class StatementTruthTotals:
    total_credits: Decimal = Decimal("0.00")
    total_debits: Decimal = Decimal("0.00")
    net_movement: Decimal = Decimal("0.00")
    opening_balance: Decimal | None = None
    closing_balance: Decimal | None = None
    expected_closing_balance: Decimal | None = None
    reconciliation_difference: Decimal = Decimal("0.00")
    reconciliation_status: str = "unknown"
    truth_trust_level: str = "needs_review"


@dataclass
class BudgetModelTotals:
    recurring_income_monthly: Decimal = Decimal("0.00")
    recurring_baseline_monthly: Decimal = Decimal("0.00")
    variable_discretionary_monthly: Decimal = Decimal("0.00")
    observed_one_off_total: Decimal = Decimal("0.00")
    observed_transfer_total: Decimal = Decimal("0.00")
    irregular_income_total: Decimal = Decimal("0.00")
    core_net: Decimal = Decimal("0.00")
    observed_net: Decimal = Decimal("0.00")
    modeling_allowed: bool = False
    modeling_restrictions: list[str] = field(default_factory=list)
    classification_reconciliation_status: str | None = None
    classification_reconciliation_difference: Decimal | None = None
    classified_section_totals: dict[str, Decimal] = field(default_factory=dict)
    section_confidence: dict[str, float] = field(default_factory=dict)
    statement_model_confidence: float | None = None


@dataclass
class ReviewMetrics:
    needs_review_count: int = 0
    low_confidence_group_count: int = 0
    uncategorized_review_count: int = 0


def compute_statement_window_days(*, statement_start_date: date | None, statement_end_date: date | None) -> int | None:
    if not isinstance(statement_start_date, date) or not isinstance(statement_end_date, date):
        return None
    return max((statement_end_date - statement_start_date).days + 1, 0)


def observational_monthly_estimate(*, observed_total: Decimal, statement_window_days: int | None) -> Decimal | None:
    if statement_window_days is None or statement_window_days < 7:
        return None
    if observed_total <= Decimal("0.00"):
        return Decimal("0.00")
    return ((observed_total / Decimal(statement_window_days)) * MONTHLY_DAYS).quantize(CENT, rounding=ROUND_HALF_UP)


def compute_statement_totals(
    *,
    import_row: BudgetImportSession,
    parsed_rows: list[BudgetParsedTransaction],
    expected_closing_balance: Decimal | None,
    truth_trust_level: str,
) -> StatementTruthTotals:
    total_debits = sum((row.amount for row in parsed_rows if row.direction == "debit"), Decimal("0.00"))
    total_credits = sum((row.amount for row in parsed_rows if row.direction == "credit"), Decimal("0.00"))
    return StatementTruthTotals(
        total_credits=total_credits,
        total_debits=total_debits,
        net_movement=total_credits - total_debits,
        opening_balance=import_row.opening_balance,
        closing_balance=import_row.closing_balance,
        expected_closing_balance=expected_closing_balance,
        reconciliation_difference=import_row.reconciliation_difference,
        reconciliation_status=import_row.reconciliation_status,
        truth_trust_level=truth_trust_level,
    )


def compute_budget_model_totals(
    *,
    lines: list[object],
    modeling_allowed: bool,
    modeling_restrictions: list[str],
    statement_window_days: int | None = None,
) -> BudgetModelTotals:
    recurring_income = sum(
        (
            _provisional_monthly(item)
            for item in lines
            if bool(getattr(item, "included")) and getattr(item, "bucket_assignment") == BucketAssignment.INCOME_RECURRING
            and _final_bucket(item) in {"", "income"}
        ),
        Decimal("0.00"),
    )
    recurring_baseline = sum(
        (
            getattr(item, "normalized_monthly")
            for item in lines
            if bool(getattr(item, "included")) and getattr(item, "bucket_assignment") == BucketAssignment.RECURRING_BASELINE
            and _final_bucket(item) in {"", "recurring_baseline_expenses"}
        ),
        Decimal("0.00"),
    )
    variable_discretionary = sum(
        (
            _variable_monthly_value(
                item,
                statement_window_days=statement_window_days,
            )
            for item in lines
            if bool(getattr(item, "included"))
            and getattr(item, "bucket_assignment") == BucketAssignment.VARIABLE_DISCRETIONARY
            and _final_bucket(item) in {"", "variable_spending"}
        ),
        Decimal("0.00"),
    )
    observed_one_off = sum(
        (
            getattr(item, "observed_window_total")
            for item in lines
            if bool(getattr(item, "included"))
            and getattr(item, "bucket_assignment") == BucketAssignment.ONE_OFF_EXCEPTIONAL
            and _final_bucket(item) in {"", "one_off_spending"}
        ),
        Decimal("0.00"),
    )
    observed_transfer = sum(
        (
            getattr(item, "observed_window_total")
            for item in lines
            if bool(getattr(item, "included"))
            and getattr(item, "bucket_assignment") == BucketAssignment.TRANSFER_MONEY_MOVEMENT
            and _final_bucket(item) in {"", "transfers"}
        ),
        Decimal("0.00"),
    )
    irregular_income_total = sum(
        (
            getattr(item, "observed_window_total")
            for item in lines
            if bool(getattr(item, "included"))
            and getattr(item, "bucket_assignment") == BucketAssignment.INCOME_IRREGULAR
            and _final_bucket(item) in {"", "income"}
        ),
        Decimal("0.00"),
    )
    if modeling_allowed:
        recurring_income = sum(
            (
                _provisional_monthly(item)
                for item in lines
                if bool(getattr(item, "included"))
                and getattr(item, "bucket_assignment") == BucketAssignment.INCOME_RECURRING
                and _final_bucket(item) in {"", "income"}
            ),
            Decimal("0.00"),
        )
        recurring_baseline = sum(
            (
                getattr(item, "normalized_monthly")
                for item in lines
                if bool(getattr(item, "included"))
                and getattr(item, "bucket_assignment") == BucketAssignment.RECURRING_BASELINE
                and _final_bucket(item) in {"", "recurring_baseline_expenses"}
                and bool(_meta(item).get("modeling_eligible", True))
            ),
            Decimal("0.00"),
        )
        variable_discretionary = sum(
            (
                _variable_monthly_value(
                    item,
                    statement_window_days=statement_window_days,
                )
                for item in lines
                if bool(getattr(item, "included"))
                and getattr(item, "bucket_assignment") == BucketAssignment.VARIABLE_DISCRETIONARY
                and _final_bucket(item) in {"", "variable_spending"}
            ),
            Decimal("0.00"),
        )

    return BudgetModelTotals(
        recurring_income_monthly=recurring_income,
        recurring_baseline_monthly=recurring_baseline,
        variable_discretionary_monthly=variable_discretionary,
        observed_one_off_total=observed_one_off,
        observed_transfer_total=observed_transfer,
        irregular_income_total=irregular_income_total,
        core_net=recurring_income - recurring_baseline,
        observed_net=recurring_income - recurring_baseline - variable_discretionary,
        modeling_allowed=modeling_allowed,
        modeling_restrictions=list(modeling_restrictions),
    )


def compute_review_metrics(lines: list[object]) -> ReviewMetrics:
    needs_review_count = sum(1 for line in lines if getattr(line, "review_reasons", []))
    low_confidence_group_count = sum(1 for line in lines if float(getattr(line, "confidence", 0.0)) < 0.6)
    uncategorized_review_count = sum(
        1
        for line in lines
        if "unknown_merchant" in list(getattr(line, "review_reasons", []) or [])
        or str(_meta(line).get("final_bucket", "")) == "uncategorized"
    )
    return ReviewMetrics(
        needs_review_count=needs_review_count,
        low_confidence_group_count=low_confidence_group_count,
        uncategorized_review_count=uncategorized_review_count,
    )
