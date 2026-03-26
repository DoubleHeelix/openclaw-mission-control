from app.services.budget_v2.read_api import _resolved_review_reasons


def test_explicit_bucket_decision_clears_non_parser_review_reasons() -> None:
    reasons = _resolved_review_reasons(
        review_reasons=[
            "salary_like_single_occurrence",
            "low_confidence",
            "transfer_like_requires_review",
            "parser_anomaly",
        ],
        effective_bucket_assignment="income_recurring",
        effective_cadence="unknown",
        transaction_ops=set(),
        group_ops={"mark_recurring"},
    )

    assert reasons == ["parser_anomaly"]


def test_explicit_cadence_decision_clears_cadence_review_reasons() -> None:
    reasons = _resolved_review_reasons(
        review_reasons=[
            "cadence_ambiguous_material",
            "weak_cadence_evidence",
            "statement_shorter_than_cycle",
            "low_confidence",
        ],
        effective_bucket_assignment="variable_discretionary",
        effective_cadence="monthly",
        transaction_ops=set(),
        group_ops={"set_cadence"},
    )

    assert reasons == ["low_confidence"]


def test_bucket_specific_filters_clear_salary_like_reason_for_recurring_income() -> None:
    reasons = _resolved_review_reasons(
        review_reasons=["salary_like_single_occurrence", "credit_requires_review"],
        effective_bucket_assignment="income_recurring",
        effective_cadence="monthly",
        transaction_ops=set(),
        group_ops=set(),
    )

    assert reasons == []


def test_bucket_specific_filters_clear_single_occurrence_reason_for_one_off() -> None:
    reasons = _resolved_review_reasons(
        review_reasons=["single_occurrence_only", "likely_one_off", "low_confidence"],
        effective_bucket_assignment="one_off_exceptional",
        effective_cadence="irregular",
        transaction_ops=set(),
        group_ops=set(),
    )

    assert reasons == ["low_confidence"]
