from datetime import date
from decimal import Decimal

from app.services.budget_v2.cadence import infer_cadence, normalize_from_cadence, normalize_from_source


def test_fortnightly_interval_inference_prefers_interval_evidence() -> None:
    cadence, confidence, summary = infer_cadence(
        [date(2026, 2, 13), date(2026, 2, 27), date(2026, 3, 13), date(2026, 3, 27)]
    )
    assert cadence == "fortnightly"
    assert confidence > 0.8
    assert summary["intervals"] == [14, 14, 14]


def test_monthly_normalization_from_single_cycle_amount() -> None:
    normalized = normalize_from_cadence(Decimal("25.99"), "monthly")
    assert normalized["monthly"] == Decimal("25.99")
    assert normalized["yearly"] == Decimal("311.88")


def test_period_source_normalization_is_consistent() -> None:
    normalized = normalize_from_source(Decimal("100.00"), "fortnightly")
    assert normalized["yearly"] == Decimal("2600.00")
    assert normalized["monthly"] == Decimal("216.67")
    assert normalized["weekly"] == Decimal("50.00")
