"""Interval-first cadence inference and normalization conversion helpers."""

from __future__ import annotations

from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from statistics import mean

from app.services.budget_v2.config import ANNUAL_FACTORS, CADENCE_WINDOWS


def _annual_factor(period: str) -> Decimal:
    return Decimal(str(ANNUAL_FACTORS.get(period, 12)))


def _q(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def infer_cadence(dates: list[date]) -> tuple[str, float, dict[str, object]]:
    if len(dates) < 2:
        return "unknown", 0.3, {"intervals": [], "reason": "insufficient_observations"}

    sorted_dates = sorted(dates)
    intervals = [(sorted_dates[idx] - sorted_dates[idx - 1]).days for idx in range(1, len(sorted_dates))]
    avg_interval = mean(intervals)
    variance_proxy = mean(abs(day - avg_interval) for day in intervals)

    for cadence, window in CADENCE_WINDOWS.items():
        if window.min_days <= avg_interval <= window.max_days:
            stability = max(0.0, 1.0 - (variance_proxy / max(avg_interval, 1.0)))
            confidence = min(0.98, 0.45 + (0.12 * len(intervals)) + (0.4 * stability))
            return cadence, confidence, {
                "intervals": intervals,
                "avg_interval": round(avg_interval, 2),
                "variance_proxy": round(variance_proxy, 2),
                "reason": f"intervals_match_{cadence}",
            }

    return "irregular", 0.52, {
        "intervals": intervals,
        "avg_interval": round(avg_interval, 2),
        "variance_proxy": round(variance_proxy, 2),
        "reason": "intervals_inconsistent",
    }


def normalize_from_cadence(base_amount: Decimal, cadence: str) -> dict[str, Decimal]:
    annual = base_amount * _annual_factor(cadence)
    monthly = annual / Decimal("12")
    weekly = annual / Decimal("52")
    fortnightly = annual / Decimal("26")
    return {
        "weekly": _q(weekly),
        "fortnightly": _q(fortnightly),
        "monthly": _q(monthly),
        "yearly": _q(annual),
    }


def normalize_from_source(source_amount: Decimal, source_period: str) -> dict[str, Decimal]:
    annual = source_amount * _annual_factor(source_period)
    return {
        "weekly": _q(annual / Decimal("52")),
        "fortnightly": _q(annual / Decimal("26")),
        "monthly": _q(annual / Decimal("12")),
        "yearly": _q(annual),
    }


def derive_source_amount(period_values: dict[str, Decimal], authoritative_field: str) -> tuple[Decimal, str]:
    period = authoritative_field if authoritative_field in {"weekly", "fortnightly", "monthly", "yearly"} else "monthly"
    value = period_values.get(period, Decimal("0"))
    if period == "yearly":
        return _q(value), "yearly"
    factor = _annual_factor(period)
    if factor == 0:
        return _q(value), period
    return _q((value * Decimal("12")) / factor) if period == "monthly" else _q(value), period
