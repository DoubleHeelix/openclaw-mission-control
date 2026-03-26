"""Merchant memory lookup and hint shaping for the deterministic budget classifier."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.services.budget_v2.types import NormalizedTransaction


@dataclass
class MerchantMemoryHints:
    category_hint: str | None = None
    subcategory_hint: str | None = None
    baseline_tendency: float = 0.0
    transfer_tendency: float = 0.0
    income_tendency: float = 0.0
    confidence_adjustment: float = 0.0
    mapping_source: str = "none"
    reasons: list[str] = field(default_factory=list)


def build_memory_hint(*, tx: NormalizedTransaction, memory_row: Any | None) -> MerchantMemoryHints:
    if memory_row is None:
        return MerchantMemoryHints()

    source = getattr(memory_row, "source", None) or "merchant_memory"
    category = getattr(memory_row, "category", None)
    subcategory = getattr(memory_row, "subcategory", None)
    confidence = float(getattr(memory_row, "confidence", 0.0) or 0.0)
    descriptor = (tx.normalized_description or "").upper()
    category_text = f"{category} {subcategory}".upper()

    baseline_tendency = 0.0
    transfer_tendency = 0.0
    income_tendency = 0.0

    if category == "Income":
        income_tendency = max(0.5, confidence)
    if category in {
        "General / Home",
        "Insurance",
        "Health & Fitness",
        "Financing Costs",
    } or any(token in category_text for token in ("RENT", "POWER", "PHONE", "INTERNET", "INSURANCE", "GYM")):
        baseline_tendency = max(0.45, confidence)
    if "TRANSFER" in category_text or "MONEY MOVEMENT" in category_text:
        transfer_tendency = max(0.6, confidence)
    if any(token in descriptor for token in ("WISE", "OSKO", "NPP", "PAYID")):
        transfer_tendency = max(transfer_tendency, 0.35)

    mapping_source = "user_confirmed" if source == "manual_override" else "merchant_memory"
    return MerchantMemoryHints(
        category_hint=category,
        subcategory_hint=subcategory,
        baseline_tendency=baseline_tendency,
        transfer_tendency=transfer_tendency,
        income_tendency=income_tendency,
        confidence_adjustment=min(confidence * 0.15, 0.12),
        mapping_source=mapping_source,
        reasons=[f"memory:{mapping_source}"],
    )
