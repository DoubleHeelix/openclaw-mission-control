"""Deterministic merchant identity and review helpers."""

from __future__ import annotations

import hashlib
import re
from decimal import Decimal

from app.services.budget_v2.merchant_classifier import canonicalize_merchant_descriptor

NOISE_PREFIXES = (
    "POS",
    "VISA",
    "DEBIT",
    "PURCHASE",
    "CARD PURCHASE",
    "EFTPOS",
    "PAYMENT",
    "MOBILE BANKING",
    "INTERNET BANKING",
)
TRAILING_REFERENCE = re.compile(r"\b\d{4,}\b")
CARD_SUFFIX = re.compile(r"\b(?:CARD|ACCT|ACCOUNT|A/C)\s*\d{2,4}\b")
DATE_TOKEN = re.compile(r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b")
MULTISPACE = re.compile(r"\s+")


def merchant_base_name(value: str | None) -> str | None:
    if not value:
        return None
    text = value.upper().strip()
    for prefix in NOISE_PREFIXES:
        if text.startswith(prefix):
            text = text[len(prefix) :].strip()
    text = DATE_TOKEN.sub(" ", text)
    text = CARD_SUFFIX.sub(" ", text)
    text = TRAILING_REFERENCE.sub(" ", text)
    text = MULTISPACE.sub(" ", text).strip()
    return canonicalize_merchant_descriptor(text) or text or None


def merchant_fingerprint(value: str | None) -> str | None:
    normalized = merchant_base_name(value)
    if not normalized:
        return None
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def canonical_group_key(category: str, subcategory: str, descriptor: str | None) -> str:
    normalized = merchant_base_name(descriptor) or "UNKNOWN"
    return f"{category}|{subcategory}|{normalized[:120]}"


def confidence_label(confidence: float, review_reasons: list[str]) -> str:
    if review_reasons or confidence < 0.6:
        return "Needs review"
    if confidence >= 0.82:
        return "High confidence"
    return "Medium confidence"


def review_priority(amount: Decimal, review_reasons: list[str], cadence_confidence: float | None = None) -> float:
    priority = abs(float(amount))
    if "duplicate_group_candidate" in review_reasons:
        priority += 500
    if "likely_payroll_candidate" in review_reasons:
        priority += 400
    if "cadence_ambiguous_material" in review_reasons or "weak_cadence_evidence" in review_reasons:
        priority += 250
    if cadence_confidence is not None and cadence_confidence < 0.55:
        priority += 100
    return priority
