"""Config-driven merchant-family classifier for merchant-expense interpretation only."""

from __future__ import annotations

from dataclasses import dataclass
import re

from app.services.budget_v2.config import (
    MERCHANT_FAMILY_RULES,
    SAFE_BROAD_MERCHANT_BUCKETS,
    UNKNOWN_EXPENSE_CATEGORY,
    UNKNOWN_EXPENSE_SUBCATEGORY,
)


@dataclass
class MerchantCategoryMatch:
    category: str
    subcategory: str
    confidence: float
    explanation: str
    bucket_lean: str = "discretionary"
    baseline_eligible: bool = False
    ancillary: bool = False


def _tokenize(text: str) -> set[str]:
    return {token for token in text.upper().split(" ") if token}


def _compact(text: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", text.upper())


def _stem_token(token: str) -> str:
    token = token.upper()
    if len(token) > 5 and token.endswith("IES"):
        return token[:-3] + "Y"
    if len(token) > 4 and token.endswith("ES"):
        return token[:-2]
    if len(token) > 3 and token.endswith("S"):
        return token[:-1]
    return token


def _token_variants(token: str) -> set[str]:
    upper = token.upper()
    stemmed = _stem_token(upper)
    return {upper, stemmed}


def _matches_alias(text: str, alias: str) -> bool:
    upper_text = text.upper()
    upper_alias = alias.upper()
    if upper_alias in upper_text:
        return True
    compact_text = _compact(upper_text)
    compact_alias = _compact(upper_alias)
    if compact_alias and compact_alias in compact_text:
        return True
    alias_tokens = [tok for tok in upper_alias.split() if tok]
    text_tokens = [tok for tok in upper_text.split() if tok]
    if len(alias_tokens) <= 1 or len(text_tokens) < len(alias_tokens):
        return False
    for index in range(len(text_tokens) - len(alias_tokens) + 1):
        window = text_tokens[index : index + len(alias_tokens)]
        if all(
            _stem_token(actual) == _stem_token(expected)
            or _stem_token(actual).startswith(_stem_token(expected))
            or _stem_token(expected).startswith(_stem_token(actual))
            for actual, expected in zip(window, alias_tokens)
        ):
            return True
    return False


REFERENCE_CODE_TOKEN = re.compile(r"^[A-Z]{0,4}\d[A-Z0-9]{2,}$")
TRAILING_LOCATION_TOKEN = re.compile(r"^[A-Z]{2,}$")
ADDRESS_START_TOKEN = re.compile(r"^(?:\d+[A-Z]?|LOT\d+|UNIT\d+|SHOP\d+)$")
ADDRESS_HINT_TOKENS = {"ST", "STREET", "RD", "ROAD", "AVE", "AVENUE", "DR", "DRIVE", "LANE", "LN", "CT", "COURT", "HWY", "HIGHWAY", "BLVD", "BOULEVARD"}


def canonicalize_merchant_descriptor(descriptor: str | None) -> str | None:
    text = " ".join((descriptor or "").upper().split())
    if not text:
        return None

    for rule in MERCHANT_FAMILY_RULES:
        aliases = sorted((str(a).upper() for a in (rule.get("aliases") or ())), key=len, reverse=True)
        matched_alias = next((alias for alias in aliases if _matches_alias(text, alias)), None)
        if matched_alias is not None:
            return matched_alias

    tokens = [token for token in text.split(" ") if token]
    while tokens and (tokens[0].isdigit() or REFERENCE_CODE_TOKEN.match(tokens[0])):
        tokens.pop(0)

    if not tokens:
        return None

    cleaned: list[str] = []
    for index, token in enumerate(tokens):
        if len(cleaned) >= 6:
            break
        next_token = tokens[index + 1] if index + 1 < len(tokens) else None
        if cleaned and (
            ADDRESS_START_TOKEN.match(token)
            or token in ADDRESS_HINT_TOKENS
            or (token.isdigit() and (next_token in ADDRESS_HINT_TOKENS or len(cleaned) >= 2))
        ):
            break
        cleaned.append(token)

    while len(cleaned) > 2 and TRAILING_LOCATION_TOKEN.match(cleaned[-1]) and len(cleaned[-1]) <= 3:
        cleaned.pop()

    normalized = " ".join(cleaned).strip()
    return normalized or None


def classify_merchant_descriptor(descriptor: str) -> MerchantCategoryMatch | None:
    text = descriptor.upper().strip()
    if not text:
        return None

    tokens = _tokenize(text)

    best: MerchantCategoryMatch | None = None
    for rule in MERCHANT_FAMILY_RULES:
        aliases = tuple(str(a).upper() for a in (rule.get("aliases") or ()))
        if not aliases:
            continue
        matched_alias = next((alias for alias in aliases if _matches_alias(text, alias)), None)
        if matched_alias is None:
            continue
        confidence = float(rule.get("confidence", 0.8))
        candidate = MerchantCategoryMatch(
            category=str(rule.get("category", UNKNOWN_EXPENSE_CATEGORY)),
            subcategory=str(rule.get("subcategory", UNKNOWN_EXPENSE_SUBCATEGORY)),
            confidence=confidence,
            explanation=f"Merchant family alias matched: {matched_alias}.",
            bucket_lean=str(rule.get("bucket_lean", "discretionary")),
            baseline_eligible=bool(rule.get("baseline_eligible", False)),
            ancillary=bool(rule.get("ancillary", False)),
        )
        if best is None or candidate.confidence > best.confidence:
            best = candidate

    if best is not None:
        return best

    for fallback in SAFE_BROAD_MERCHANT_BUCKETS:
        category = str(fallback.get("category", UNKNOWN_EXPENSE_CATEGORY))
        subcategory = str(fallback.get("subcategory", UNKNOWN_EXPENSE_SUBCATEGORY))
        fb_tokens = {tok.upper() for tok in (fallback.get("tokens") or ())}
        overlap = {
            fallback_token
            for fallback_token in fb_tokens
            if any(
                fallback_token in _token_variants(actual)
                or any(variant.startswith(fallback_token) or fallback_token.startswith(variant) for variant in _token_variants(actual))
                for actual in tokens
            )
        }
        if overlap:
            token = sorted(overlap)[0]
            return MerchantCategoryMatch(
                category=category,
                subcategory=subcategory,
                confidence=0.66,
                explanation=f"Broad merchant fallback token matched: {token}.",
                bucket_lean="discretionary",
                baseline_eligible=False,
                ancillary=False,
            )

    return None
