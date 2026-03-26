"""Descriptor and amount normalization for budget pipeline."""

from __future__ import annotations

import hashlib
import re
from decimal import Decimal

from app.services.budget_v2.config import CLASSIFICATION_VERSION, PAYMENT_RAIL_PREFIXES
from app.services.budget_v2.constants import MovementType
from app.services.budget_v2.identity import merchant_base_name, merchant_fingerprint
from app.services.budget_v2.merchant_classifier import canonicalize_merchant_descriptor
from app.services.budget_v2.parsers.common import detect_leakage_tokens
from app.services.budget_v2.types import NormalizedTransaction, ParsedTransaction

WHITESPACE = re.compile(r"\s+")
PUNCT = re.compile(r"[^A-Z0-9*/ ]+")
LONG_NUMBER = re.compile(r"\b\d{4,}\b")
SHORT_DATE = re.compile(r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b")
CARD_SUFFIX = re.compile(r"\b(?:XX+\d{2,4}|\*+\d{2,4})\b")
STORE_CODE = re.compile(r"\b[A-Z]?\d{3,}[A-Z]?\b")
ACCOUNT_NUMBER = re.compile(r"\b(?:\d{2}-\d{4}-\d{7}-\d{2}|\d{15,16})\b")
CARD_LIKE_NUMBER = re.compile(r"\b(?:\d{4}-\d{4}-\d{4}-\d{4}|\d{15,16})\b")
TRAILING_REF_NUMBER = re.compile(r"\b\d{5,}\b$")
TRANSFER_PHRASE = re.compile(r"\b(?:DEBIT|CREDIT)\s+TRANSFER\b")
PAYMENT_PHRASE = re.compile(r"\b(?:DIRECT\s+CREDIT|DIRECT\s+DEBIT|BILL\s+PAYMENT)\b")
ORIG_DATE_PHRASE = re.compile(r"\bORIG\s+DATE\b.*$")
TRAILING_PAY_TOKEN = re.compile(r"\bPAY\b(?=\s*$)")
COMPACT_RAIL_PREFIXES = {"DD", "DC", "BP", "VT", "EP", "AP", "CQ"}

NOISE_TOKENS = {
    "POS",
    "VISA",
    "DEBIT",
    "CREDIT",
    "PURCHASE",
    "CARD",
    "EFTPOS",
    "AUTH",
    "PENDING",
    "REFERENCE",
    "REF",
    "TXN",
    "TRANSACTION",
    "AU",
    "NZ",
    "PTY",
    "LTD",
    "LIMITED",
}

REFUND_KEYWORDS = ("REFUND", "REVERSAL", "CHARGEBACK")
TRANSFER_KEYWORDS = (
    "TRANSFER",
    "PAYMENT TO",
    "PAYMENT FROM",
    "INTERNAL",
    "TO ACCT",
    "FROM ACCT",
    "OWN ACCOUNT",
    "OSKO",
    "PAYID",
)
DEBT_PAYMENT_KEYWORDS = (
    "CREDIT CARD PAYMENT",
    "CC PAYMENT",
    "LOAN REPAYMENT",
    "MORTGAGE REPAYMENT",
)
FEE_KEYWORDS = (
    "FEE",
    "ACCOUNT FEE",
    "MONTHLY FEE",
    "SERVICE FEE",
    "OVERDRAFT FEE",
)
CASH_KEYWORDS = (
    "ATM",
    "CASH WITHDRAWAL",
)
INCOME_KEYWORDS = (
    "SALARY",
    "PAYROLL",
    "WAGES",
    "PAY",
    "EMPLOYER",
)


def _normalize_desc(raw: str) -> str:
    up = (raw or "").upper()
    up = up.replace("\\", " ").replace("/", " ").replace("\n", " ").replace("\r", " ")
    up = PUNCT.sub(" ", up)
    up = CARD_SUFFIX.sub(" ", up)
    up = SHORT_DATE.sub(" ", up)
    up = WHITESPACE.sub(" ", up).strip()
    return up


def _extract_payment_rail(desc: str) -> tuple[str | None, str]:
    for prefix in PAYMENT_RAIL_PREFIXES:
        if desc == prefix or desc.startswith(f"{prefix} "):
            stripped = desc[len(prefix) :].strip()
            return prefix, stripped or desc
        if prefix in COMPACT_RAIL_PREFIXES and desc.startswith(prefix) and len(desc) > len(prefix):
            next_char = desc[len(prefix)]
            if next_char.isalpha() or next_char.isdigit():
                stripped = desc[len(prefix) :].strip()
                return prefix, stripped or desc
    return None, desc


def _clean_merchant_source(desc: str, payment_rail: str | None) -> str:
    cleaned = desc
    cleaned = ACCOUNT_NUMBER.sub(" ", cleaned)
    cleaned = CARD_LIKE_NUMBER.sub(" ", cleaned)
    cleaned = ORIG_DATE_PHRASE.sub(" ", cleaned)
    cleaned = TRANSFER_PHRASE.sub(" ", cleaned)
    cleaned = PAYMENT_PHRASE.sub(" ", cleaned)
    cleaned = WHITESPACE.sub(" ", cleaned).strip()
    cleaned = TRAILING_PAY_TOKEN.sub(" ", cleaned)
    cleaned = TRAILING_REF_NUMBER.sub(" ", cleaned)

    if payment_rail in {"DD", "DC"} and "TRANSFER" in desc:
        return "INTERNAL TRANSFER"

    cleaned = WHITESPACE.sub(" ", cleaned).strip()
    return cleaned or desc


def _strip_noise_for_merchant(desc: str) -> str:
    cleaned = desc
    cleaned = LONG_NUMBER.sub(" ", cleaned)
    cleaned = STORE_CODE.sub(" ", cleaned)
    cleaned = WHITESPACE.sub(" ", cleaned).strip()

    tokens = []
    for token in cleaned.split():
        if token in NOISE_TOKENS:
            continue
        tokens.append(token)

    return " ".join(tokens).strip()


def _merchant_candidate(desc: str) -> str | None:
    cleaned = _strip_noise_for_merchant(desc)
    canonical = canonicalize_merchant_descriptor(cleaned)
    return canonical or cleaned or None


def _signed_amount(amount: Decimal, direction: str) -> Decimal:
    absolute = abs(Decimal(amount))
    return absolute if direction == "credit" else absolute * Decimal("-1")


def _default_movement_type(direction: str) -> str:
    return MovementType.INCOME if direction == "credit" else MovementType.EXPENSE


def _infer_movement_type(*, normalized_desc: str, direction: str) -> str:
    if any(token in normalized_desc for token in REFUND_KEYWORDS):
        return "refund"
    if any(token in normalized_desc for token in DEBT_PAYMENT_KEYWORDS):
        return "debt_payment"
    if any(token in normalized_desc for token in TRANSFER_KEYWORDS):
        return "internal_transfer"
    if any(token in normalized_desc for token in FEE_KEYWORDS):
        return "fee"
    if any(token in normalized_desc for token in CASH_KEYWORDS):
        return "cash_withdrawal"
    if direction == "credit" and any(token in normalized_desc for token in INCOME_KEYWORDS):
        return "income"
    return _default_movement_type(direction)


def normalize_transactions(parsed: list[ParsedTransaction]) -> list[NormalizedTransaction]:
    output: list[NormalizedTransaction] = []

    for tx in parsed:
        amount = abs(Decimal(tx.amount))
        normalized_desc = _normalize_desc(tx.raw_description)
        payment_rail, remainder = _extract_payment_rail(normalized_desc)
        merchant_source = _clean_merchant_source(remainder, payment_rail)
        merchant = _merchant_candidate(merchant_source)
        merchant_name = merchant_base_name(merchant or remainder)
        fingerprint = merchant_fingerprint(merchant_name)

        signed_amount = _signed_amount(amount, tx.direction)
        movement_type = _infer_movement_type(
            normalized_desc=normalized_desc,
            direction=tx.direction,
        )

        flags = list(tx.parser_flags or [])
        if detect_leakage_tokens(normalized_desc) and "suspected_footer_merge" not in flags:
            flags.append("suspected_footer_merge")

        row_key = (
            f"{tx.transaction_date}|{amount:.2f}|{tx.direction}|{normalized_desc}|"
            f"{tx.balance_after if tx.balance_after is not None else ''}"
        )
        row_hash = hashlib.sha256(row_key.encode("utf-8")).hexdigest()

        output.append(
            NormalizedTransaction(
                row_index=tx.row_index,
                transaction_date=tx.transaction_date,
                effective_date=tx.effective_date,
                amount=amount,
                direction=tx.direction,
                raw_description=tx.raw_description,
                normalized_description=normalized_desc,
                payment_rail=payment_rail,
                merchant_candidate=merchant,
                reference=tx.raw_reference,
                balance_after=tx.balance_after,
                parser_confidence=tx.parser_confidence,
                row_hash=row_hash,
                signed_amount=signed_amount,
                movement_type=movement_type,
                merchant_base_name=merchant_name,
                merchant_fingerprint=fingerprint,
                metadata={
                    **(tx.metadata or {}),
                    "parser_flags": flags,
                    "parser_warning_flags": flags,
                    "signed_amount": str(signed_amount),
                    "movement_type": movement_type,
                    "merchant_base_name": merchant_name,
                    "merchant_fingerprint": fingerprint,
                    "amount_source": "parsed_amount_absolute",
                    "date_source": "transaction_date" if tx.transaction_date else "missing",
                    "balance_source": "balance_after" if tx.balance_after is not None else "missing",
                    "description_continuation_detected": bool("\\" in (tx.raw_description or "") or "\n" in (tx.raw_description or "")),
                    "parser_row_quality": "high" if tx.parser_confidence >= 0.8 else "medium" if tx.parser_confidence >= 0.55 else "low",
                    "classification_version": CLASSIFICATION_VERSION,
                    "merchant_cleaned_source": merchant_source,
                },
            )
        )

    return output
