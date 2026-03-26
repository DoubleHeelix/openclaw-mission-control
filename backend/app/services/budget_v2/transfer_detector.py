"""Transfer assessment stage for normalized transactions."""

from __future__ import annotations

from dataclasses import dataclass, field
import re

from app.services.budget_v2.config import PERSON_TRANSFER_HINTS, TRANSFER_KEYWORDS
from app.services.budget_v2.merchant_memory import MerchantMemoryHints
from app.services.budget_v2.types import NormalizedTransaction

PERSON_NAME_PATTERN = re.compile(r"\b(?:TO|FROM)\s+[A-Z]{2,}(?:\s+[A-Z]{2,}){0,4}\b")
SELF_TRANSFER_HINTS = (
    "SAVINGS",
    "OFFSET",
    "MORTGAGE OFFSET",
    "OWN ACCOUNT",
    "INTERNAL",
    "TRANSFER BETWEEN ACCOUNTS",
    "TO ACCT",
    "FROM ACCT",
)

DEBT_PAYMENT_HINTS = (
    "CREDIT CARD PAYMENT",
    "CC PAYMENT",
    "LOAN REPAYMENT",
    "MORTGAGE REPAYMENT",
)

REFUND_HINTS = (
    "REFUND",
    "REVERSAL",
    "CHARGEBACK",
)

TRANSFER_RAILS = {"NPP", "OSKO", "PAYID", "WISE"}
ACCOUNT_TRANSFER_PATTERN = re.compile(
    r"(?:\b\d{2}[- ]\d{4}[- ]\d{7}[- ]\d{2}\b|\b\d{4}[- ]\d{4}[- ]\d{4}[- ]\d{4}\b|\b(?:\d{4}\s+){3}\d{4}\b|\b(?:\d{2}\s+\d{4}\s+\d{7}\s+\d{2})\b|\b\d{15,16}\b)"
)
TRANSFER_DIRECTION_PATTERN = re.compile(r"\b(?:DEBIT|CREDIT)\s+TRANSFER\b")


@dataclass
class TransferAssessment:
    state: str
    confidence: float
    reasons: list[str] = field(default_factory=list)


def assess_transfer(*, tx: NormalizedTransaction, memory: MerchantMemoryHints | None = None) -> TransferAssessment:
    desc = (tx.normalized_description or "").upper()
    raw_desc = (tx.raw_description or "").upper()
    reasons: list[str] = []
    score = 0.0

    if (
        (ACCOUNT_TRANSFER_PATTERN.search(desc) or ACCOUNT_TRANSFER_PATTERN.search(raw_desc))
        and (TRANSFER_DIRECTION_PATTERN.search(desc) or TRANSFER_DIRECTION_PATTERN.search(raw_desc))
    ):
        reasons.append("account_transfer_pattern")
        return TransferAssessment("transfer_confirmed_internal", 0.95, reasons)

    # Strong exclusions first
    if any(token in desc for token in REFUND_HINTS):
        return TransferAssessment("not_transfer", 0.02, ["refund_semantics"])

    if tx.payment_rail in TRANSFER_RAILS:
        score += 0.30
        reasons.append(f"payment_rail:{tx.payment_rail}")

    if any(token in desc for token in TRANSFER_KEYWORDS):
        score += 0.28
        reasons.append("transfer_keywords")

    if PERSON_NAME_PATTERN.search(desc) or any(hint in desc for hint in PERSON_TRANSFER_HINTS):
        score += 0.15
        reasons.append("person_counterparty_pattern")

    if any(hint in desc for hint in SELF_TRANSFER_HINTS):
        score += 0.38
        reasons.append("self_transfer_hint")

    if any(hint in desc for hint in DEBT_PAYMENT_HINTS):
        score += 0.34
        reasons.append("debt_payment_hint")

    if memory is not None and memory.transfer_tendency > 0:
        score += min(memory.transfer_tendency, 0.25)
        reasons.extend(memory.reasons)

    # Strong internal transfer
    if score >= 0.72 and any(hint in desc for hint in SELF_TRANSFER_HINTS):
        return TransferAssessment("transfer_confirmed_internal", min(score, 0.98), reasons)

    # Debt payments behave like money movement, not expense
    if score >= 0.70 and any(hint in desc for hint in DEBT_PAYMENT_HINTS):
        return TransferAssessment("transfer_confirmed_debt_payment", min(score, 0.96), reasons)

    # External/person-to-person transfer
    if score >= 0.78 and tx.direction == "debit" and (
        tx.payment_rail in TRANSFER_RAILS or PERSON_NAME_PATTERN.search(desc)
    ):
        return TransferAssessment("transfer_confirmed_external", min(score, 0.95), reasons)

    # Directional credits that look transfer-like
    if score >= 0.62 and tx.direction == "credit" and (
        tx.payment_rail in TRANSFER_RAILS or "TRANSFER" in desc or "FROM ACCT" in desc
    ):
        return TransferAssessment("transfer_likely_inbound", min(score, 0.88), reasons)

    if score >= 0.45:
        return TransferAssessment("transfer_likely", min(score, 0.85), reasons)

    return TransferAssessment("not_transfer", max(score, 0.05), reasons)
