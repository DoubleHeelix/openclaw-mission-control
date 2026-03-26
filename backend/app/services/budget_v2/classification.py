"""Category classification with explicit interpretation input and review reason tagging."""

from __future__ import annotations

from dataclasses import dataclass
import re

from app.services.budget_v2.config import (
    AUTO_MEMORY_THRESHOLD,
    BALANCED_REVIEW_CONFIDENCE_THRESHOLD,
    CLASSIFICATION_VERSION,
    LOW_CONFIDENCE_THRESHOLD,
    UNKNOWN_EXPENSE_CATEGORY,
    UNKNOWN_EXPENSE_SUBCATEGORY,
)
from app.services.budget_v2.constants import BucketAssignment, MovementType
from app.services.budget_v2.merchant_classifier import classify_merchant_descriptor
from app.services.budget_v2.types import InterpretedTransaction, NormalizedTransaction

EMPLOYERISH_PATTERN = re.compile(r"\b(?:PTY|LTD|LIMITED|GROUP|HOLDINGS|SERVICES|AUSTRA|AUSTRALIA|PAYMENTS?)\b")


@dataclass
class ClassificationResult:
    classification_type: str
    category: str
    subcategory: str
    confidence: float
    explanation: str
    evidence_source: str
    impact_on_baseline: str
    bucket_assignment: str
    observed_only: bool
    review_reasons: list[str]
    metadata: dict[str, object] | None = None


def classify_transaction(
    tx: NormalizedTransaction,
    interpreted: InterpretedTransaction,
) -> ClassificationResult:
    t = interpreted.interpretation_type
    interp_conf = interpreted.interpretation_confidence

    if t == "parser_anomaly":
        return ClassificationResult(
            classification_type="uncategorized_review",
            category="Expenses",
            subcategory="Parser Anomaly",
            confidence=0.4,
            explanation=interpreted.interpretation_reason,
            evidence_source="deterministic_rule",
            impact_on_baseline="excluded",
            bucket_assignment=BucketAssignment.VARIABLE_DISCRETIONARY,
            observed_only=True,
            review_reasons=["parser_anomaly", "suspected_leakage"],
            metadata={
                "bucket_lean": "discretionary",
                "baseline_eligible": False,
                "ancillary": True,
                "movement_type": MovementType.OTHER_NEEDS_REVIEW,
                "classification_version": CLASSIFICATION_VERSION,
                "mapping_source": "rule",
            },
        )

    if t == "income_payroll":
        return ClassificationResult(
            classification_type="fixed_recurring_subscription",
            category="Income",
            subcategory="Salary / Wages",
            confidence=interp_conf,
            explanation=interpreted.interpretation_reason,
            evidence_source="deterministic_rule",
            impact_on_baseline="included",
            bucket_assignment=BucketAssignment.INCOME_RECURRING,
            observed_only=False,
            review_reasons=[],
            metadata={
                "bucket_lean": "income",
                "baseline_eligible": False,
                "ancillary": False,
                "merchant_confidence": interp_conf,
                "bucket_confidence": 0.92,
                "likely_payroll_candidate": True,
                "movement_type": MovementType.INCOME,
                "classification_version": CLASSIFICATION_VERSION,
                "mapping_source": "rule",
            },
        )

    if t == "income_other":
        payroll_candidate = bool(EMPLOYERISH_PATTERN.search(tx.normalized_description or ""))
        review = [] if interp_conf >= BALANCED_REVIEW_CONFIDENCE_THRESHOLD else ["low_confidence"]
        if payroll_candidate:
            review.append("likely_payroll_candidate")
        return ClassificationResult(
            classification_type="grouped_merchant_spend",
            category="Income",
            subcategory="Salary / Wages candidate" if payroll_candidate else "Other Income",
            confidence=interp_conf,
            explanation=interpreted.interpretation_reason,
            evidence_source="deterministic_rule",
            impact_on_baseline="included",
            bucket_assignment=BucketAssignment.INCOME_IRREGULAR,
            observed_only=False,
            review_reasons=review,
            metadata={
                "bucket_lean": "income",
                "baseline_eligible": False,
                "ancillary": False,
                "merchant_confidence": interp_conf,
                "bucket_confidence": 0.72 if payroll_candidate else 0.64,
                "likely_payroll_candidate": payroll_candidate,
                "movement_type": MovementType.INCOME,
                "classification_version": CLASSIFICATION_VERSION,
                "mapping_source": "rule",
            },
        )

    if t == "refund_reversal":
        return ClassificationResult(
            classification_type="refund_or_reversal",
            category="Refund / Reversal",
            subcategory="Refund / Reversal",
            confidence=interp_conf,
            explanation=interpreted.interpretation_reason,
            evidence_source="deterministic_rule",
            impact_on_baseline="excluded",
            bucket_assignment=BucketAssignment.VARIABLE_DISCRETIONARY,
            observed_only=True,
            review_reasons=[],
            metadata={
                "bucket_lean": "discretionary",
                "baseline_eligible": False,
                "ancillary": True,
                "merchant_confidence": interp_conf,
                "bucket_confidence": 0.9,
                "movement_type": MovementType.REFUND,
                "classification_version": CLASSIFICATION_VERSION,
                "mapping_source": "rule",
            },
        )

    if t in {"internal_transfer", "p2p_transfer_reimbursement"}:
        return ClassificationResult(
            classification_type="transfer_or_money_movement",
            category="Transfer / Money Movement",
            subcategory="Transfer / Money Movement",
            confidence=interp_conf,
            explanation=interpreted.interpretation_reason,
            evidence_source="deterministic_rule",
            impact_on_baseline="excluded",
            bucket_assignment=BucketAssignment.TRANSFER_MONEY_MOVEMENT,
            observed_only=True,
            review_reasons=[],
            metadata={
                "bucket_lean": "transfer",
                "baseline_eligible": False,
                "ancillary": False,
                "merchant_confidence": interp_conf,
                "bucket_confidence": 0.9,
                "movement_type": MovementType.INTERNAL_TRANSFER,
                "classification_version": CLASSIFICATION_VERSION,
                "mapping_source": "rule",
            },
        )

    if t == "cash_withdrawal":
        return ClassificationResult(
            classification_type="transfer_or_money_movement",
            category="Cash Withdrawal",
            subcategory="Cash Withdrawal",
            confidence=interp_conf,
            explanation=interpreted.interpretation_reason,
            evidence_source="deterministic_rule",
            impact_on_baseline="excluded",
            bucket_assignment=BucketAssignment.TRANSFER_MONEY_MOVEMENT,
            observed_only=True,
            review_reasons=[],
            metadata={
                "bucket_lean": "transfer",
                "baseline_eligible": False,
                "ancillary": False,
                "merchant_confidence": interp_conf,
                "bucket_confidence": 0.9,
                "movement_type": MovementType.CASH_WITHDRAWAL,
                "classification_version": CLASSIFICATION_VERSION,
                "mapping_source": "rule",
            },
        )

    if t == "debt_payment":
        return ClassificationResult(
            classification_type="debt_payment",
            category="Debt / Credit",
            subcategory="Debt Payment",
            confidence=interp_conf,
            explanation=interpreted.interpretation_reason,
            evidence_source="deterministic_rule",
            impact_on_baseline="excluded",
            bucket_assignment=BucketAssignment.TRANSFER_MONEY_MOVEMENT,
            observed_only=True,
            review_reasons=[],
            metadata={
                "bucket_lean": "transfer",
                "baseline_eligible": False,
                "ancillary": False,
                "merchant_confidence": interp_conf,
                "bucket_confidence": 0.88,
                "movement_type": MovementType.DEBT_PAYMENT,
                "classification_version": CLASSIFICATION_VERSION,
                "mapping_source": "rule",
            },
        )

    if t == "merchant_fee":
        return ClassificationResult(
            classification_type="bank_fee",
            category=UNKNOWN_EXPENSE_CATEGORY,
            subcategory=UNKNOWN_EXPENSE_SUBCATEGORY,
            confidence=interp_conf,
            explanation=interpreted.interpretation_reason,
            evidence_source="deterministic_rule",
            impact_on_baseline="included",
            bucket_assignment=BucketAssignment.VARIABLE_DISCRETIONARY,
            observed_only=True,
            review_reasons=[],
            metadata={
                "bucket_lean": "discretionary",
                "baseline_eligible": False,
                "ancillary": True,
                "merchant_confidence": interp_conf,
                "bucket_confidence": 0.82,
                "movement_type": MovementType.FEE,
                "classification_version": CLASSIFICATION_VERSION,
                "mapping_source": "rule",
            },
        )

    # Merchant classification only applies to merchant_expense interpretation.
    descriptor = tx.merchant_candidate or tx.normalized_description
    merchant_match = classify_merchant_descriptor(descriptor or "")
    if merchant_match is not None:
        confidence = max(interp_conf, merchant_match.confidence)
        review_reasons: list[str] = []
        if confidence < LOW_CONFIDENCE_THRESHOLD:
            review_reasons.append("low_confidence")
        if confidence < 0.7:
            review_reasons.append("unknown_merchant")
        return ClassificationResult(
            classification_type="grouped_merchant_spend",
            category=merchant_match.category,
            subcategory=merchant_match.subcategory,
            confidence=confidence,
            explanation=merchant_match.explanation,
            evidence_source="semantic_fallback",
            impact_on_baseline="included",
            bucket_assignment=BucketAssignment.VARIABLE_DISCRETIONARY,
            observed_only=True,
            review_reasons=review_reasons,
            metadata={
                "bucket_lean": merchant_match.bucket_lean,
                "baseline_eligible": merchant_match.baseline_eligible,
                "ancillary": merchant_match.ancillary,
                "merchant_confidence": confidence,
                "bucket_confidence": 0.74 if merchant_match.bucket_lean == "baseline" else 0.68,
                "movement_type": MovementType.EXPENSE,
                "classification_version": CLASSIFICATION_VERSION,
                "mapping_source": "semantic_fallback",
            },
        )

    return ClassificationResult(
        classification_type="uncategorized_review",
        category=UNKNOWN_EXPENSE_CATEGORY,
        subcategory=UNKNOWN_EXPENSE_SUBCATEGORY,
        confidence=min(interp_conf, 0.48),
        explanation="Merchant spend unresolved after deterministic and family-level matching.",
        evidence_source="semantic_fallback",
        impact_on_baseline="excluded",
        bucket_assignment=BucketAssignment.VARIABLE_DISCRETIONARY,
        observed_only=True,
        review_reasons=["unknown_merchant", "low_confidence"],
        metadata={
            "bucket_lean": "discretionary",
            "baseline_eligible": False,
            "ancillary": False,
            "merchant_confidence": min(interp_conf, 0.48),
            "bucket_confidence": 0.45,
            "movement_type": MovementType.OTHER_NEEDS_REVIEW,
            "classification_version": CLASSIFICATION_VERSION,
            "mapping_source": "semantic_fallback",
        },
    )


def should_promote_memory(confidence: float, evidence_source: str) -> bool:
    return evidence_source in {"deterministic_rule", "manual_override"} and confidence >= AUTO_MEMORY_THRESHOLD
