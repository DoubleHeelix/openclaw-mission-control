"""Build line items and statement-level totals from classified transactions."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from difflib import SequenceMatcher

from app.services.budget_v2.cadence import infer_cadence, normalize_from_cadence, normalize_from_source
from app.services.budget_v2.config import BALANCED_REVIEW_CADENCE_THRESHOLD
from app.services.budget_v2.merchant_classifier import canonicalize_merchant_descriptor
from app.services.budget_v2.types import BudgetLine, ClassifiedTransaction

RECURRING_CADENCES = {"weekly", "fortnightly", "monthly", "quarterly", "yearly"}
ONE_OFF_THRESHOLD = Decimal("100.00")
MONTHLY_DAYS = Decimal("30.4375")
CENT = Decimal("0.01")


def _group_key(tx: ClassifiedTransaction) -> str:
    base = canonicalize_merchant_descriptor(tx.merchant_candidate or tx.normalized_description) or "UNKNOWN"
    final_bucket = str((tx.metadata or {}).get("final_bucket", ""))
    bucket_part = f"{final_bucket}|" if final_bucket else ""
    return f"{bucket_part}{tx.category}|{tx.subcategory}|{base[:120]}"


def _frequency_label(tx_count: int, cadence: str) -> str:
    if tx_count <= 1:
        return "1 occurrence in statement window"
    if cadence in RECURRING_CADENCES:
        return f"{tx_count} observations, {cadence} candidate"
    if cadence == "irregular":
        return f"{tx_count} observations, irregular spacing"
    return f"{tx_count} observations, cadence unknown"


def _derive_confidence_label(*, merchant_confidence: float, cadence_confidence: float, bucket_confidence: float, is_modeled: bool, review_reasons: list[str]) -> str:
    if review_reasons:
        return "Needs review"
    if is_modeled and min(merchant_confidence, cadence_confidence, bucket_confidence) >= 0.82:
        return "High confidence"
    if not is_modeled and min(merchant_confidence, bucket_confidence) >= 0.68:
        return "Medium confidence"
    if min(merchant_confidence, bucket_confidence) >= 0.68 and (not is_modeled or cadence_confidence >= 0.68):
        return "Medium confidence"
    return "Needs review"


def _median_decimal(values: list[Decimal]) -> Decimal:
    ordered = sorted(values)
    if not ordered:
        return Decimal("0.00")
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / Decimal("2")


def _quantize(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def _line_trust_and_modeling(
    *,
    bucket_assignment: str,
    line_type: str,
    review_reasons: list[str],
    cadence: str,
    cadence_confidence: float,
    confidence: float,
    current_tx_count: int,
    historical_occurrence_count: int,
    historical_amounts: list[Decimal],
) -> tuple[str, bool, str | None]:
    if bucket_assignment not in {"recurring_baseline", "income_recurring"}:
        return "observational", False, "Observed-only bucket."
    if review_reasons:
        return "needs_review", False, "Unresolved review reasons."
    if cadence not in RECURRING_CADENCES:
        return "needs_review", False, "Cadence is not stable enough."
    threshold = 2 if line_type == "income" else 3
    total_occurrences = current_tx_count + historical_occurrence_count
    if total_occurrences < threshold:
        return "provisional", False, "Not enough observations across statements."
    if confidence < 0.68 or cadence_confidence < 0.68:
        return "provisional", False, "Confidence is below modeling threshold."
    if historical_amounts:
        comparison = historical_amounts + [_median_decimal(historical_amounts)]
        median_amount = _median_decimal(comparison)
        if median_amount > Decimal("0.00"):
            latest_gap = abs(historical_amounts[-1] - median_amount) if historical_amounts else Decimal("0.00")
            if latest_gap > (median_amount * Decimal("0.35")):
                return "provisional", False, "Amount variance is too high for trusted modeling."
    return "verified", True, None


def _decide_bucket(
    *,
    first: ClassifiedTransaction,
    tx_count: int,
    cadence: str,
    cadence_confidence: float,
    observed_amount: Decimal,
    merchant_confidence: float,
) -> tuple[str, str, bool, bool, str, str, float, list[str], str]:
    metadata = first.metadata if isinstance(first.metadata, dict) else {}
    bucket_lean = str(metadata.get("bucket_lean", "discretionary"))
    baseline_eligible = bool(metadata.get("baseline_eligible", False))
    ancillary = bool(metadata.get("ancillary", False))
    likely_payroll = bool(metadata.get("likely_payroll_candidate", False))
    review_reasons: list[str] = []

    recurring_candidate = cadence in RECURRING_CADENCES and tx_count >= 2 and cadence_confidence >= 0.68
    strong_recurring = cadence in RECURRING_CADENCES and tx_count >= 3 and cadence_confidence >= 0.82
    moderate_modeled = cadence in RECURRING_CADENCES and tx_count >= 3 and cadence_confidence >= 0.75

    if first.category in {"Transfer / Money Movement", "Cash Withdrawal", "Refund / Reversal"}:
        return (
            "transfer_money_movement",
            "unknown",
            False,
            False,
            "excluded",
            "Transfers, withdrawals, and refunds are excluded from modeled budget totals.",
            0.9,
            review_reasons,
            "observational_only",
        )

    if first.category == "Income":
        if likely_payroll and not recurring_candidate:
            review_reasons.append("likely_payroll_candidate")
        if recurring_candidate and (likely_payroll or first.interpretation_type == "income_payroll"):
            return (
                "income_recurring",
                "recurring_candidate",
                True,
                True,
                "included",
                "Recurring payroll-like credits contribute to recurring income totals.",
                0.88 if strong_recurring else 0.76,
                review_reasons,
                "modeled_recurring",
            )
        return (
            "income_irregular",
            "unknown",
            False,
            False,
            "included",
            "Income remains irregular/observational until recurrence evidence is strong enough.",
            0.7 if likely_payroll else 0.62,
            review_reasons,
            "observational_only",
        )

    if ancillary:
        return (
            "variable_discretionary",
            "discretionary_candidate",
            False,
            False,
            "included",
            "Ancillary fee-like spend remains discretionary and observational by default.",
            0.84,
            review_reasons,
            "observational_only",
        )

    if tx_count == 1:
        review_reasons.append("single_occurrence_only")
        if observed_amount >= ONE_OFF_THRESHOLD or baseline_eligible:
            review_reasons.append("likely_one_off")
            return (
                "one_off_exceptional",
                "one_off_candidate",
                False,
                False,
                "included",
                "Single-occurrence spend is treated as one-off by default.",
                0.72,
                review_reasons,
                "observational_only",
            )
        return (
            "variable_discretionary",
            "discretionary_candidate",
            False,
            False,
            "included",
            "Single-occurrence discretionary spend remains observational by default.",
            0.68,
            review_reasons,
            "observational_only",
        )

    if cadence not in RECURRING_CADENCES:
        review_reasons.append("weak_cadence_evidence")
        return (
            "variable_discretionary",
            "discretionary_candidate",
            False,
            False,
            "included",
            "Weak or unknown cadence remains observational and excluded from recurring totals.",
            0.62,
            review_reasons,
            "observational_only",
        )

    if baseline_eligible and moderate_modeled and merchant_confidence >= 0.74:
        return (
            "recurring_baseline",
            "recurring_candidate",
            True,
            tx_count >= 2,
            "reserve_only" if cadence == "yearly" else "included",
            "Baseline-leaning merchant family with sufficient cadence evidence is modeled as recurring.",
            0.9 if strong_recurring else 0.78,
            review_reasons,
            "modeled_recurring",
        )

    if recurring_candidate:
        review_reasons.append("weak_cadence_evidence")
        return (
            "variable_discretionary",
            "discretionary_candidate",
            False,
            False,
            "included",
            "Repeated discretionary spend remains observational until the user promotes it or evidence strengthens.",
            0.68 if strong_recurring else 0.64,
            review_reasons,
            "observational_only",
        )

    review_reasons.append("weak_cadence_evidence")
    return (
        "variable_discretionary",
        "discretionary_candidate",
        False,
        False,
        "included",
        "Cadence evidence is too weak to model this line as recurring.",
        0.6,
        review_reasons,
        "observational_only",
    )


def _attach_duplicate_candidates(lines: list[BudgetLine]) -> list[BudgetLine]:
    for idx, line in enumerate(lines):
        label = line.group_label.upper()
        if line.line_type != "expense":
            continue
        candidates: list[dict[str, object]] = []
        for other in lines[idx + 1 :]:
            if other.line_type != "expense":
                continue
            if line.category != other.category:
                continue
            sim = SequenceMatcher(None, label, other.group_label.upper()).ratio()
            amount_gap = abs(line.observed_amount - other.observed_amount)
            similar_amount = amount_gap <= max(Decimal("50.00"), line.observed_amount * Decimal("0.12"))
            if sim >= 0.62 and similar_amount:
                candidates.append(
                    {
                        "group_key": other.group_key,
                        "group_label": other.group_label,
                        "similarity": round(sim, 2),
                    }
                )
                other.duplicate_group_candidates.append(
                    {
                        "group_key": line.group_key,
                        "group_label": line.group_label,
                        "similarity": round(sim, 2),
                    }
                )
        if candidates:
            line.duplicate_group_candidates.extend(candidates)

    for line in lines:
        if line.duplicate_group_candidates:
            line.merge_candidate_confidence = max(float(candidate.get("similarity", 0.0)) for candidate in line.duplicate_group_candidates)
            if "duplicate_group_candidate" not in line.review_reasons:
                line.review_reasons.append("duplicate_group_candidate")
            line.metadata["likely_merge_targets"] = line.duplicate_group_candidates
            line.metadata["merge_candidate_confidence"] = line.merge_candidate_confidence
    return lines


def build_lines(
    transactions: list[ClassifiedTransaction],
    historical_recurrence: dict[str, dict[str, object]] | None = None,
    statement_window_days: int | None = None,
) -> list[BudgetLine]:
    grouped: dict[str, list[ClassifiedTransaction]] = defaultdict(list)
    for tx in transactions:
        key = _group_key(tx)
        grouped[key].append(tx)

    lines: list[BudgetLine] = []
    for key, items in grouped.items():
        amounts = [item.amount for item in items]
        dates = [item.transaction_date for item in items if isinstance(item.transaction_date, date)]
        cadence, cadence_conf, cadence_meta = infer_cadence(dates)
        avg_amount = sum(amounts, Decimal("0")) / Decimal(str(max(len(amounts), 1)))
        observed = sum(amounts, Decimal("0"))
        first = items[0]
        line_type = "income" if first.category == "Income" else "expense"
        recurrence_key = str((first.metadata or {}).get("recurrence_key", key))
        historical = (historical_recurrence or {}).get(recurrence_key, {})
        historical_occurrence_count = int(historical.get("occurrence_count", 0) or 0)
        historical_amounts = [Decimal(str(item)) for item in list(historical.get("amounts", []) or [])]
        first_meta = first.metadata if isinstance(first.metadata, dict) else {}
        merchant_confidence = float(
            sum(Decimal(str((item.metadata or {}).get("merchant_confidence", item.confidence))) for item in items)
            / Decimal(str(len(items)))
        )
        if first_meta.get("final_bucket"):
            bucket_assignment = str(first_meta.get("bucket_assignment", "variable_discretionary"))
            review_flags = set(first_meta.get("review_flags", []) or [])
            reasons = set(first_meta.get("reasons", []) or [])
            if (
                str(first_meta.get("final_bucket")) == "income"
                and bucket_assignment == "income_irregular"
                and (
                    "salary_like_single_occurrence" in review_flags
                    or "likely_payroll_candidate" in review_flags
                    or "payroll_semantics" in reasons
                )
            ):
                bucket_assignment = "income_recurring"
            recurrence_state = (
                "recurring_candidate"
                if str(first_meta.get("final_bucket")) in {"income", "recurring_baseline_expenses"} and cadence in RECURRING_CADENCES
                else "unknown"
            )
            is_modeled = bucket_assignment in {"income_recurring", "recurring_baseline"} and cadence in RECURRING_CADENCES
            modeled_by_default = is_modeled
            impact = "excluded" if str(first_meta.get("final_bucket")) == "transfers" else "included"
            baseline_decision_reason = "; ".join(list(first_meta.get("reasons", []) or []))
            bucket_confidence = float(first_meta.get("bucket_confidence", first.confidence))
            derived_review_reasons = list(first_meta.get("review_flags", []) or [])
            modeling_status = "modeled_recurring" if is_modeled else "observational_only"
        else:
            (
                bucket_assignment,
                recurrence_state,
                is_modeled,
                modeled_by_default,
                impact,
                baseline_decision_reason,
                bucket_confidence,
                derived_review_reasons,
                modeling_status,
            ) = _decide_bucket(
                first=first,
                tx_count=len(items),
                cadence=cadence,
                cadence_confidence=cadence_conf,
                observed_amount=observed,
                merchant_confidence=merchant_confidence,
            )

        review_reasons = sorted(
            {
                reason
                for item in items
                for reason in [*(item.review_reasons or []), *list(((item.metadata or {}).get("review_flags", []) or []))]
            }
        )
        review_reasons.extend(derived_review_reasons)
        if cadence_conf < BALANCED_REVIEW_CADENCE_THRESHOLD and observed > Decimal("200") and line_type == "expense":
            review_reasons.append("cadence_ambiguous_material")
        review_reasons = sorted(set(review_reasons))

        smoothed_amount = avg_amount
        if historical_amounts and cadence in RECURRING_CADENCES:
            smoothed_amount = _median_decimal(historical_amounts + [avg_amount])

        line_trust_level, modeling_eligible, modeling_block_reason = _line_trust_and_modeling(
            bucket_assignment=bucket_assignment,
            line_type=line_type,
            review_reasons=review_reasons,
            cadence=cadence,
            cadence_confidence=cadence_conf,
            confidence=merchant_confidence,
            current_tx_count=len(items),
            historical_occurrence_count=historical_occurrence_count,
            historical_amounts=historical_amounts,
        )

        authoritative_base_amount = _quantize(smoothed_amount if is_modeled and cadence in RECURRING_CADENCES else avg_amount)
        authoritative_base_period = cadence if is_modeled and cadence in RECURRING_CADENCES else "monthly"

        normalized = normalize_from_cadence(smoothed_amount, cadence) if is_modeled and cadence in RECURRING_CADENCES else {
            "weekly": Decimal("0.00"),
            "fortnightly": Decimal("0.00"),
            "monthly": Decimal("0.00"),
            "yearly": Decimal("0.00"),
        }
        confidence = min(max(merchant_confidence, 0.0), 0.99)
        confidence_label = _derive_confidence_label(
            merchant_confidence=merchant_confidence,
            cadence_confidence=cadence_conf,
            bucket_confidence=bucket_confidence,
            is_modeled=is_modeled,
            review_reasons=review_reasons,
        )

        line = BudgetLine(
            group_key=key,
            group_label=canonicalize_merchant_descriptor(first.merchant_candidate or first.normalized_description)
            or first.merchant_candidate
            or first.normalized_description,
            line_type=line_type,
            category=first.category,
            subcategory=first.subcategory,
            inferred_cadence=cadence,
            cadence_confidence=cadence_conf,
            cadence_reason=str(cadence_meta.get("reason", "unknown")),
            observed_only=not is_modeled,
            bucket_assignment=bucket_assignment,
            modeling_status=modeling_status,
            recurrence_state=recurrence_state,
            is_modeled=is_modeled,
            modeled_by_default=modeled_by_default,
            base_amount=authoritative_base_amount,
            base_period=authoritative_base_period,
            authoritative_field="base_amount",
            source_amount=authoritative_base_amount,
            source_period=authoritative_base_period,
            observed_window_total=observed,
            normalized_weekly=normalized["weekly"],
            normalized_fortnightly=normalized["fortnightly"],
            normalized_monthly=normalized["monthly"],
            normalized_yearly=normalized["yearly"],
            reserve_monthly_equivalent=Decimal("0.00") if impact != "reserve_only" else normalized["monthly"],
            impact_on_baseline=impact,
            included=all(item.included for item in items),
            confidence=confidence,
            merchant_confidence=merchant_confidence,
            bucket_confidence=bucket_confidence,
            movement_type=first.movement_type,
            explanation=f"Grouped {len(items)} transactions. Cadence intervals: {cadence_meta.get('intervals', [])}",
            notes=None,
            transaction_count=len(items),
            observed_amount=observed,
            observed_frequency_label=_frequency_label(len(items), cadence),
            line_trust_level=line_trust_level,
            modeling_eligible=modeling_eligible,
            modeling_block_reason=modeling_block_reason,
            classification_version=str((first.metadata or {}).get("classification_version", "")) or None,
            mapping_source=str((first.metadata or {}).get("mapping_source", first.evidence_source)) or None,
            line_integrity_status="verified" if len({item.row_index for item in items}) == len(items) else "needs_repair",
            row_indexes=[item.row_index for item in items],
            review_reasons=review_reasons,
            metadata={
                "bucket_suggestion": "suggested_baseline" if bucket_assignment in {"recurring_baseline", "income_recurring"} else "suggested_discretionary",
                "baseline_decision_reason": baseline_decision_reason,
                "confidence_label": confidence_label,
                "merchant_confidence": merchant_confidence,
                "bucket_confidence": bucket_confidence,
                "modeling_status": modeling_status,
                "recurrence_state": recurrence_state,
                "is_modeled": is_modeled,
                "modeled_by_default": modeled_by_default,
                "observed_amount": observed,
                "observed_frequency_label": _frequency_label(len(items), cadence),
                "movement_type": first.movement_type,
                "final_bucket": first_meta.get("final_bucket"),
                "reasons": list(first_meta.get("reasons", []) or []),
                "review_flags": list(first_meta.get("review_flags", []) or []),
                "line_trust_level": line_trust_level,
                "modeling_eligible": modeling_eligible,
                "modeling_block_reason": modeling_block_reason,
                "classification_version": str((first.metadata or {}).get("classification_version", "")) or None,
                "mapping_source": str((first.metadata or {}).get("mapping_source", first.evidence_source)) or None,
                "historical_occurrence_count": historical_occurrence_count,
                "smoothed_amount": _quantize(smoothed_amount),
                "recurrence_key": recurrence_key,
                "line_integrity_status": "verified" if len({item.row_index for item in items}) == len(items) else "needs_repair",
            },
        )
        lines.append(line)

    lines = _attach_duplicate_candidates(lines)
    for line in lines:
        line.metadata["duplicate_group_candidates"] = line.duplicate_group_candidates
        line.metadata["merge_candidate_confidence"] = line.merge_candidate_confidence
        line.metadata["confidence_label"] = _derive_confidence_label(
            merchant_confidence=line.merchant_confidence,
            cadence_confidence=line.cadence_confidence,
            bucket_confidence=line.bucket_confidence,
            is_modeled=line.is_modeled,
            review_reasons=line.review_reasons,
        )
    return lines


def apply_line_source(line: BudgetLine, *, amount: Decimal, period: str, authoritative_field: str = "base_amount") -> None:
    normalized = normalize_from_source(amount, period)
    line.base_amount = _quantize(amount)
    line.base_period = period
    line.source_amount = line.base_amount
    line.source_period = period
    line.authoritative_field = authoritative_field
    line.normalized_weekly = normalized["weekly"]
    line.normalized_fortnightly = normalized["fortnightly"]
    line.normalized_monthly = normalized["monthly"]
    line.normalized_yearly = normalized["yearly"]
    line.is_modeled = True
    line.modeled_by_default = False
    line.modeling_status = "user_forced_recurring"
    line.observed_only = False
    if line.impact_on_baseline == "reserve_only":
        line.reserve_monthly_equivalent = normalized["monthly"]
    line.metadata["is_modeled"] = True
    line.metadata["modeled_by_default"] = False
    line.metadata["modeling_status"] = "user_forced_recurring"


def build_snapshot_summary(lines: list[BudgetLine]) -> dict[str, Decimal]:
    summary = {
        "observed_spend": Decimal("0.00"),
        "monthly_recurring_income": Decimal("0.00"),
        "monthly_irregular_income": Decimal("0.00"),
        "monthly_recurring_baseline_expenses": Decimal("0.00"),
        "monthly_variable_discretionary": Decimal("0.00"),
        "monthly_one_off_exceptional": Decimal("0.00"),
        "monthly_transfer_excluded": Decimal("0.00"),
        "observed_variable_discretionary_total": Decimal("0.00"),
        "observed_one_off_exceptional_total": Decimal("0.00"),
        "observed_irregular_income_total": Decimal("0.00"),
        "observed_transfer_total": Decimal("0.00"),
        "net_recurring_monthly": Decimal("0.00"),
        "net_observed_total": Decimal("0.00"),
        "core_monthly_baseline": Decimal("0.00"),
        "observed_discretionary_monthly": Decimal("0.00"),
        "annual_infrequent_oneoff_spend": Decimal("0.00"),
        "transfer_money_movement_spend": Decimal("0.00"),
        "reserve_adjusted_monthly_cost": Decimal("0.00"),
        "total_income_monthly": Decimal("0.00"),
        "total_expenses_monthly": Decimal("0.00"),
        "net_monthly": Decimal("0.00"),
    }

    reserve_only = Decimal("0.00")
    for line in lines:
        summary["observed_spend"] += line.observed_window_total
        if not line.included:
            continue
        if line.impact_on_baseline == "reserve_only":
            reserve_only += line.reserve_monthly_equivalent
        if line.bucket_assignment == "income_recurring":
            summary["monthly_recurring_income"] += line.normalized_monthly
        elif line.bucket_assignment == "income_irregular":
            summary["observed_irregular_income_total"] += line.observed_amount
        elif line.bucket_assignment == "recurring_baseline":
            summary["monthly_recurring_baseline_expenses"] += line.normalized_monthly
        elif line.bucket_assignment == "variable_discretionary":
            summary["monthly_variable_discretionary"] += line.normalized_monthly
            summary["observed_variable_discretionary_total"] += line.observed_amount
        elif line.bucket_assignment == "one_off_exceptional":
            summary["observed_one_off_exceptional_total"] += line.observed_amount
        elif line.bucket_assignment == "transfer_money_movement":
            summary["observed_transfer_total"] += line.observed_amount

    summary["monthly_irregular_income"] = Decimal("0.00")
    summary["monthly_one_off_exceptional"] = Decimal("0.00")
    summary["monthly_transfer_excluded"] = Decimal("0.00")
    summary["net_recurring_monthly"] = summary["monthly_recurring_income"] - summary["monthly_recurring_baseline_expenses"]
    summary["net_observed_total"] = (
        summary["monthly_recurring_income"]
        - summary["monthly_recurring_baseline_expenses"]
        - summary["observed_variable_discretionary_total"]
    )
    summary["core_monthly_baseline"] = summary["monthly_recurring_baseline_expenses"]
    summary["observed_discretionary_monthly"] = summary["monthly_variable_discretionary"]
    summary["annual_infrequent_oneoff_spend"] = summary["observed_one_off_exceptional_total"]
    summary["transfer_money_movement_spend"] = summary["observed_transfer_total"]
    summary["reserve_adjusted_monthly_cost"] = summary["monthly_recurring_baseline_expenses"] + reserve_only
    summary["total_income_monthly"] = summary["monthly_recurring_income"]
    summary["total_expenses_monthly"] = summary["monthly_recurring_baseline_expenses"] + summary["monthly_variable_discretionary"]
    summary["net_monthly"] = summary["total_income_monthly"] - summary["total_expenses_monthly"]

    return {key: value.quantize(Decimal("0.01")) for key, value in summary.items()}
