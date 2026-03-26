"""Internal typed objects for budget v2 pipeline stages."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal


@dataclass
class ParsedTransaction:
    row_index: int
    transaction_date: date | None
    effective_date: date | None
    amount: Decimal
    direction: str
    raw_description: str
    raw_reference: str | None = None
    balance_after: Decimal | None = None
    page_number: int | None = None
    source_line_refs: list[int] = field(default_factory=list)
    parser_flags: list[str] = field(default_factory=list)
    parser_confidence: float = 0.0
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass
class ParsedStatementResult:
    statement_id: str
    bank_name: str | None
    account_name: str | None
    account_ref_masked: str | None
    statement_start_date: date | None
    statement_end_date: date | None
    parser_name: str
    parser_confidence: float
    parser_flags: list[str]
    metadata: dict[str, object]
    transactions: list[ParsedTransaction]


@dataclass
class NormalizedTransaction:
    row_index: int
    transaction_date: date | None
    effective_date: date | None
    amount: Decimal
    direction: str
    raw_description: str
    normalized_description: str
    payment_rail: str | None
    merchant_candidate: str | None
    reference: str | None
    balance_after: Decimal | None
    parser_confidence: float
    row_hash: str
    signed_amount: Decimal = Decimal("0.00")
    movement_type: str = "other_needs_review"
    merchant_base_name: str | None = None
    merchant_fingerprint: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass
class NormalizedTransactionFacts:
    transaction: NormalizedTransaction
    amount_source: str | None = None
    date_source: str | None = None
    balance_source: str | None = None
    description_continuation_detected: bool = False
    parser_row_quality: str | None = None


@dataclass
class InterpretedTransaction:
    interpretation_type: str
    interpretation_confidence: float
    interpretation_reason: str


@dataclass
class ClassifiedTransaction:
    row_index: int
    transaction_date: date | None
    amount: Decimal
    direction: str
    raw_description: str
    normalized_description: str
    payment_rail: str | None
    merchant_candidate: str | None
    interpretation_type: str
    interpretation_confidence: float
    interpretation_reason: str
    classification_type: str
    category: str
    subcategory: str
    confidence: float
    explanation: str
    evidence_source: str
    group_key: str | None
    inferred_cadence: str | None
    cadence_confidence: float | None
    cadence_reason: str | None
    impact_on_baseline: str
    included: bool
    observed_only: bool
    signed_amount: Decimal = Decimal("0.00")
    movement_type: str = "other_needs_review"
    merchant_base_name: str | None = None
    merchant_fingerprint: str | None = None
    review_reasons: list[str] = field(default_factory=list)
    metadata: dict[str, object] = field(default_factory=dict)


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


@dataclass
class TransferAssessment:
    state: str
    confidence: float
    reasons: list[str] = field(default_factory=list)


@dataclass
class IncomeAssessment:
    state: str
    confidence: float
    reasons: list[str] = field(default_factory=list)
    category: str = "Income"
    subcategory: str = "Other Income"


@dataclass
class RecurrenceAssessment:
    state: str
    confidence: float
    cadence: str | None = None
    reasons: list[str] = field(default_factory=list)
    occurrence_count: int = 0
    historical_occurrence_count: int = 0
    median_amount: Decimal = Decimal("0.00")


@dataclass
class ExpenseAssessment:
    proposed_bucket: str
    confidence: float
    category: str
    subcategory: str
    reasons: list[str] = field(default_factory=list)
    review_flags: list[str] = field(default_factory=list)
    bucket_lean: str = "discretionary"
    baseline_eligible: bool = False
    ancillary: bool = False


@dataclass
class ResolvedTransaction:
    row_index: int
    transaction_date: date | None
    amount: Decimal
    signed_amount: Decimal
    direction: str
    raw_description: str
    normalized_description: str
    payment_rail: str | None
    merchant_candidate: str | None
    merchant_base_name: str | None
    merchant_fingerprint: str | None
    movement_type: str
    final_bucket: str
    interpretation_type: str
    interpretation_confidence: float
    interpretation_reason: str
    classification_type: str
    category: str
    subcategory: str
    confidence: float
    reasons: list[str] = field(default_factory=list)
    review_flags: list[str] = field(default_factory=list)
    evidence_source: str = "rule"
    mapping_source: str | None = None
    bucket_assignment: str = "variable_discretionary"
    included: bool = True
    observed_only: bool = True
    impact_on_baseline: str = "included"
    inferred_cadence: str | None = None
    cadence_confidence: float | None = None
    cadence_reason: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass
class SectionAggregate:
    totals: dict[str, Decimal] = field(default_factory=dict)
    credit_totals: dict[str, Decimal] = field(default_factory=dict)
    debit_totals: dict[str, Decimal] = field(default_factory=dict)
    transaction_count_by_bucket: dict[str, int] = field(default_factory=dict)


@dataclass
class StatementDiagnostics:
    classification_reconciliation_status: str
    classification_reconciliation_difference: Decimal
    section_confidence: dict[str, float] = field(default_factory=dict)
    statement_model_confidence: float = 0.0
    review_queue: list[dict[str, object]] = field(default_factory=list)
    hard_warnings: list[str] = field(default_factory=list)


@dataclass
class BudgetLine:
    group_key: str
    group_label: str
    line_type: str
    category: str
    subcategory: str
    inferred_cadence: str
    cadence_confidence: float
    cadence_reason: str
    observed_only: bool
    bucket_assignment: str
    modeling_status: str
    recurrence_state: str
    is_modeled: bool
    modeled_by_default: bool
    base_amount: Decimal
    base_period: str
    authoritative_field: str
    source_amount: Decimal
    source_period: str
    observed_window_total: Decimal
    normalized_weekly: Decimal
    normalized_fortnightly: Decimal
    normalized_monthly: Decimal
    normalized_yearly: Decimal
    reserve_monthly_equivalent: Decimal
    impact_on_baseline: str
    included: bool
    confidence: float
    merchant_confidence: float
    bucket_confidence: float
    explanation: str
    notes: str | None
    transaction_count: int
    observed_amount: Decimal
    observed_frequency_label: str
    movement_type: str = "other_needs_review"
    line_trust_level: str = "needs_review"
    modeling_eligible: bool = False
    modeling_block_reason: str | None = None
    classification_version: str | None = None
    mapping_source: str | None = None
    line_integrity_status: str = "verified"
    duplicate_group_candidates: list[dict[str, object]] = field(default_factory=list)
    merge_candidate_confidence: float = 0.0
    row_indexes: list[int] = field(default_factory=list)
    review_reasons: list[str] = field(default_factory=list)
    metadata: dict[str, object] = field(default_factory=dict)
