"""Schemas for Budget V2 ingestion, review, overrides, and reporting."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from sqlmodel import Field, SQLModel


BudgetCadence = Literal["weekly", "fortnightly", "monthly", "quarterly", "yearly", "irregular", "unknown"]
BudgetImpact = Literal["included", "excluded", "reserve_only"]


class BudgetParserCapability(SQLModel):
    name: str
    banks: list[str] = Field(default_factory=list)
    formats: list[str] = Field(default_factory=lambda: ["pdf"])


class BudgetParserListResponse(SQLModel):
    parsers: list[BudgetParserCapability] = Field(default_factory=list)


class BudgetImportResponse(SQLModel):
    import_id: UUID
    status: str
    source_bank: str | None = None
    parser_name: str | None = None
    parser_confidence: float | None = None
    parser_warnings: list[str] = Field(default_factory=list)
    transaction_count: int = 0


class BudgetResetResponse(SQLModel):
    reset: bool = True
    deleted_import_count: int = 0


class BudgetStatementTruth(SQLModel):
    total_credits: Decimal = Decimal("0")
    total_debits: Decimal = Decimal("0")
    net_movement: Decimal = Decimal("0")
    opening_balance: Decimal | None = None
    closing_balance: Decimal | None = None
    expected_closing_balance: Decimal | None = None
    reconciliation_difference: Decimal = Decimal("0")
    reconciliation_status: str = "unknown"
    truth_trust_level: str = "needs_review"


class BudgetModelTotalsResponse(SQLModel):
    recurring_income_monthly: Decimal = Decimal("0")
    recurring_baseline_monthly: Decimal = Decimal("0")
    variable_discretionary_monthly: Decimal = Decimal("0")
    observed_one_off_total: Decimal = Decimal("0")
    observed_transfer_total: Decimal = Decimal("0")
    irregular_income_total: Decimal = Decimal("0")
    core_net: Decimal = Decimal("0")
    observed_net: Decimal = Decimal("0")
    modeling_allowed: bool = False
    modeling_restrictions: list[str] = Field(default_factory=list)
    classification_reconciliation_status: str | None = None
    classification_reconciliation_difference: Decimal | None = None
    classified_section_totals: dict[str, Decimal] = Field(default_factory=dict)
    section_confidence: dict[str, float] = Field(default_factory=dict)
    statement_model_confidence: float | None = None


class BudgetTrustResponse(SQLModel):
    reconciliation_status: str = "unknown"
    totals_trust_level: str = "needs_review"
    truth_trust_level: str = "needs_review"
    modeling_allowed: bool = False
    modeling_restrictions: list[str] = Field(default_factory=list)
    trust_reasons: list[str] = Field(default_factory=list)


class BudgetImportSummaryResponse(SQLModel):
    import_id: UUID
    status: str
    source_bank: str | None = None
    source_format: str = "pdf"
    parser_name: str | None = None
    parser_confidence: float | None = None
    parser_warnings: list[str] = Field(default_factory=list)
    statement_start_date: date | None = None
    statement_end_date: date | None = None
    transaction_count: int = 0
    extracted_debit_total: Decimal = Decimal("0")
    extracted_credit_total: Decimal = Decimal("0")
    parsed_debit_count: int = 0
    parsed_credit_count: int = 0
    parsed_debit_total: Decimal = Decimal("0")
    parsed_credit_total: Decimal = Decimal("0")
    opening_balance: Decimal | None = None
    closing_balance: Decimal | None = None
    statement_total_debits: Decimal | None = None
    statement_total_credits: Decimal | None = None
    expected_closing_balance: Decimal | None = None
    reconciliation_status: str = "unknown"
    reconciliation_reason: str | None = None
    reconciliation_difference: Decimal = Decimal("0")
    warning_reasons: list[str] = Field(default_factory=list)
    document_type: str | None = None
    document_reconcilable: bool | None = None
    document_warnings: list[str] = Field(default_factory=list)
    text_extraction_length: int = 0
    table_detected: bool = False
    page_resume_count: int = 0
    page_transaction_counts: dict[str, int] = Field(default_factory=dict)
    coverage_estimate: float | None = None
    suspected_missing_pages: bool = False
    duplicate_rows_detected: int = 0
    overlap_status: str = "clear"
    parser_failure_reason: str | None = None
    row_quality_counts: dict[str, int] = Field(default_factory=dict)
    direction_source_counts: dict[str, int] = Field(default_factory=dict)
    needs_review: bool = False
    low_confidence_group_count: int = 0
    uncategorized_review_count: int = 0
    scope_warnings: list[str] = Field(default_factory=list)
    parser_coverage_warnings: list[str] = Field(default_factory=list)
    statement_truth: BudgetStatementTruth = Field(default_factory=BudgetStatementTruth)
    budget_model: BudgetModelTotalsResponse = Field(default_factory=BudgetModelTotalsResponse)
    trust: BudgetTrustResponse = Field(default_factory=BudgetTrustResponse)
    updated_at: datetime


class BudgetTransactionReviewRow(SQLModel):
    id: UUID
    transaction_date: date | None = None
    effective_date: date | None = None
    amount: Decimal
    signed_amount: Decimal = Decimal("0")
    direction: Literal["credit", "debit"]
    movement_type: str = "other_needs_review"
    raw_description: str
    normalized_description: str
    merchant_base_name: str | None = None
    merchant_fingerprint: str | None = None
    direction_source: str = "fallback"
    parser_flags: list[str] = Field(default_factory=list)
    row_quality_bucket: str | None = None
    row_shape_confidence: float | None = None
    amount_parse_confidence: float | None = None
    date_parse_confidence: float | None = None
    section_context_confidence: float | None = None
    amount_source: str | None = None
    date_source: str | None = None
    balance_source: str | None = None
    description_continuation_detected: bool = False
    classification_version: str | None = None
    mapping_source: str | None = None
    transaction_classification_confidence: float | None = None
    final_bucket: str | None = None
    reasons: list[str] = Field(default_factory=list)
    review_flags: list[str] = Field(default_factory=list)
    payment_rail: str | None = None
    merchant_candidate: str | None = None
    interpretation_type: str
    interpretation_confidence: float = 0.0
    interpretation_reason: str = ""
    classification_type: str
    category: str
    subcategory: str
    confidence: float
    merchant_confidence: float = 0.0
    bucket_confidence: float = 0.0
    explanation: str
    evidence_source: str
    bucket_assignment: str = "observed_discretionary"
    confidence_label: str = "Needs review"
    inferred_cadence: BudgetCadence | None = None
    cadence_confidence: float | None = None
    cadence_reason: str | None = None
    impact_on_baseline: BudgetImpact = "included"
    included: bool = True
    observed_only: bool = False
    review_reasons: list[str] = Field(default_factory=list)
    group_key: str | None = None
    group_transaction_count: int = 0
    review_priority: float = 0.0
    likely_merge_targets: list[dict[str, object]] = Field(default_factory=list)
    likely_payroll_candidate: bool = False


class BudgetTransactionsResponse(SQLModel):
    items: list[BudgetTransactionReviewRow] = Field(default_factory=list)
    total: int = 0


class BudgetNeedsReviewResponse(SQLModel):
    items: list[BudgetTransactionReviewRow] = Field(default_factory=list)
    total: int = 0


class BudgetLineItemResponse(SQLModel):
    id: UUID
    group_key: str
    group_label: str
    line_type: str
    category: str
    subcategory: str
    inferred_cadence: BudgetCadence
    cadence_confidence: float
    cadence_reason: str
    observed_only: bool
    bucket_assignment: str
    bucket_suggestion: str = "suggested_discretionary"
    modeling_status: str = "observational_only"
    recurrence_state: str = "unknown"
    is_modeled: bool = False
    modeled_by_default: bool = False
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
    impact_on_baseline: BudgetImpact
    included: bool
    confidence: float
    merchant_confidence: float = 0.0
    bucket_confidence: float = 0.0
    confidence_label: str = "Needs review"
    explanation: str
    baseline_decision_reason: str | None = None
    notes: str | None = None
    review_reasons: list[str] = Field(default_factory=list)
    transaction_count: int
    observed_amount: Decimal = Decimal("0")
    observational_monthly_estimate: Decimal | None = None
    observed_frequency_label: str = ""
    line_trust_level: str = "needs_review"
    modeling_eligible: bool = False
    modeling_block_reason: str | None = None
    classification_version: str | None = None
    mapping_source: str | None = None
    line_integrity_status: str | None = None
    final_bucket: str | None = None
    section_confidence: float | None = None
    reasons: list[str] = Field(default_factory=list)
    review_flags: list[str] = Field(default_factory=list)
    duplicate_group_candidates: list[dict[str, object]] = Field(default_factory=list)
    merge_candidate_confidence: float = 0.0
    movement_type: str = "other_needs_review"
    trust_level: str = "needs_review"
    modeling_allowed: bool = False


class BudgetLinesResponse(SQLModel):
    items: list[BudgetLineItemResponse] = Field(default_factory=list)
    totals: dict[str, Decimal] = Field(default_factory=dict)
    statement_truth: BudgetStatementTruth = Field(default_factory=BudgetStatementTruth)
    budget_model: BudgetModelTotalsResponse = Field(default_factory=BudgetModelTotalsResponse)
    trust: BudgetTrustResponse = Field(default_factory=BudgetTrustResponse)


class BudgetOverrideOperation(SQLModel):
    target_type: Literal["transaction", "group"]
    target_id: str
    operation: Literal[
        "set_category",
        "set_subcategory",
        "set_cadence",
        "set_include",
        "set_bucket_assignment",
        "set_base_amount_period",
        "set_notes",
        "set_authoritative_period_values",
        "set_normalized_monthly",
        "remember_mapping",
        "forget_mapping",
        "merge_group",
        "split_group",
        "reassign_transactions",
        "mark_recurring",
        "mark_one_off",
    ]
    payload: dict[str, object] = Field(default_factory=dict)
    persist_memory: bool = False


class BudgetOverrideRequest(SQLModel):
    operations: list[BudgetOverrideOperation] = Field(default_factory=list)


class BudgetOverrideResponse(SQLModel):
    applied: int


class BudgetMerchantMemoryUpsertRequest(SQLModel):
    merchant_key: str
    category: str
    subcategory: str
    confidence: float = 0.95


class BudgetMerchantMemoryItem(SQLModel):
    id: UUID
    merchant_key: str
    merchant_fingerprint: str | None = None
    category: str
    subcategory: str
    confidence: float
    source: str
    mapping_source: str | None = None
    scope: str | None = None
    usage_count: int = 0
    active: bool


class BudgetMerchantMemoryListResponse(SQLModel):
    items: list[BudgetMerchantMemoryItem] = Field(default_factory=list)


class BudgetSnapshotSummary(SQLModel):
    observed_spend: Decimal = Decimal("0")
    monthly_recurring_income: Decimal = Decimal("0")
    monthly_irregular_income: Decimal = Decimal("0")
    monthly_recurring_baseline_expenses: Decimal = Decimal("0")
    monthly_variable_discretionary: Decimal = Decimal("0")
    monthly_one_off_exceptional: Decimal = Decimal("0")
    monthly_transfer_excluded: Decimal = Decimal("0")
    observed_variable_discretionary_total: Decimal = Decimal("0")
    observed_one_off_exceptional_total: Decimal = Decimal("0")
    observed_irregular_income_total: Decimal = Decimal("0")
    observed_transfer_total: Decimal = Decimal("0")
    net_recurring_monthly: Decimal = Decimal("0")
    net_observed_total: Decimal = Decimal("0")
    core_monthly_baseline: Decimal = Decimal("0")
    observed_discretionary_monthly: Decimal = Decimal("0")
    annual_infrequent_oneoff_spend: Decimal = Decimal("0")
    transfer_money_movement_spend: Decimal = Decimal("0")
    reserve_adjusted_monthly_cost: Decimal = Decimal("0")
    total_income_monthly: Decimal = Decimal("0")
    total_expenses_monthly: Decimal = Decimal("0")
    net_monthly: Decimal = Decimal("0")


class BudgetSnapshotResponse(SQLModel):
    import_id: UUID
    summary: BudgetSnapshotSummary
    statement_truth: BudgetStatementTruth = Field(default_factory=BudgetStatementTruth)
    budget_model: BudgetModelTotalsResponse = Field(default_factory=BudgetModelTotalsResponse)
    trust: BudgetTrustResponse = Field(default_factory=BudgetTrustResponse)
    generated_at: datetime
