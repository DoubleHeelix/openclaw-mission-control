"""Typed persistence models for the Budget V2 engine."""

from __future__ import annotations

from datetime import datetime, date
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import JSON, Column, Numeric, Text
from sqlmodel import Field

from app.core.time import utcnow
from app.models.tenancy import TenantScoped


class BudgetImportSession(TenantScoped, table=True):
    __tablename__ = "budget_import_sessions"  # pyright: ignore[reportAssignmentType]

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    organization_id: UUID = Field(foreign_key="organizations.id", index=True)
    status: str = Field(default="pending", index=True)
    source_bank: str | None = Field(default=None, index=True)
    source_format: str = Field(default="pdf", index=True)
    parser_name: str | None = Field(default=None, index=True)
    parser_confidence: float | None = Field(default=None)
    parser_warnings: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    statement_start_date: date | None = Field(default=None)
    statement_end_date: date | None = Field(default=None)
    transaction_count: int = Field(default=0)
    extracted_debit_total: Decimal = Field(
        default=Decimal("0"), sa_column=Column(Numeric(18, 2), nullable=False)
    )
    extracted_credit_total: Decimal = Field(
        default=Decimal("0"), sa_column=Column(Numeric(18, 2), nullable=False)
    )
    opening_balance: Decimal | None = Field(default=None, sa_column=Column(Numeric(18, 2)))
    closing_balance: Decimal | None = Field(default=None, sa_column=Column(Numeric(18, 2)))
    reconciliation_status: str = Field(default="unknown", index=True)
    reconciliation_difference: Decimal = Field(
        default=Decimal("0"), sa_column=Column(Numeric(18, 2), nullable=False)
    )
    needs_review: bool = Field(default=False, index=True)
    low_confidence_group_count: int = Field(default=0)
    uncategorized_review_count: int = Field(default=0)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class BudgetRawFile(TenantScoped, table=True):
    __tablename__ = "budget_raw_files"  # pyright: ignore[reportAssignmentType]

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    organization_id: UUID = Field(foreign_key="organizations.id", index=True)
    import_session_id: UUID = Field(foreign_key="budget_import_sessions.id", index=True)
    filename: str
    content_type: str | None = None
    byte_size: int = Field(default=0)
    sha256: str = Field(index=True)
    raw_text: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow)


class BudgetParsedStatement(TenantScoped, table=True):
    __tablename__ = "budget_parsed_statements"  # pyright: ignore[reportAssignmentType]

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    organization_id: UUID = Field(foreign_key="organizations.id", index=True)
    import_session_id: UUID = Field(foreign_key="budget_import_sessions.id", index=True)
    statement_id: str = Field(index=True)
    bank_name: str | None = Field(default=None, index=True)
    account_name: str | None = None
    account_ref_masked: str | None = None
    statement_start_date: date | None = None
    statement_end_date: date | None = None
    parser_name: str
    parser_confidence: float = Field(default=0.0)
    parser_flags: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    extra_data: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column("metadata", JSON)
    )
    created_at: datetime = Field(default_factory=utcnow)


class BudgetParsedTransaction(TenantScoped, table=True):
    __tablename__ = "budget_parsed_transactions"  # pyright: ignore[reportAssignmentType]

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    organization_id: UUID = Field(foreign_key="organizations.id", index=True)
    import_session_id: UUID = Field(foreign_key="budget_import_sessions.id", index=True)
    parsed_statement_id: UUID = Field(foreign_key="budget_parsed_statements.id", index=True)
    row_index: int = Field(default=0, index=True)
    transaction_date: date | None = None
    effective_date: date | None = None
    amount: Decimal = Field(sa_column=Column(Numeric(18, 2), nullable=False))
    direction: str = Field(index=True)
    raw_description: str
    raw_reference: str | None = None
    balance_after: Decimal | None = Field(default=None, sa_column=Column(Numeric(18, 2)))
    page_number: int | None = None
    source_line_refs: list[int] = Field(default_factory=list, sa_column=Column(JSON))
    parser_flags: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    parser_confidence: float = Field(default=0.0)
    created_at: datetime = Field(default_factory=utcnow)


class BudgetNormalizedTransaction(TenantScoped, table=True):
    __tablename__ = "budget_normalized_transactions"  # pyright: ignore[reportAssignmentType]

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    organization_id: UUID = Field(foreign_key="organizations.id", index=True)
    import_session_id: UUID = Field(foreign_key="budget_import_sessions.id", index=True)
    parsed_transaction_id: UUID = Field(foreign_key="budget_parsed_transactions.id", index=True)
    normalized_description: str = Field(index=True)
    payment_rail: str | None = Field(default=None, index=True)
    merchant_candidate: str | None = Field(default=None, index=True)
    reference: str | None = None
    row_hash: str = Field(index=True)
    dedupe_rank: int = Field(default=0)
    extra_data: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column("metadata", JSON)
    )
    created_at: datetime = Field(default_factory=utcnow)


class BudgetTransactionClassification(TenantScoped, table=True):
    __tablename__ = "budget_transaction_classifications"  # pyright: ignore[reportAssignmentType]

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    organization_id: UUID = Field(foreign_key="organizations.id", index=True)
    import_session_id: UUID = Field(foreign_key="budget_import_sessions.id", index=True)
    normalized_transaction_id: UUID = Field(
        foreign_key="budget_normalized_transactions.id", index=True
    )
    interpretation_type: str = Field(index=True)
    interpretation_confidence: float = Field(default=0.0)
    interpretation_reason: str = Field(default="")
    classification_type: str = Field(index=True)
    category: str = Field(index=True)
    subcategory: str = Field(index=True)
    confidence: float = Field(default=0.0)
    explanation: str = Field(default="")
    evidence_source: str = Field(default="rule", index=True)
    group_key: str | None = Field(default=None, index=True)
    inferred_cadence: str | None = Field(default=None, index=True)
    cadence_confidence: float | None = None
    cadence_reason: str | None = None
    impact_on_baseline: str = Field(default="included", index=True)
    included: bool = Field(default=True, index=True)
    observed_only: bool = Field(default=False, index=True)
    review_reasons: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    extra_data: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column("metadata", JSON)
    )
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class BudgetMerchantMemory(TenantScoped, table=True):
    __tablename__ = "budget_merchant_memory"  # pyright: ignore[reportAssignmentType]

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    organization_id: UUID = Field(foreign_key="organizations.id", index=True)
    merchant_key: str = Field(index=True)
    merchant_fingerprint: str | None = Field(default=None, index=True)
    category: str
    subcategory: str
    confidence: float = Field(default=0.0)
    source: str = Field(default="manual")
    active: bool = Field(default=True, index=True)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class BudgetManualOverride(TenantScoped, table=True):
    __tablename__ = "budget_manual_overrides"  # pyright: ignore[reportAssignmentType]

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    organization_id: UUID = Field(foreign_key="organizations.id", index=True)
    import_session_id: UUID = Field(foreign_key="budget_import_sessions.id", index=True)
    target_type: str = Field(index=True)
    target_id: str = Field(index=True)
    operation: str = Field(index=True)
    payload: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    created_by_user_id: UUID | None = Field(default=None, foreign_key="users.id", index=True)
    created_at: datetime = Field(default_factory=utcnow)


class BudgetLineItem(TenantScoped, table=True):
    __tablename__ = "budget_line_items"  # pyright: ignore[reportAssignmentType]

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    organization_id: UUID = Field(foreign_key="organizations.id", index=True)
    import_session_id: UUID = Field(foreign_key="budget_import_sessions.id", index=True)
    group_key: str = Field(index=True)
    group_label: str
    line_type: str = Field(index=True)
    category: str = Field(index=True)
    subcategory: str = Field(index=True)
    inferred_cadence: str = Field(default="irregular", index=True)
    cadence_confidence: float = Field(default=0.0)
    cadence_reason: str = Field(default="")
    observed_only: bool = Field(default=False, index=True)
    bucket_assignment: str = Field(default="observed_discretionary", index=True)
    base_amount: Decimal = Field(default=Decimal("0"), sa_column=Column(Numeric(18, 2), nullable=False))
    base_period: str = Field(default="monthly")
    authoritative_field: str = Field(default="base_amount")
    source_amount: Decimal = Field(default=Decimal("0"), sa_column=Column(Numeric(18, 2), nullable=False))
    source_period: str = Field(default="monthly")
    observed_window_total: Decimal = Field(sa_column=Column(Numeric(18, 2), nullable=False))
    normalized_weekly: Decimal = Field(sa_column=Column(Numeric(18, 2), nullable=False))
    normalized_fortnightly: Decimal = Field(sa_column=Column(Numeric(18, 2), nullable=False))
    normalized_monthly: Decimal = Field(sa_column=Column(Numeric(18, 2), nullable=False))
    normalized_yearly: Decimal = Field(sa_column=Column(Numeric(18, 2), nullable=False))
    reserve_monthly_equivalent: Decimal = Field(
        default=Decimal("0"), sa_column=Column(Numeric(18, 2), nullable=False)
    )
    impact_on_baseline: str = Field(default="included", index=True)
    included: bool = Field(default=True, index=True)
    confidence: float = Field(default=0.0)
    explanation: str = Field(default="")
    notes: str | None = Field(default=None)
    review_reasons: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    transaction_count: int = Field(default=0)
    extra_data: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column("metadata", JSON)
    )
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class BudgetSnapshot(TenantScoped, table=True):
    __tablename__ = "budget_snapshots"  # pyright: ignore[reportAssignmentType]

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    organization_id: UUID = Field(foreign_key="organizations.id", index=True)
    import_session_id: UUID = Field(foreign_key="budget_import_sessions.id", index=True)
    summary: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
