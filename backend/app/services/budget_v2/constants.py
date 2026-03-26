"""Shared Budget V2 enums and constants."""

from __future__ import annotations

from enum import StrEnum


class MovementType(StrEnum):
    INCOME = "income"
    EXPENSE = "expense"
    INTERNAL_TRANSFER = "internal_transfer"
    DEBT_PAYMENT = "debt_payment"
    REFUND = "refund"
    FEE = "fee"
    CASH_WITHDRAWAL = "cash_withdrawal"
    OTHER_NEEDS_REVIEW = "other_needs_review"


class BucketAssignment(StrEnum):
    RECURRING_BASELINE = "recurring_baseline"
    VARIABLE_DISCRETIONARY = "variable_discretionary"
    ONE_OFF_EXCEPTIONAL = "one_off_exceptional"
    TRANSFER_MONEY_MOVEMENT = "transfer_money_movement"
    INCOME_RECURRING = "income_recurring"
    INCOME_IRREGULAR = "income_irregular"


class FinalBucket(StrEnum):
    INCOME = "income"
    RECURRING_BASELINE_EXPENSES = "recurring_baseline_expenses"
    VARIABLE_SPENDING = "variable_spending"
    ONE_OFF_SPENDING = "one_off_spending"
    TRANSFERS = "transfers"
    FEES = "fees"
    UNCATEGORIZED = "uncategorized"


class ReconciliationStatus(StrEnum):
    RECONCILED = "reconciled"
    PARSER_INCOMPLETE = "parser_incomplete"
    SOURCE_NON_RECONCILABLE = "source_non_reconcilable"
    EXTRACTION_DEGRADED = "extraction_degraded"
    FAILED_RECONCILIATION = "failed_reconciliation"
    UNKNOWN = "unknown"


class TrustLevel(StrEnum):
    VERIFIED = "verified"
    PROVISIONAL = "provisional"
    PARTIAL = "partial"
    FAILED_RECONCILIATION = "failed_reconciliation"
    NEEDS_REVIEW = "needs_review"


class ModelingState(StrEnum):
    MODELED_RECURRING = "modeled_recurring"
    OBSERVATIONAL_ONLY = "observational_only"
    USER_FORCED_RECURRING = "user_forced_recurring"
