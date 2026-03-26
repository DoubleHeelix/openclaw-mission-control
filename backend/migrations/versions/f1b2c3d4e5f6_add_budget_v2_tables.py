"""add budget v2 tables

Revision ID: f1b2c3d4e5f6
Revises: fa6e83f8d9a1, b497b348ebb4
Create Date: 2026-03-18 16:45:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f1b2c3d4e5f6"
down_revision = ("fa6e83f8d9a1", "b497b348ebb4")
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "budget_import_sessions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("source_bank", sa.String(), nullable=True),
        sa.Column("source_format", sa.String(), nullable=False),
        sa.Column("parser_name", sa.String(), nullable=True),
        sa.Column("parser_confidence", sa.Float(), nullable=True),
        sa.Column("parser_warnings", sa.JSON(), nullable=False),
        sa.Column("statement_start_date", sa.Date(), nullable=True),
        sa.Column("statement_end_date", sa.Date(), nullable=True),
        sa.Column("transaction_count", sa.Integer(), nullable=False),
        sa.Column("extracted_debit_total", sa.Numeric(18, 2), nullable=False),
        sa.Column("extracted_credit_total", sa.Numeric(18, 2), nullable=False),
        sa.Column("opening_balance", sa.Numeric(18, 2), nullable=True),
        sa.Column("closing_balance", sa.Numeric(18, 2), nullable=True),
        sa.Column("reconciliation_status", sa.String(), nullable=False),
        sa.Column("reconciliation_difference", sa.Numeric(18, 2), nullable=False),
        sa.Column("needs_review", sa.Boolean(), nullable=False),
        sa.Column("low_confidence_group_count", sa.Integer(), nullable=False),
        sa.Column("uncategorized_review_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_budget_import_sessions_organization_id", "budget_import_sessions", ["organization_id"]
    )
    op.create_index("ix_budget_import_sessions_status", "budget_import_sessions", ["status"])
    op.create_index(
        "ix_budget_import_sessions_source_bank", "budget_import_sessions", ["source_bank"]
    )
    op.create_index(
        "ix_budget_import_sessions_reconciliation_status",
        "budget_import_sessions",
        ["reconciliation_status"],
    )

    op.create_table(
        "budget_raw_files",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("import_session_id", sa.Uuid(), nullable=False),
        sa.Column("filename", sa.String(), nullable=False),
        sa.Column("content_type", sa.String(), nullable=True),
        sa.Column("byte_size", sa.Integer(), nullable=False),
        sa.Column("sha256", sa.String(), nullable=False),
        sa.Column("raw_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["import_session_id"], ["budget_import_sessions.id"]),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_budget_raw_files_organization_id", "budget_raw_files", ["organization_id"])
    op.create_index("ix_budget_raw_files_import_session_id", "budget_raw_files", ["import_session_id"])
    op.create_index("ix_budget_raw_files_sha256", "budget_raw_files", ["sha256"])

    op.create_table(
        "budget_parsed_statements",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("import_session_id", sa.Uuid(), nullable=False),
        sa.Column("statement_id", sa.String(), nullable=False),
        sa.Column("bank_name", sa.String(), nullable=True),
        sa.Column("account_name", sa.String(), nullable=True),
        sa.Column("account_ref_masked", sa.String(), nullable=True),
        sa.Column("statement_start_date", sa.Date(), nullable=True),
        sa.Column("statement_end_date", sa.Date(), nullable=True),
        sa.Column("parser_name", sa.String(), nullable=False),
        sa.Column("parser_confidence", sa.Float(), nullable=False),
        sa.Column("parser_flags", sa.JSON(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["import_session_id"], ["budget_import_sessions.id"]),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_budget_parsed_statements_organization_id", "budget_parsed_statements", ["organization_id"]
    )
    op.create_index(
        "ix_budget_parsed_statements_import_session_id",
        "budget_parsed_statements",
        ["import_session_id"],
    )
    op.create_index("ix_budget_parsed_statements_statement_id", "budget_parsed_statements", ["statement_id"])

    op.create_table(
        "budget_parsed_transactions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("import_session_id", sa.Uuid(), nullable=False),
        sa.Column("parsed_statement_id", sa.Uuid(), nullable=False),
        sa.Column("row_index", sa.Integer(), nullable=False),
        sa.Column("transaction_date", sa.Date(), nullable=True),
        sa.Column("effective_date", sa.Date(), nullable=True),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("direction", sa.String(), nullable=False),
        sa.Column("raw_description", sa.String(), nullable=False),
        sa.Column("raw_reference", sa.String(), nullable=True),
        sa.Column("balance_after", sa.Numeric(18, 2), nullable=True),
        sa.Column("page_number", sa.Integer(), nullable=True),
        sa.Column("source_line_refs", sa.JSON(), nullable=False),
        sa.Column("parser_flags", sa.JSON(), nullable=False),
        sa.Column("parser_confidence", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["import_session_id"], ["budget_import_sessions.id"]),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"]),
        sa.ForeignKeyConstraint(["parsed_statement_id"], ["budget_parsed_statements.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_budget_parsed_transactions_organization_id",
        "budget_parsed_transactions",
        ["organization_id"],
    )
    op.create_index(
        "ix_budget_parsed_transactions_import_session_id",
        "budget_parsed_transactions",
        ["import_session_id"],
    )
    op.create_index(
        "ix_budget_parsed_transactions_parsed_statement_id",
        "budget_parsed_transactions",
        ["parsed_statement_id"],
    )
    op.create_index("ix_budget_parsed_transactions_row_index", "budget_parsed_transactions", ["row_index"])
    op.create_index("ix_budget_parsed_transactions_direction", "budget_parsed_transactions", ["direction"])

    op.create_table(
        "budget_normalized_transactions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("import_session_id", sa.Uuid(), nullable=False),
        sa.Column("parsed_transaction_id", sa.Uuid(), nullable=False),
        sa.Column("normalized_description", sa.String(), nullable=False),
        sa.Column("payment_rail", sa.String(), nullable=True),
        sa.Column("merchant_candidate", sa.String(), nullable=True),
        sa.Column("reference", sa.String(), nullable=True),
        sa.Column("row_hash", sa.String(), nullable=False),
        sa.Column("dedupe_rank", sa.Integer(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["import_session_id"], ["budget_import_sessions.id"]),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"]),
        sa.ForeignKeyConstraint(["parsed_transaction_id"], ["budget_parsed_transactions.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_budget_normalized_transactions_organization_id",
        "budget_normalized_transactions",
        ["organization_id"],
    )
    op.create_index(
        "ix_budget_normalized_transactions_import_session_id",
        "budget_normalized_transactions",
        ["import_session_id"],
    )
    op.create_index(
        "ix_budget_normalized_transactions_parsed_transaction_id",
        "budget_normalized_transactions",
        ["parsed_transaction_id"],
    )
    op.create_index(
        "ix_budget_normalized_transactions_normalized_description",
        "budget_normalized_transactions",
        ["normalized_description"],
    )
    op.create_index(
        "ix_budget_normalized_transactions_payment_rail",
        "budget_normalized_transactions",
        ["payment_rail"],
    )
    op.create_index(
        "ix_budget_normalized_transactions_merchant_candidate",
        "budget_normalized_transactions",
        ["merchant_candidate"],
    )
    op.create_index("ix_budget_normalized_transactions_row_hash", "budget_normalized_transactions", ["row_hash"])

    op.create_table(
        "budget_transaction_classifications",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("import_session_id", sa.Uuid(), nullable=False),
        sa.Column("normalized_transaction_id", sa.Uuid(), nullable=False),
        sa.Column("interpretation_type", sa.String(), nullable=False),
        sa.Column("classification_type", sa.String(), nullable=False),
        sa.Column("category", sa.String(), nullable=False),
        sa.Column("subcategory", sa.String(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("explanation", sa.String(), nullable=False),
        sa.Column("evidence_source", sa.String(), nullable=False),
        sa.Column("group_key", sa.String(), nullable=True),
        sa.Column("inferred_cadence", sa.String(), nullable=True),
        sa.Column("cadence_confidence", sa.Float(), nullable=True),
        sa.Column("impact_on_baseline", sa.String(), nullable=False),
        sa.Column("included", sa.Boolean(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["import_session_id"], ["budget_import_sessions.id"]),
        sa.ForeignKeyConstraint(["normalized_transaction_id"], ["budget_normalized_transactions.id"]),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    for idx in [
        "organization_id",
        "import_session_id",
        "normalized_transaction_id",
        "interpretation_type",
        "classification_type",
        "category",
        "subcategory",
        "evidence_source",
        "group_key",
        "inferred_cadence",
        "impact_on_baseline",
        "included",
    ]:
        op.create_index(
            f"ix_budget_transaction_classifications_{idx}",
            "budget_transaction_classifications",
            [idx],
        )

    op.create_table(
        "budget_merchant_memory",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("merchant_key", sa.String(), nullable=False),
        sa.Column("category", sa.String(), nullable=False),
        sa.Column("subcategory", sa.String(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_budget_merchant_memory_organization_id", "budget_merchant_memory", ["organization_id"])
    op.create_index("ix_budget_merchant_memory_merchant_key", "budget_merchant_memory", ["merchant_key"])
    op.create_index("ix_budget_merchant_memory_active", "budget_merchant_memory", ["active"])

    op.create_table(
        "budget_manual_overrides",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("import_session_id", sa.Uuid(), nullable=False),
        sa.Column("target_type", sa.String(), nullable=False),
        sa.Column("target_id", sa.String(), nullable=False),
        sa.Column("operation", sa.String(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_by_user_id", sa.Uuid(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["import_session_id"], ["budget_import_sessions.id"]),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    for idx in ["organization_id", "import_session_id", "target_type", "target_id", "operation", "created_by_user_id"]:
        op.create_index(f"ix_budget_manual_overrides_{idx}", "budget_manual_overrides", [idx])

    op.create_table(
        "budget_line_items",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("import_session_id", sa.Uuid(), nullable=False),
        sa.Column("group_key", sa.String(), nullable=False),
        sa.Column("group_label", sa.String(), nullable=False),
        sa.Column("line_type", sa.String(), nullable=False),
        sa.Column("category", sa.String(), nullable=False),
        sa.Column("subcategory", sa.String(), nullable=False),
        sa.Column("inferred_cadence", sa.String(), nullable=False),
        sa.Column("cadence_confidence", sa.Float(), nullable=False),
        sa.Column("observed_window_total", sa.Numeric(18, 2), nullable=False),
        sa.Column("normalized_weekly", sa.Numeric(18, 2), nullable=False),
        sa.Column("normalized_fortnightly", sa.Numeric(18, 2), nullable=False),
        sa.Column("normalized_monthly", sa.Numeric(18, 2), nullable=False),
        sa.Column("normalized_yearly", sa.Numeric(18, 2), nullable=False),
        sa.Column("reserve_monthly_equivalent", sa.Numeric(18, 2), nullable=False),
        sa.Column("impact_on_baseline", sa.String(), nullable=False),
        sa.Column("included", sa.Boolean(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("explanation", sa.String(), nullable=False),
        sa.Column("transaction_count", sa.Integer(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["import_session_id"], ["budget_import_sessions.id"]),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    for idx in [
        "organization_id",
        "import_session_id",
        "group_key",
        "line_type",
        "category",
        "subcategory",
        "inferred_cadence",
        "impact_on_baseline",
        "included",
    ]:
        op.create_index(f"ix_budget_line_items_{idx}", "budget_line_items", [idx])

    op.create_table(
        "budget_snapshots",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("import_session_id", sa.Uuid(), nullable=False),
        sa.Column("summary", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["import_session_id"], ["budget_import_sessions.id"]),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_budget_snapshots_organization_id", "budget_snapshots", ["organization_id"])
    op.create_index("ix_budget_snapshots_import_session_id", "budget_snapshots", ["import_session_id"])


def downgrade() -> None:
    for table in [
        "budget_snapshots",
        "budget_line_items",
        "budget_manual_overrides",
        "budget_merchant_memory",
        "budget_transaction_classifications",
        "budget_normalized_transactions",
        "budget_parsed_transactions",
        "budget_parsed_statements",
        "budget_raw_files",
        "budget_import_sessions",
    ]:
        op.drop_table(table)
