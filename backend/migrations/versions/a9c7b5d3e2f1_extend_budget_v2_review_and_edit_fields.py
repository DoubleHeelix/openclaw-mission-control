"""extend budget v2 review and edit fields

Revision ID: a9c7b5d3e2f1
Revises: f1b2c3d4e5f6
Create Date: 2026-03-19 14:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "a9c7b5d3e2f1"
down_revision = "f1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("budget_transaction_classifications", sa.Column("interpretation_confidence", sa.Float(), nullable=False, server_default="0"))
    op.add_column("budget_transaction_classifications", sa.Column("interpretation_reason", sa.String(), nullable=False, server_default=""))
    op.add_column("budget_transaction_classifications", sa.Column("cadence_reason", sa.String(), nullable=True))
    op.add_column("budget_transaction_classifications", sa.Column("observed_only", sa.Boolean(), nullable=False, server_default=sa.false()))
    op.add_column("budget_transaction_classifications", sa.Column("review_reasons", sa.JSON(), nullable=False, server_default="[]"))
    op.create_index("ix_budget_transaction_classifications_observed_only", "budget_transaction_classifications", ["observed_only"])

    op.add_column("budget_merchant_memory", sa.Column("merchant_fingerprint", sa.String(), nullable=True))
    op.create_index("ix_budget_merchant_memory_merchant_fingerprint", "budget_merchant_memory", ["merchant_fingerprint"])

    op.add_column("budget_line_items", sa.Column("cadence_reason", sa.String(), nullable=False, server_default=""))
    op.add_column("budget_line_items", sa.Column("observed_only", sa.Boolean(), nullable=False, server_default=sa.false()))
    op.add_column("budget_line_items", sa.Column("bucket_assignment", sa.String(), nullable=False, server_default="observed_discretionary"))
    op.add_column("budget_line_items", sa.Column("base_amount", sa.Numeric(18, 2), nullable=False, server_default="0"))
    op.add_column("budget_line_items", sa.Column("base_period", sa.String(), nullable=False, server_default="monthly"))
    op.add_column("budget_line_items", sa.Column("authoritative_field", sa.String(), nullable=False, server_default="base_amount"))
    op.add_column("budget_line_items", sa.Column("source_amount", sa.Numeric(18, 2), nullable=False, server_default="0"))
    op.add_column("budget_line_items", sa.Column("source_period", sa.String(), nullable=False, server_default="monthly"))
    op.add_column("budget_line_items", sa.Column("notes", sa.String(), nullable=True))
    op.add_column("budget_line_items", sa.Column("review_reasons", sa.JSON(), nullable=False, server_default="[]"))
    op.create_index("ix_budget_line_items_observed_only", "budget_line_items", ["observed_only"])
    op.create_index("ix_budget_line_items_bucket_assignment", "budget_line_items", ["bucket_assignment"])


def downgrade() -> None:
    op.drop_index("ix_budget_line_items_bucket_assignment", table_name="budget_line_items")
    op.drop_index("ix_budget_line_items_observed_only", table_name="budget_line_items")
    op.drop_column("budget_line_items", "review_reasons")
    op.drop_column("budget_line_items", "notes")
    op.drop_column("budget_line_items", "source_period")
    op.drop_column("budget_line_items", "source_amount")
    op.drop_column("budget_line_items", "authoritative_field")
    op.drop_column("budget_line_items", "base_period")
    op.drop_column("budget_line_items", "base_amount")
    op.drop_column("budget_line_items", "bucket_assignment")
    op.drop_column("budget_line_items", "observed_only")
    op.drop_column("budget_line_items", "cadence_reason")

    op.drop_index("ix_budget_merchant_memory_merchant_fingerprint", table_name="budget_merchant_memory")
    op.drop_column("budget_merchant_memory", "merchant_fingerprint")

    op.drop_index("ix_budget_transaction_classifications_observed_only", table_name="budget_transaction_classifications")
    op.drop_column("budget_transaction_classifications", "review_reasons")
    op.drop_column("budget_transaction_classifications", "observed_only")
    op.drop_column("budget_transaction_classifications", "cadence_reason")
    op.drop_column("budget_transaction_classifications", "interpretation_reason")
    op.drop_column("budget_transaction_classifications", "interpretation_confidence")
