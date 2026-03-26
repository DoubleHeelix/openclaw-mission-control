"""Add control-center config and records tables.

Revision ID: d13a4f7c9b21
Revises: b497b348ebb4
Create Date: 2026-02-24 17:55:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "d13a4f7c9b21"
down_revision = "b497b348ebb4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create control-center persistence tables."""
    op.create_table(
        "control_center_configs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("modules", sa.JSON(), nullable=False),
        sa.Column("network_marketing_view_mode", sa.String(), nullable=False, server_default="pipeline"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "organization_id",
            name="uq_control_center_configs_organization_id",
        ),
    )
    op.create_index(
        "ix_control_center_configs_organization_id",
        "control_center_configs",
        ["organization_id"],
        unique=False,
    )

    op.create_table(
        "control_center_records",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("organization_id", sa.Uuid(), nullable=False),
        sa.Column("module_id", sa.String(), nullable=False),
        sa.Column("module_slug", sa.String(), nullable=False),
        sa.Column("module_category", sa.String(), nullable=False),
        sa.Column("title", sa.String(), nullable=False),
        sa.Column("summary", sa.String(), nullable=True),
        sa.Column("stage", sa.String(), nullable=True),
        sa.Column("data", sa.JSON(), nullable=False),
        sa.Column("linked_task_id", sa.Uuid(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["linked_task_id"], ["tasks.id"]),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_control_center_records_organization_id",
        "control_center_records",
        ["organization_id"],
        unique=False,
    )
    op.create_index(
        "ix_control_center_records_module_id",
        "control_center_records",
        ["module_id"],
        unique=False,
    )
    op.create_index(
        "ix_control_center_records_module_slug",
        "control_center_records",
        ["module_slug"],
        unique=False,
    )
    op.create_index(
        "ix_control_center_records_module_category",
        "control_center_records",
        ["module_category"],
        unique=False,
    )
    op.create_index(
        "ix_control_center_records_linked_task_id",
        "control_center_records",
        ["linked_task_id"],
        unique=False,
    )


def downgrade() -> None:
    """Drop control-center persistence tables."""
    op.drop_index(
        "ix_control_center_records_linked_task_id",
        table_name="control_center_records",
    )
    op.drop_index(
        "ix_control_center_records_module_category",
        table_name="control_center_records",
    )
    op.drop_index(
        "ix_control_center_records_module_slug",
        table_name="control_center_records",
    )
    op.drop_index("ix_control_center_records_module_id", table_name="control_center_records")
    op.drop_index(
        "ix_control_center_records_organization_id",
        table_name="control_center_records",
    )
    op.drop_table("control_center_records")

    op.drop_index(
        "ix_control_center_configs_organization_id",
        table_name="control_center_configs",
    )
    op.drop_table("control_center_configs")
