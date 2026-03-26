"""Backfill missing network_marketing_view_mode column on control_center_configs.

Revision ID: e4c2b19f8a7d
Revises: d13a4f7c9b21
Create Date: 2026-02-25 03:30:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "e4c2b19f8a7d"
down_revision = "d13a4f7c9b21"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add missing view-mode column for drifted deployments."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("control_center_configs")}
    if "network_marketing_view_mode" not in columns:
        op.add_column(
            "control_center_configs",
            sa.Column(
                "network_marketing_view_mode",
                sa.String(),
                nullable=False,
                server_default="pipeline",
            ),
        )
    op.execute(
        sa.text(
            "UPDATE control_center_configs SET network_marketing_view_mode = 'pipeline' "
            "WHERE network_marketing_view_mode IS NULL OR network_marketing_view_mode = ''",
        ),
    )


def downgrade() -> None:
    """Drop view-mode column if present."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("control_center_configs")}
    if "network_marketing_view_mode" in columns:
        op.drop_column("control_center_configs", "network_marketing_view_mode")
