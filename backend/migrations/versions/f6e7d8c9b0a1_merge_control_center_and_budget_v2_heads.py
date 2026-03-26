"""Merge legacy control-center branch with current Budget V2 head.

Revision ID: f6e7d8c9b0a1
Revises: a9c7b5d3e2f1, e4c2b19f8a7d
Create Date: 2026-03-20 01:25:00.000000
"""

from __future__ import annotations


# revision identifiers, used by Alembic.
revision = "f6e7d8c9b0a1"
down_revision = ("a9c7b5d3e2f1", "e4c2b19f8a7d")
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Merge heads without additional schema changes."""


def downgrade() -> None:
    """No-op downgrade for merge revision."""
