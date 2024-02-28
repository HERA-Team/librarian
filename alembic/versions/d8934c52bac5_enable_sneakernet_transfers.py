# Copyright 2017 the HERA Collaboration
# Licensed under the 2-clause BSD License.

"""enable sneakernet transfers

Revision ID: d8934c52bac5
Revises: 71df5b41ae41
Create Date: 2024-02-28 15:48:44.705721

"""
import sqlalchemy as sa

from alembic import op

revision = "d8934c52bac5"
down_revision = "71df5b41ae41"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "store_metadata", sa.Column("enabled", sa.Boolean, nullable=False, default=True)
    )


def downgrade():
    op.drop_column("store_metadata", "enabled")
