# Copyright 2017 the HERA Collaboration
# Licensed under the 2-clause BSD License.

"""refactor stores

Revision ID: baf3ead3d88b
Revises: fa863eafacb0
Create Date: 2023-11-28 14:57:43.537445

"""
import sqlalchemy as sa

from alembic import op

revision = "baf3ead3d88b"
down_revision = "fa863eafacb0"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "store_metadata",
        sa.Column("id", sa.Integer(), nullable=False, autoincrement=True),
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column("store_type", sa.Integer(), nullable=False),
        sa.Column("store_data", sa.PickleType(), nullable=True),
        sa.Column("transfer_manager_data", sa.PickleType(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )


def downgrade():
    op.drop_table("store_metadata")
