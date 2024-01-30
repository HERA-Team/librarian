# Copyright 2017 the HERA Collaboration
# Licensed under the 2-clause BSD License.

"""add incoming transfer table

Revision ID: 82ea9906e7d4
Revises: baf3ead3d88b
Create Date: 2023-12-01 15:10:41.994989

"""
import sqlalchemy as sa

from alembic import op

revision = "82ea9906e7d4"
down_revision = "baf3ead3d88b"
branch_labels = None
depends_on = None

table_name = "incoming_transfers"


def upgrade():
    op.create_table(
        table_name,
        sa.Column(
            "id",
            sa.Integer(),
            primary_key=True,
            autoincrement=True,
        ),
        sa.Column(
            "status",
            sa.Enum(
                "INITIATED",
                "STAGED",
                "ONGOING",
                "COMPLETED",
                "FAILED",
                "CANCELLED",
                name="TransferStatus",
            ),
            nullable=False,
        ),
        sa.Column("uploader", sa.String(256), nullable=False),
        sa.Column("transfer_size", sa.BigInteger(), nullable=False),
        sa.Column("store_id", sa.Integer()),
        sa.Column("transfer_manager_name", sa.String(256)),
        sa.Column("start_time", sa.DateTime, nullable=False),
        sa.Column("end_time", sa.DateTime),
        sa.Column("staging_path", sa.String(256)),
        sa.Column("store_path", sa.String(256)),
        sa.Column("transfer_data", sa.PickleType),
    )


def downgrade():
    op.drop_table(table_name)
