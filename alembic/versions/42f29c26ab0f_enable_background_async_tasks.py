# Copyright 2017 the HERA Collaboration
# Licensed under the 2-clause BSD License.

"""enable background async tasks

Revision ID: 42f29c26ab0f
Revises: d8934c52bac5
Create Date: 2024-04-25 11:36:16.264574

"""
import sqlalchemy as sa

from alembic import op

revision = "42f29c26ab0f"
down_revision = "d8934c52bac5"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("store_metadata") as batch_op:
        batch_op.add_column(
            sa.Column("async_transfer_manager_data", sa.PickleType(), nullable=True)
        )

    op.create_table(
        "send_queue",
        sa.Column("id", sa.Integer(), nullable=False, primary_key=True),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("created_time", sa.DateTime(), nullable=False),
        sa.Column("retries", sa.Integer(), nullable=False),
        sa.Column("destination", sa.String(length=256), nullable=False),
        sa.Column("async_transfer_manager", sa.PickleType(), nullable=True),
        sa.Column("consumed", sa.Boolean(), default=False),
        sa.Column("consumed_time", sa.DateTime(), nullable=True),
        sa.Column("completed", sa.Boolean(), default=False),
        sa.Column("completed_time", sa.DateTime(), nullable=True),
    )


def downgrade():
    op.drop_table("send_queue")

    op.drop_column("store_metadata", "async_transfer_manager_data")
