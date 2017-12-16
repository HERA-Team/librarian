# Copyright 2016-2017 the HERA Collaboration
# Licensed under the MIT License.

"""Make File obsids optional.

Revision ID: 464899566429
Revises: b02eb31402b1
Create Date: 2017-12-16 11:51:09.128207

"""
from alembic import op
import sqlalchemy as sa


revision = '464899566429'
down_revision = 'b02eb31402b1'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('file', 'obsid', existing_type=sa.BIGINT(), nullable=True)


def downgrade():
    # This probably won't work in practice since the rows with null obsids will need
    # to be deleted.
    op.alter_column('file', 'obsid', existing_type=sa.BIGINT(), nullable=False)
