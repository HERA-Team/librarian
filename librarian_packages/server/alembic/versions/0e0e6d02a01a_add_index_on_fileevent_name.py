# Copyright 2016 the HERA Collaboration
# Licensed under the MIT License.

"""Add index on FileEvent.name.

Revision ID: 0e0e6d02a01a
Revises: bc996bc8dc1c
Create Date: 2016-12-13 11:01:34.767814

"""
from alembic import op
import sqlalchemy as sa


revision = '0e0e6d02a01a'
down_revision = 'bc996bc8dc1c'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index('file_event_name', 'file_event', ['name'], unique=False)


def downgrade():
    op.drop_index('file_event_name', table_name='file_event')
