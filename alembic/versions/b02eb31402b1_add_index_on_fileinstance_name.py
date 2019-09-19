# Copyright 2017 the HERA Collaboration
# Licensed under the MIT License.

"""add index on FileInstance.name

Revision ID: b02eb31402b1
Revises: 0e0e6d02a01a
Create Date: 2017-02-09 17:32:01.979450

"""
from alembic import op
import sqlalchemy as sa


revision = 'b02eb31402b1'
down_revision = '0e0e6d02a01a'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index('file_instance_name', 'file_instance', ['name'], unique=False)


def downgrade():
    op.drop_index('file_instance_name', table_name='file_instance')
