# Copyright 2016 the HERA Collaboration
# Licensed under the MIT License.

"""Add FileInstance.deletion_policy.

Revision ID: bc996bc8dc1c
Revises: 71df5b41ae41
Create Date: 2016-12-06 11:03:39.667168

"""
from alembic import op
import sqlalchemy as sa

revision = 'bc996bc8dc1c'
down_revision = '71df5b41ae41'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('file_instance',
                  sa.Column('deletion_policy', sa.Integer(),
                            nullable=False,
                            server_default='0',  # = DeletionPolicies.DISALLOWED
                            )
                  )


def downgrade():
    op.drop_column('file_instance', 'deletion_policy')
