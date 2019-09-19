# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the MIT License.

"""Initial schema.

Revision ID: 71df5b41ae41
Revises:
Create Date: 2016-12-06 09:33:05.772135

"""
from alembic import op
import sqlalchemy as sa

revision = '71df5b41ae41'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # This is the schema used by the first Librarian deployments.
    op.create_table('observing_session',
                    sa.Column('id', sa.BigInteger(), nullable=False),
                    sa.Column('start_time_jd', sa.Float(precision=u'53'), nullable=False),
                    sa.Column('stop_time_jd', sa.Float(precision=u'53'), nullable=False),
                    sa.PrimaryKeyConstraint('id')
                    )

    op.create_table('standing_order',
                    sa.Column('id', sa.Integer(), nullable=False),
                    sa.Column('name', sa.String(length=64), nullable=False),
                    sa.Column('search', sa.Text(), nullable=False),
                    sa.Column('conn_name', sa.String(length=64), nullable=False),
                    sa.PrimaryKeyConstraint('id'),
                    sa.UniqueConstraint('name')
                    )

    op.create_table('store',
                    sa.Column('id', sa.BigInteger(), nullable=False),
                    sa.Column('name', sa.String(length=256), nullable=False),
                    sa.Column('ssh_host', sa.String(length=256), nullable=False),
                    sa.Column('path_prefix', sa.String(length=256), nullable=False),
                    sa.Column('http_prefix', sa.String(length=256), nullable=True),
                    sa.Column('available', sa.Boolean(), nullable=False),
                    sa.PrimaryKeyConstraint('id'),
                    sa.UniqueConstraint('name')
                    )

    op.create_table('observation',
                    sa.Column('obsid', sa.BigInteger(), nullable=False),
                    sa.Column('start_time_jd', sa.Float(precision=u'53'), nullable=False),
                    sa.Column('stop_time_jd', sa.Float(precision=u'53'), nullable=True),
                    sa.Column('start_lst_hr', sa.Float(precision=u'53'), nullable=True),
                    sa.Column('session_id', sa.BigInteger(), nullable=True),
                    sa.ForeignKeyConstraint(['session_id'], [u'observing_session.id'], ),
                    sa.PrimaryKeyConstraint('obsid')
                    )

    op.create_table('file',
                    sa.Column('name', sa.String(length=256), nullable=False),
                    sa.Column('type', sa.String(length=32), nullable=False),
                    sa.Column('create_time', sa.DateTime(), nullable=False),
                    sa.Column('obsid', sa.BigInteger(), nullable=False),
                    sa.Column('size', sa.BigInteger(), nullable=False),
                    sa.Column('md5', sa.String(length=32), nullable=False),
                    sa.Column('source', sa.String(length=64), nullable=False),
                    sa.ForeignKeyConstraint(['obsid'], [u'observation.obsid'], ),
                    sa.PrimaryKeyConstraint('name')
                    )

    op.create_table('file_event',
                    sa.Column('id', sa.BigInteger(), nullable=False),
                    sa.Column('name', sa.String(length=256), nullable=True),
                    sa.Column('time', sa.DateTime(), nullable=False),
                    sa.Column('type', sa.String(length=64), nullable=True),
                    sa.Column('payload', sa.Text(), nullable=True),
                    sa.ForeignKeyConstraint(['name'], [u'file.name'], ),
                    sa.PrimaryKeyConstraint('id')
                    )

    op.create_table('file_instance',
                    sa.Column('store', sa.BigInteger(), nullable=False),
                    sa.Column('parent_dirs', sa.String(length=128), nullable=False),
                    sa.Column('name', sa.String(length=256), nullable=False),
                    sa.ForeignKeyConstraint(['name'], [u'file.name'], ),
                    sa.ForeignKeyConstraint(['store'], [u'store.id'], ),
                    sa.PrimaryKeyConstraint('store', 'parent_dirs', 'name')
                    )


def downgrade():
    raise Exception('I refuse to drop all of the Librarian tables!')
    # op.drop_table('file_instance')
    # op.drop_table('file_event')
    # op.drop_table('file')
    # op.drop_table('observation')
    # op.drop_table('store')
    # op.drop_table('standing_order')
    # op.drop_table('observing_session')
