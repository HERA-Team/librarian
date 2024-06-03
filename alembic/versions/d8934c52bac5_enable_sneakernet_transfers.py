# Copyright 2017 the HERA Collaboration
# Licensed under the 2-clause BSD License.

"""enable sneakernet transfers

Revision ID: d8934c52bac5
Revises: 71df5b41ae41
Create Date: 2024-02-28 15:48:44.705721

"""
import sqlalchemy as sa
from sqlalchemy.orm import load_only
from sqlalchemy.orm.session import Session

from alembic import op
from librarian_server.orm import File, OutgoingTransfer, StoreMetadata

revision = "d8934c52bac5"
down_revision = "71df5b41ae41"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("store_metadata") as batch_op:
        batch_op.add_column(sa.Column("enabled", sa.Boolean, default=True))

    with op.batch_alter_table("incoming_transfers") as batch_op:
        batch_op.add_column(sa.Column("source_transfer_id", sa.Integer))

    with op.batch_alter_table("outgoing_transfers") as batch_op:
        batch_op.add_column(sa.Column("file_name", sa.String(256)))

        batch_op.create_foreign_key(
            "fk_outgoing_transfers_file_name_files", "files", ["file_name"], ["name"]
        )

    # Now perform data migration
    session = Session(bind=op.get_bind())

    for store in (
        session.query(StoreMetadata).options(load_only(StoreMetadata.enabled)).all()
    ):
        store.enabled = True
        session.commit()

    for transfer in (
        session.query(OutgoingTransfer)
        .options(load_only(OutgoingTransfer.file_name))
        .all()
    ):
        file = session.query(File).options(load_only(File.name)).get(transfer.file_name)
        transfer.file_name = file.name
        transfer.file = file
        session.commit()

    # Now mark the columns as not nullable
    with op.batch_alter_table("store_metadata") as batch_op:
        batch_op.alter_column("enabled", nullable=False)

    with op.batch_alter_table("outgoing_transfers") as batch_op:
        batch_op.alter_column("file_name", nullable=False)


def downgrade():
    op.drop_column("store_metadata", "enabled")
    op.drop_column("incoming_transfers", "source_transfer_id")
    op.drop_column("outgoing_transfers", "file_name")
