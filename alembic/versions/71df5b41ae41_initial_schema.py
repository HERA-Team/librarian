# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the 2-clause BSD License.

"""Initial schema.

Revision ID: 71df5b41ae41
Revises:
Create Date: 2016-12-06 09:33:05.772135

"""
revision = "71df5b41ae41"
down_revision = None
branch_labels = None
depends_on = None

from alembic import op
from sqlalchemy import (
    Column,
    DateTime,
    BigInteger,
    String,
    Integer,
    PrimaryKeyConstraint,
    ForeignKey,
    Enum,
    PickleType,
    Boolean,
)

import enum


class DeletionPolicy(enum.Enum):
    """
    Enumeration for whether or not a file can be deleted from a store.

    Always defaults to 'DISALLOWED' when parsing.
    """

    DISALLOWED = 0
    ALLOWED = 1


class TransferStatus(enum.Enum):
    """
    The status of a transfer.
    """

    INITIATED = 0
    "Transfer has been initiated, but client has not yet started moving data"
    ONGOING = 1
    "Client is currently (asynchronously) moving data to us. This is not possible with all transfer managers."
    STAGED = 2
    "Transfer has been staged, server is ready to complete the transfer."
    COMPLETED = 3
    "Transfer is completed"
    FAILED = 4
    "Transfer has been confirmed to have failed."
    CANCELLED = 5
    "Transfer has been cancelled by the client."


def upgrade():
    op.create_table(
        "files",
        Column("name", String(256), primary_key=True, unique=True, nullable=False),
        Column("create_time", DateTime, nullable=False),
        Column("size", BigInteger, nullable=False),
        Column("checksum", String(256), nullable=False),
        Column("uploader", String(256), nullable=False),
        Column("source", String(256), nullable=False),
        PrimaryKeyConstraint("name"),
    )

    op.create_table(
        "store_metadata",
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(256), nullable=False, unique=True),
        Column("ingestable", Boolean, nullable=False, default=True),
        Column("store_type", Integer, nullable=False),
        Column("store_data", PickleType),
        Column("transfer_manager_data", PickleType),
    )

    op.create_table(
        "instances",
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=True,
            unique=True,
            nullable=False,
        ),
        Column("path", String(256), nullable=False),
        Column("deletion_policy", Enum(DeletionPolicy), nullable=False),
        Column("created_time", DateTime, nullable=False),
        Column("file_name", String(256), ForeignKey("files.name")),
        Column("store_id", Integer, ForeignKey("store_metadata.id")),
    )

    op.create_table(
        "incoming_transfers",
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=True,
            unique=True,
            nullable=False,
        ),
        Column("status", Enum(TransferStatus), nullable=False),
        Column("uploader", String(256), nullable=False),
        Column("transfer_size", BigInteger, nullable=False),
        Column("transfer_checksum", String(256), nullable=False),
        Column("store_id", Integer, ForeignKey("store_metadata.id")),
        Column("transfer_manager_name", String(256)),
        Column("start_time", DateTime, nullable=False),
        Column("end_time", DateTime),
        Column("staging_path", String(256)),
        Column("store_path", String(256)),
        Column("transfer_data", PickleType),
    )

    op.create_table(
        "outgoing_transfers",
        Column(
            "id",
            Integer(),
            primary_key=True,
            autoincrement=True,
            unique=True,
            nullable=False,
        ),
        Column("status", Enum(TransferStatus), nullable=False),
        Column("destination", String(256), nullable=False),
        Column("transfer_size", BigInteger, nullable=False),
        Column("transfer_checksum", String(256), nullable=False),
        Column("start_time", DateTime, nullable=False),
        Column("end_time", DateTime),
        Column("instance_id", Integer, ForeignKey("instances.id"), nullable=False),
        Column("remote_store_id", Integer, nullable=False),
        Column("transfer_manager_name", String(256)),
        Column("transfer_data", PickleType),
    )

    op.create_table(
        "clone_transfers",
        Column(
            "id",
            Integer,
            primary_key=True,
            autoincrement=True,
            unique=True,
            nullable=False,
        ),
        Column("status", Enum(TransferStatus), nullable=False),
        Column("start_time", DateTime, nullable=False),
        Column("end_time", DateTime),
        Column(
            "source_store_id", Integer, ForeignKey("store_metadata.id"), nullable=False
        ),
        Column(
            "destination_store_id",
            Integer,
            ForeignKey("store_metadata.id"),
            nullable=False,
        ),
        Column("transfer_manager_name", String(256)),
        Column(
            "source_instance_id", Integer, ForeignKey("instances.id"), nullable=False
        ),
        Column("destination_instance_id", Integer, ForeignKey("instances.id")),
    )


def downgrade():
    op.drop_table("incoming_transfers")
    op.drop_table("instances")
    op.drop_table("files")
    op.drop_table("store_metadata")
