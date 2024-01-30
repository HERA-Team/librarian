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

import enum

from sqlalchemy import (BigInteger, Boolean, Column, DateTime, Enum,
                        ForeignKey, Integer, PickleType, PrimaryKeyConstraint,
                        String)

from alembic import op
from hera_librarian.deletion import DeletionPolicy
from hera_librarian.errors import ErrorCategory, ErrorSeverity
from hera_librarian.transfer import TransferStatus


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
        Column("available", Boolean, nullable=False),
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
        Column("upload_name", String(256), nullable=False),
        Column("source", String(256), nullable=False),
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
        Column("remote_transfer_id", Integer),
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

    op.create_table(
        "remote_instances",
        Column("id", Integer(), primary_key=True, autoincrement=True, unique=True),
        Column("file_name", String(256), ForeignKey("files.name"), nullable=False),
        Column("store_id", Integer(), nullable=False),
        Column("librarian_id", Integer(), ForeignKey("librarians.id"), nullable=False),
        Column("copy_time", DateTime(), nullable=False),
        Column("sender", String(256), nullable=False),
    )

    op.create_table(
        "librarians",
        Column("id", Integer(), primary_key=True, autoincrement=True),
        Column("name", String(256), nullable=False, unique=True),
        Column("url", String(256), nullable=False),
        Column("port", Integer(), nullable=False),
        # Securely store authenticator using a password hashing function
        Column("authenticator", String(256), nullable=False),
        Column("last_seen", DateTime(), nullable=False),
        Column("last_heard", DateTime(), nullable=False),
    )

    op.create_table(
        "errors",
        Column("id", Integer(), primary_key=True, autoincrement=True, unique=True),
        Column("severity", Enum(ErrorSeverity), nullable=False),
        Column("category", Enum(ErrorCategory), nullable=False),
        Column("message", String, nullable=False),
        Column("raised_time", DateTime(), nullable=False),
        Column("cleared_time", DateTime()),
        Column("cleared", Boolean(), nullable=False),
        Column("caller", String(256)),
    )


def downgrade():
    op.drop_table("incoming_transfers")
    op.drop_table("instances")
    op.drop_table("files")
    op.drop_table("store_metadata")
