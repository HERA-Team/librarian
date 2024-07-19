"""
Host for store metadata and related tasks.

Includes the StoreMetadata class, which is a database model.
"""

import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, reconstructor

from hera_librarian.async_transfers import (
    CoreAsyncTransferManager,
    async_transfer_manager_from_name,
)
from hera_librarian.deletion import DeletionPolicy
from hera_librarian.models.uploads import UploadCompletionRequest
from hera_librarian.transfers import CoreTransferManager, transfer_manager_from_name

from .. import database as db
from ..stores import CoreStore, Stores
from .file import File
from .instance import Instance
from .transfer import IncomingTransfer, TransferStatus


class StoreMetadata(db.Base):
    """
    A store is an abstracted concept of a storage location for files.

    It is made up of two key components: a staging area, and a store area.

    The staging area is where files are initially placed when they are uploaded.
    This is done to ensure that the file is fully uploaded before it is
    committed to the store area.

    Stores are defined in hera_librarian.stores.*, and are of many types, taking
    values in the enum defined therein.
    """

    # The store represented by this metadata.
    # store_manager: CoreStore
    # The transfer managers that can be used by this store.
    # transfer_managers: dict[str, CoreTransferManager]
    # The async transfer managers that can be used by this store. These are the ones
    # known to this librarian about itself, which are sent to other librarians.
    # async_transfer_managers: dict[str, AsyncCoreTransferManager]

    __tablename__ = "store_metadata"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    "Unique ID of this store."
    name = db.Column(db.String(256), nullable=False, unique=True)
    "The name of this store (as defined in the parameter file)."
    ingestable = db.Column(db.Boolean, nullable=False, default=True)
    "Whether this store accepts ingest requests or is just for cloning other stores."
    enabled = db.Column(db.Boolean, nullable=False, default=True)
    "Whether this store is enabled or not. If not, it will not be used for any operations."
    store_type = db.Column(db.Integer, nullable=False)
    "The type of this store. Indexes into hera_librarain.stores.Stores."
    store_data = db.Column(db.PickleType)
    "The data required for this store."
    transfer_manager_data = db.Column(db.PickleType)
    "The transfer managers that are valid for this store. Used for e.g. Clone transfers and Uploads"
    async_transfer_manager_data = db.Column(db.PickleType)
    "The async transfer managers that are valid for this store. User for inter-librarian transfers."

    def __init__(
        self,
        name: str,
        ingestable: bool,
        store_type: int,
        store_data: dict,
        transfer_manager_data: dict[str, dict],
        async_transfer_manager_data: dict[str, dict] | None = None,
    ):
        super().__init__()

        self.name = name
        self.ingestable = ingestable
        self.store_type = store_type
        self.store_data = store_data
        self.transfer_manager_data = transfer_manager_data

        if async_transfer_manager_data is None:
            async_transfer_manager_data = {}

        self.async_transfer_manager_data = async_transfer_manager_data

        self.__init_on_load__()

    @reconstructor
    def __init_on_load__(self):
        # Because the ORM calls __new__ not __init__, we need to do this
        # business of creating the objects from database pickles here.
        self.store_manager: CoreStore = Stores[self.store_type](**self.store_data)

        self.transfer_managers: dict[str, CoreTransferManager] = {
            name: transfer_manager_from_name(name)(**data)
            for name, data in self.transfer_manager_data.items()
            if data.get("available", False)
        }

        if self.async_transfer_manager_data is not None:
            self.async_transfer_managers: dict[str, CoreAsyncTransferManager] = {
                name: async_transfer_manager_from_name(name)(**data)
                for name, data in self.async_transfer_manager_data.items()
                if data.get("available", False)
            }

    def ingest_staged_file(
        self,
        transfer: IncomingTransfer,
        session: "Session",
        deletion_policy: DeletionPolicy = DeletionPolicy.DISALLOWED,
    ) -> Instance:
        """
        Ingests a file into the store. Creates a new File and associated file Instance.

        Parameters
        ----------
        request : UploadCompletionRequest
            The request object containing information about the file upload.
        session : Session
            The database session to use.
        deletion_policy : DeletionPolicy
            The deletion policy to use for this file.

        Returns
        -------
        Instance
            The created file instance.

        Raises
        ------
        FileExistsError
            If the file already exists on the store.
        ValueError
            If the file does not match the expected size or checksum.
        SQLAlchemyError
            If there was a problem committing the file to the database.
        """

        # We do not have any custom metadata any more. So MetaMode is no longer required...

        if not self.enabled:
            raise ValueError(f"Store {self.name} is not enabled.")

        # Do not trust the second request; get our original information from the
        # database. Could validate against the request?
        upload_name = transfer.upload_name
        staging_directory = self.store_manager.resolve_path_staging(
            transfer.staging_path
        )
        staged_path = staging_directory / upload_name
        store_path = self.store_manager.resolve_path_store(transfer.store_path)

        # First up, check that we got what we expected!
        try:
            info = self.store_manager.path_info(staged_path)
        except FileNotFoundError:
            transfer.status = TransferStatus.FAILED
            session.commit()

            raise FileNotFoundError(
                f"File {staged_path} not found in staging area. "
                "It is likely there was a problem with the file upload. "
            )
        if (
            info.size != transfer.transfer_size
            or info.md5 != transfer.transfer_checksum
        ):
            # We have a problem! The file is not what we expected. Delete it quickly!
            self.store_manager.unstage(staging_directory)

            transfer.status = TransferStatus.FAILED
            session.commit()

            raise ValueError(
                f"File {staged_path} does not match expected size/checksum; "
                f"expected {transfer.transfer_size}/{transfer.transfer_checksum}, "
                f"got {info.size}/{info.md5}."
            )

        # If we got here, we got what we expected. Let's try to commit the file to the store.
        try:
            resolved_store_path = self.store_manager.store(store_path)
        except FileExistsError:
            # We have a problem! The file already exists on the store, or the namespace
            # is reserved.
            self.store_manager.unstage(staging_directory)

            transfer.status = TransferStatus.FAILED
            session.commit()

            raise FileExistsError(f"File {store_path} already exists on store.")

        # Clean up the database!
        transfer.status = TransferStatus.COMPLETED
        transfer.end_time = datetime.datetime.now()

        # Now create the File in the database.
        file = File.new_file(
            filename=transfer.store_path,
            size=transfer.transfer_size,
            checksum=transfer.transfer_checksum,
            uploader=transfer.uploader,
            source=transfer.source,
        )

        # And the file instance associated with this.
        instance = Instance.new_instance(
            path=resolved_store_path,
            file=file,
            store=self,
            deletion_policy=deletion_policy,
        )

        session.add(file)
        session.add(instance)

        # Commit our change to the transfer, file, and instance simultaneously.

        try:
            session.commit()

            # We're good to go and move the file to where it needs to be.
            self.store_manager.commit(
                staging_path=staged_path, store_path=resolved_store_path
            )
            self.store_manager.unstage(staging_directory)
        except SQLAlchemyError as e:
            # Need to rollback everything. The upload failed...
            self.store_manager.unstage(staging_directory)

            session.rollback()

            transfer.status = TransferStatus.FAILED
            session.commit()

        return instance

    def __repr__(self) -> str:
        return (
            f"<StoreMetadata {self.name} (type: {self.store_type}) "
            f"(id: {self.id}) (ingestable: {self.ingestable})>"
        )
