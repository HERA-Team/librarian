"""
Host for store metadata and related tasks.

Includes the StoreMetadata class, which is a database model.
"""


from .. import database as db

from ..stores import Stores, CoreStore
from hera_librarian.transfers import CoreTransferManager, transfer_manager_from_name
from hera_librarian.models.uploads import UploadCompletionRequest

from ..webutil import ServerError
from ..deletion import DeletionPolicy

from enum import Enum
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import reconstructor
from sqlalchemy.exc import SQLAlchemyError

from .file import File
from .instance import Instance
from .transfer import TransferStatus, IncomingTransfer

import datetime


class MetaMode(Enum):
    """
    Metadata inference mode.
    """

    INFER = 0
    DIRECT = 1

    @classmethod
    def from_str(cls, string: str) -> "MetaMode":
        if string.lower() == "infer":
            return cls.INFER
        elif string.lower() == "direct":
            return cls.DIRECT
        else:
            raise ValueError(f"Invalid MetaMode string {string}.")


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

    __tablename__ = "store_metadata"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    "Unique ID of this store."
    name = db.Column(db.String(256), nullable=False, unique=True)
    "The name of this store (as defined in the parameter file)."
    ingestable = db.Column(db.Boolean, nullable=False, default=True)
    "Whether this store accepts ingest requests or is just for cloning other stores."
    store_type = db.Column(db.Integer, nullable=False)
    "The type of this store. Indexes into hera_librarain.stores.Stores."
    store_data = db.Column(db.PickleType)
    "The data required for this store."
    transfer_manager_data = db.Column(db.PickleType)
    "The transfer managers that are valid for this store."

    def __init__(
        self,
        name: str,
        ingestable: bool,
        store_type: int,
        store_data: dict,
        transfer_manager_data: dict[str, dict],
    ):
        super().__init__()

        self.name = name
        self.ingestable = ingestable
        self.store_type = store_type
        self.store_data = store_data
        self.transfer_manager_data = transfer_manager_data

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

    def ingest_staged_file(
        self,
        request: UploadCompletionRequest,
        transfer: IncomingTransfer,
    ) -> Instance:
        """
        Ingests a file into the store. Creates a new File and associated file Instance.

        Parameters
        ----------
        request : UploadCompletionRequest
            The request object containing information about the file upload.
        transfer : IncomingTransfer
            The transfer object containing information about the file transfer.

        Returns
        -------
        Instance
            The created file instance.

        Raises
        ------
        ServerError
            If the file does not match the expected size or checksum, or if the file already exists on the store.
        """

        # We do not have any custom metadata any more. So MetaMode is no longer required...

        staged_path = request.staging_location
        store_path = request.destination_location
        deletion_policy = DeletionPolicy.from_str(request.deletion_policy)
        uploader = request.uploader

        # First up, check that we got what we expected!
        info = self.store_manager.path_info(staged_path)

        if (
            info.size != transfer.transfer_size
            or info.md5 != transfer.transfer_checksum
        ):
            # We have a problem! The file is not what we expected. Delete it quickly!
            self.store_manager.unstage(staged_path)

            transfer.status = TransferStatus.FAILED
            db.session.commit()

            raise ServerError(
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
            self.store_manager.unstage(staged_path)

            transfer.status = TransferStatus.FAILED
            db.session.commit()

            raise FileExistsError(f"File {store_path} already exists on store.")

        # Clean up the database!
        transfer.status = TransferStatus.COMPLETED
        transfer.end_time = datetime.datetime.now()

        # Now create the File in the database.
        file = File.new_file(
            filename=request.destination_location,
            size=transfer.transfer_size,
            checksum=transfer.transfer_checksum,
            uploader=transfer.uploader,
            source=transfer.uploader,
        )

        # And the file instance associated with this.

        instance = Instance.new_instance(
            path=resolved_store_path,
            file=file,
            store=self,
            deletion_policy=deletion_policy,
        )

        db.session.add(file)
        db.session.add(instance)

        # Commit our change to the transfer, file, and instance simultaneously.

        try:
            db.session.commit()

            # We're good to go and move the file to where it needs to be.
            self.store_manager.commit(
                staging_path=staged_path, store_path=resolved_store_path
            )
            self.store_manager.unstage(request.staging_name)
        except SQLAlchemyError as e:
            # Need to rollback everything. The upload failed...
            self.store_manager.unstage(request.staging_name)

            db.session.rollback()

            try:
                transfer.status = TransferStatus.FAILED
                db.session.commit()
            except SQLAlchemyError as e:
                # We can't even set the transfer status... We are in big trouble!
                raise ServerError(
                    "Unhandled database exception when rolling back failed upload: ", e
                )

        return instance

    @classmethod
    def from_name(cls, name) -> "StoreMetadata":
        stores = db.query(cls, name=name).all()

        if len(stores) == 0:
            raise ServerError(f"Store {name} does not exist")
        elif len(stores) > 1:
            raise ServerError(f"Multiple stores with name {name} exist")

        return stores[0]

    @classmethod
    def from_id(cls, id) -> "StoreMetadata":
        stores = db.query(cls, id=id).all()

        if len(stores) == 0:
            raise ServerError(f"Store with ID {id} does not exist")
        elif len(stores) > 1:
            raise ServerError(f"Multiple stores with ID {id} exist")

        return stores[0]

    def __repr__(self) -> str:
        return (
            f"<StoreMetadata {self.name} (type: {self.store_type}) "
            f"(id: {self.id}) (ingestable: {self.ingestable})>"
        )
