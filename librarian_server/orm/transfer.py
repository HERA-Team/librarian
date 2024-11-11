"""
ORM for incoming and outgoing transfers.
"""

import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from hera_librarian.models.checkin import CheckinUpdateRequest, CheckinUpdateResponse
from hera_librarian.models.clone import (
    CloneFailRequest,
    CloneFailResponse,
    CloneStagedRequest,
    CloneStagedResponse,
)
from hera_librarian.transfer import TransferStatus

from .. import database as db
from ..logger import log
from .librarian import Librarian

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from .file import File
    from .instance import Instance


class IncomingTransfer(db.Base):
    """
    An incoming transfer to this librarian. Created once an upload is initialized,
    and then is deleted once we have successfully moved the incoming file to the store
    location from its staging directory.

    The presence of this ORM in a database table with a status other than
    TransferStatus.COMPLETED indicates that there is unfinished business; a
    client has left us hanging, or maybe there is a background task that is yet
    to complete.
    """

    __tablename__ = "incoming_transfers"

    # NOTE: SQLite does not allow autoincrement PKs that are BigIntegers.
    id = db.Column(db.Integer, primary_key=True, autoincrement=True, unique=True)
    "The unique ID of this interaction. Can be used to look up the interaction by the client."
    status = db.Column(db.Enum(TransferStatus), nullable=False)
    "Current status of the transfer"
    uploader = db.Column(db.String(256), nullable=False)
    "The name of the uploader."
    upload_name = db.Column(db.String(256), nullable=False)
    "The name of the file that is being uploaded."
    source = db.Column(db.String(256), nullable=False)
    "The source of this file. Could be same as uploader, but could also be another librarian."
    transfer_size = db.Column(db.BigInteger, nullable=False)
    "The expected transfer size in bytes."
    transfer_checksum = db.Column(db.String(256), nullable=False)
    "The checksum of the transfer."

    store_id = db.Column(db.Integer, db.ForeignKey("store_metadata.id"), nullable=False)
    "The ID of the store that this interaction is with."
    store = db.relationship(
        "StoreMetadata", primaryjoin="IncomingTransfer.store_id == StoreMetadata.id"
    )
    "The store that this object is on or going to."
    transfer_manager_name = db.Column(db.String(256))
    "Name of the transfer manager that the client is using/used to upload the file."

    start_time = db.Column(db.DateTime, nullable=False)
    "The time at which this interaction was started."
    end_time = db.Column(db.DateTime)
    "The time at which this interaction was ended."

    staging_path = db.Column(db.String(256))
    "Staging path of file on store. Path relative to top of staging area."
    store_path = db.Column(db.String(256))
    "Store path of file on store. Its final resting place. Path relative to top of store."

    transfer_data = db.Column(db.PickleType)
    "Serialized transfer data, likely from the transfer manager. For instance, this could include the Globus data."

    source_transfer_id: int = db.Column(db.Integer)
    "The ID of the corresponding OutgoingTransfer on a remote system."

    @classmethod
    def new_transfer(
        self,
        uploader: str,
        upload_name: str,
        source: str,
        transfer_size: int,
        transfer_checksum: str,
    ) -> "IncomingTransfer":
        """
        Create a new transfer!

        Transfers start out with a status of INITIATED.
        """

        return IncomingTransfer(
            status=TransferStatus.INITIATED,
            uploader=uploader,
            upload_name=upload_name,
            source=source,
            transfer_size=transfer_size,
            transfer_checksum=transfer_checksum,
            start_time=datetime.datetime.now(datetime.timezone.utc),
        )

    def fail_transfer(self, session: "Session", commit: bool = True):
        """
        Fail the transfer and (optionally) commit to the database.
        """

        self.status = TransferStatus.FAILED
        self.end_time = datetime.datetime.now(datetime.timezone.utc)

        try:
            self.store.store_manager.unstage(Path(self.staging_path))
        except (FileNotFoundError, OSError, AttributeError):
            pass

        if commit:
            session.commit()

        if self.source_transfer_id is None:
            # No remote transfer ID, so we can't do anything.
            return

        # Now here's the interesting part - we need to communicate to the
        # remote librarian that the transfer failed!
        librarian: Librarian = (
            session.query(Librarian).filter_by(name=self.source).first()
        )

        if not librarian:
            # Librarian doesn't exist. We can't do anything.
            log.error(
                "Remote librarian does not exist when trying to fail transfer. "
                "This state should be entirely unreachable."
            )
            return

        client = librarian.client()

        request = CheckinUpdateRequest(
            source_transfer_ids=[self.source_transfer_id],
            destination_transfer_ids=[],
            new_status=TransferStatus.FAILED,
        )

        try:
            response: CheckinUpdateResponse = client.post(
                "checkin/update",
                request=request,
                response=CheckinUpdateResponse,
            )
        except Exception as e:
            log.error(
                f"Failed to communicate to remote librarian that transfer {self.id} "
                f"failed with exception {e}. It is likely that there is a stale transfer "
                f"on remote librarian {self.source} with id {self.source_transfer_id}."
            )

        return


class OutgoingTransfer(db.Base):
    """
    An outgoing transfer from this librarian. Created once an upload to another librarian
    is initiated.
    """

    __tablename__ = "outgoing_transfers"

    # NOTE: SQLite does not allow autoincrement PKs that are BigIntegers.
    id = db.Column(db.Integer, primary_key=True, autoincrement=True, unique=True)
    "The unique ID of this interaction. Can be used to look up the interaction by the client."
    status = db.Column(db.Enum(TransferStatus), nullable=False)
    "Current status of the transfer"
    destination = db.Column(db.String(256), nullable=False)
    "The name of the destination librarian."
    transfer_size = db.Column(db.BigInteger, nullable=False)
    "The expected transfer size in bytes."
    transfer_checksum = db.Column(db.String(256), nullable=False)
    "The checksum of the transfer."

    start_time = db.Column(db.DateTime, nullable=False)
    "The time at which this interaction was started."
    end_time = db.Column(db.DateTime)
    "The time at which this interaction was ended."

    file_name = db.Column(db.String(256), db.ForeignKey("files.name"), nullable=False)
    "The name of the file that is being uploaded."
    file = db.relationship(
        "File", primaryjoin="File.name == OutgoingTransfer.file_name"
    )
    "The file that is being uploaded."

    instance_id = db.Column(db.Integer, db.ForeignKey("instances.id"), nullable=False)
    "The ID of the instance that this transfer is copying."
    instance = db.relationship(
        "Instance", primaryjoin="Instance.id == OutgoingTransfer.instance_id"
    )
    "The instance that is being copied."

    remote_transfer_id = db.Column(db.Integer)
    "The ID of the corresponding IncomingTransfer on the remote librarian."
    transfer_manager_name = db.Column(db.String(256))
    "Name of the transfer manager that the client is using/used to upload the file."

    transfer_data = db.Column(db.PickleType)
    "Serialized transfer manager data, likely from the transfer manager. For instance, this could include the Globus data. In practice, though, most outbound transfers are done asynchronously and this is empty."

    send_queue_id = db.Column(db.Integer, db.ForeignKey("send_queue.id"))
    "The send queue ID for this transfer, if it has one."
    send_queue = db.relationship(
        "SendQueue", primaryjoin="OutgoingTransfer.send_queue_id == SendQueue.id"
    )
    "The send queue that we are using to transfer this OutgoingTransfer."

    source_path = db.Column(db.String(256))
    "The source path of the transfer. If doing a simple 'cp', this is the first argument."
    dest_path = db.Column(db.String(256))
    "The destination path of the transfer. If doing a simple 'cp', this is the second argument."

    @classmethod
    def new_transfer(
        self, destination: str, instance: "Instance", file: "File"
    ) -> "OutgoingTransfer":
        """
        Create a new transfer!

        Transfers start out with a status of INITIATED.
        """

        return OutgoingTransfer(
            status=TransferStatus.INITIATED,
            destination=destination,
            transfer_size=file.size,
            transfer_checksum=file.checksum,
            file_name=file.name,
            instance_id=instance.id,
            start_time=datetime.datetime.now(datetime.timezone.utc),
        )

    def fail_transfer(self, session: "Session", commit: bool = True):
        """
        Fail the transfer and (optionally) commit to the database.
        """

        self.status = TransferStatus.FAILED
        self.end_time = datetime.datetime.now(datetime.timezone.utc)

        if commit:
            session.commit()

        if self.remote_transfer_id is None:
            # No remote transfer ID, so we can't do anything.
            return

        # Now here's the interesting part - we need to communicate to the
        # remote librarian that the transfer failed!

        librarian: Librarian = (
            session.query(Librarian).filter_by(name=self.destination).first()
        )

        if not librarian:
            # Librarian doesn't exist. We can't do anything.
            log.error(
                "Remote librarian does not exist when trying to fail transfer. "
                "This state should be entirely unreachable."
            )
            return

        client = librarian.client()

        request = CloneFailRequest(
            source_transfer_id=self.id,
            destination_transfer_id=self.remote_transfer_id,
            reason="Transfer failed on source librarian.",
        )

        try:
            response: CloneFailResponse = client.post(
                "clone/fail",
                request=request,
                response=CloneFailResponse,
            )

            if not response.success:
                raise Exception(
                    "Remote librarian refused or failed to set transfer status to FAILED."
                )
        except Exception as e:
            log.error(
                f"Failed to communicate to remote librarian that transfer {self.id} "
                f"failed with exception {e}. It is likely that there is a stale transfer "
                f"on remote librarian {self.destination} with id {self.remote_transfer_id}."
            )

        return

    def staged_transfer(self, session: "Session", commit: bool = True):
        """
        Stage the transfer and (optionally) commit to the database.
        """

        self.status = TransferStatus.STAGED
        self.end_time = datetime.datetime.now(datetime.timezone.utc)

        if commit:
            session.commit()

        # Again, the interesting part - we need to communicate to the remote
        # librarian that the transfer made it, and ask it to check!

        librarian: Librarian = (
            session.query(Librarian).filter_by(name=self.destination).first()
        )

        if not librarian:
            log.error(
                "Remote librarian does not exist when trying to fail transfer. "
                "This state should be entirely unreachable."
            )

        client = librarian.client()

        request = CloneStagedRequest(
            source_transfer_id=self.id,
            destination_transfer_id=self.remote_transfer_id,
        )

        try:
            response: CloneStagedResponse = client.post(
                "clone/staged",
                request=request,
                response=CloneStagedResponse,
            )

            if not response.success:
                raise Exception(
                    "Remote librarian refused or failed to set transfer status to STAGED",
                )
        except Exception as e:
            log.error(
                f"Failed to communicate to remote librarian that transfer {self.id} "
                f"was staged with exception {e}. It is likely that there is a stale transfer "
                f"on remote librarian {self.destination} with id {self.remote_transfer_id}."
            )

        return


class CloneTransfer(db.Base):
    """
    A record of a clone transfer. This is a local transfer between two stores.
    """

    __tablename__ = "clone_transfers"

    # NOTE: SQLite does not allow autoincrement PKs that are BigIntegers.
    id = db.Column(db.Integer, primary_key=True, autoincrement=True, unique=True)
    "The unique ID of this interaction. Can be used to look up the interaction by the client."
    status = db.Column(db.Enum(TransferStatus), nullable=False)
    "Current status of the transfer"
    start_time = db.Column(db.DateTime, nullable=False)
    "The time at which this interaction was started."
    end_time = db.Column(db.DateTime)
    "The time at which this interaction was ended."

    source_store_id = db.Column(
        db.Integer, db.ForeignKey("store_metadata.id"), nullable=False
    )
    "The ID of the source store that this interaction is with."
    destination_store_id = db.Column(
        db.Integer, db.ForeignKey("store_metadata.id"), nullable=False
    )
    "The ID of the destination store that this interaction is with."

    transfer_manager_name = db.Column(db.String(256))
    "Name of the transfer manager that the client is using/used to upload the file."

    source_instance_id = db.Column(
        db.Integer, db.ForeignKey("instances.id"), nullable=False
    )
    "The ID of the instance that this transfer is copying."
    destination_instance_id = db.Column(db.Integer, db.ForeignKey("instances.id"))
    "The ID of the instance that this transfer is copying to."

    source_instance = db.relationship(
        "Instance", primaryjoin="Instance.id == CloneTransfer.source_instance_id"
    )
    "The instance that is being copied."
    destination_instance = db.relationship(
        "Instance", primaryjoin="Instance.id == CloneTransfer.destination_instance_id"
    )
    "The instance that is being copied to."

    @classmethod
    def new_transfer(
        self, source_store_id: int, destination_store_id: int, source_instance_id: int
    ) -> "CloneTransfer":
        """
        Create a new transfer!

        Transfers start out with a status of INITIATED.
        """

        return CloneTransfer(
            status=TransferStatus.INITIATED,
            source_store_id=source_store_id,
            destination_store_id=destination_store_id,
            source_instance_id=source_instance_id,
            start_time=datetime.datetime.now(datetime.timezone.utc),
        )

    def fail_transfer(self, session: "Session"):
        """
        Fail the transfer and commit to the database.
        """

        self.status = TransferStatus.FAILED
        self.end_time = datetime.datetime.now(datetime.timezone.utc)

        session.commit()

        return
