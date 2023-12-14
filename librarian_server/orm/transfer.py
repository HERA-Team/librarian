"""
ORM for incoming and outgoing transfers.
"""


from .. import database as db

from enum import Enum
import datetime


class TransferStatus(Enum):
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
    transfer_size = db.Column(db.BigInteger, nullable=False)
    "The expected transfer size in bytes."
    transfer_checksum = db.Column(db.String(256), nullable=False)
    "The checksum of the transfer."
    
    store_id = db.Column(db.Integer, db.ForeignKey("store_metadata.id"), nullable=False)
    "The ID of the store that this interaction is with."
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

    @classmethod
    def new_transfer(self, uploader: str, transfer_size: int, transfer_checksum: str) -> "IncomingTransfer":
        """
        Create a new transfer!

        Transfers start out with a status of INITIATED.
        """

        return IncomingTransfer(
            status=TransferStatus.INITIATED,
            uploader=uploader,
            transfer_size=transfer_size,
            transfer_checksum=transfer_checksum,
            start_time=datetime.datetime.utcnow(),
        )


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

    instance_id = db.Column(db.Integer, db.ForeignKey("instances.id"), nullable=False)
    "The ID of the instance that this transfer is copying."
    instance = db.relationship("Instance", primaryjoin="Instance.id == OutgoingTransfer.instance_id")
    "The instance that is being copied."

    remote_store_id = db.Column(db.Integer, nullable=False)
    "The ID of the store that this interaction is going to."
    transfer_manager_name = db.Column(db.String(256))
    "Name of the transfer manager that the client is using/used to upload the file."
    
    transfer_data = db.Column(db.PickleType)
    "Serialized transfer manager data, likely from the transfer manager. For instance, this could include the Globus data."
