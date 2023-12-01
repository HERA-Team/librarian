"""
ORM for incoming and outgoing transfers.
"""


from .. import db

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


class IncomingTransfer(db.Model):
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

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    "The unique ID of this interaction. Can be used to look up the interaction by the client."
    status = db.Column(db.Enum(TransferStatus), nullable=False)
    "Current status of the transfer"
    uploader = db.Column(db.String(256), nullable=False)
    "The name of the uploader."
    transfer_size = db.Column(db.BigInteger, nullable=False)
    "The expected transfer size in bytes."
    
    store_id = db.Column(db.Integer)
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
    def new_transfer(self, uploader: str, transfer_size: int) -> "IncomingTransfer":
        """
        Create a new transfer!

        Transfers start out with a status of INITIATED.
        """

        return IncomingTransfer(
            status=TransferStatus.INITIATED,
            uploader=uploader,
            transfer_size=transfer_size,
            start_time=datetime.datetime.utcnow(),
        )

        return