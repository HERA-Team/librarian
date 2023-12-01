"""
ORM for incoming and outgoing transfers.
"""


from .. import db

from enum import Enum


class TransferStatus(Enum):
    """
    The status of a transfer.
    """

    INITIATED = 0
    "Transfer has been initiated, but client has not yet started moving data"
    ONGOING = 1
    "Client is currently (asynchronously) moving data to us. This is not possible with all transfer managers."
    COMPLETED = 2
    "Transfer is completed"
    FAILED = 3
    "Transfer has been confirmed to have failed."
    CANCELLED = 4
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
    store_id = db.Column(db.Integer, nullable=False)
    "The ID of the store that this interaction is with."
    uploader = db.Column(db.String(256), nullable=False)
    "The name of the uploader."

    transfer_manager_name = db.Column(db.String(256))
    "Name of the transfer manager that the client is using/used to upload the file."

    start_time = db.Column(db.DateTime)
    "The time at which this interaction was started."
    end_time = db.Column(db.DateTime)
    "The time at which this interaction was ended."

    staging_path = db.Column(db.String(256))
    "Staging path of file on store. Path relative to top of staging area."
    store_path = db.Column(db.String(256))
    "Store path of file on store. Its final resting place. Path relative to top of store."

    transfer_data = db.Column(db.PickleType)
    "Serialized transfer data, likely from the transfer manager. For instance, this could include the Globus data."
