"""
The ORM definitions for the priority queue used for transfering data to other
librarians and clients.

During the transfer process, the librarian server will either synchronously
(not using this functionality) or asynchronously (using this functionality)
request that a piece of data is transferred. In the former case, it is
simply copied and the server waits for the copy to complete. In the latter
scenario, the server places an item in this priority queue and a separate
thread handles the transfer.

There are a few reasons for this:

- Reducing load on the server and background task threads.
- Allowing for batching of transfers.
- Ability to track retries and failures.
"""

import datetime
from typing import TYPE_CHECKING

from loguru import logger
from sqlalchemy.orm import Session

from hera_librarian.async_transfers import CoreAsyncTransferManager
from hera_librarian.errors import ErrorCategory, ErrorSeverity
from hera_librarian.exceptions import LibrarianError
from hera_librarian.models.checkin import CheckinUpdateRequest, CheckinUpdateResponse
from hera_librarian.transfer import TransferStatus

from .. import database as db
from .librarian import Librarian

if TYPE_CHECKING:
    from .transfer import OutgoingTransfer


class SendQueue(db.Base):
    """
    The priority queue for sending data to other stores, librarians, and clients
    using a simple local copy.
    """

    __tablename__ = "send_queue"

    id = db.Column(db.Integer, primary_key=True)
    "The unique identifier for this item in the queue."
    priority = db.Column(db.Integer, nullable=False)
    "The priority of this item in the queue. Higher is higher priority."
    created_time: datetime.datetime = db.Column(db.DateTime, nullable=False)
    "The time that this item was added to the queue."
    transfers = db.relationship("OutgoingTransfer", back_populates="send_queue")
    "The transfers this item is associated with."
    retries = db.Column(db.Integer, nullable=False)
    "The number of times this item has been tried to be sent."

    destination = db.Column(db.String(256), nullable=False)
    "The name of the destination librarian."

    async_transfer_manager = db.Column(db.PickleType)
    "The transfer manager that this queue item will use."

    consumed: bool = db.Column(db.Boolean, default=False)
    "Whether this queue item has been consumed."
    consumed_time: datetime = db.Column(db.DateTime)
    "The time at which the item was consumed. Useful for pruning."

    completed: bool = db.Column(db.Boolean, default=False)
    "Whether this queue item has been entirely completed."
    completed_time: datetime = db.Column(db.DateTime)
    "The time at which the queue item was marked as completed."

    failed: bool = db.Column(db.Boolean, default=False)
    "Whether this queue item failed, and that is the reason for completed status."

    @classmethod
    def new_item(
        cls,
        priority: int,
        destination: str,
        transfers: list["OutgoingTransfer"],
        async_transfer_manager: CoreAsyncTransferManager,
    ) -> "SendQueue":
        """
        Create a new item in the queue.

        Parameters
        ----------
        priority : int
            The priority of this transfer. Older, and higher priority, transfers will be completed first.
            Higher numbers mean higher priority.
        destination : str
            The name of the destination librarian.
        transfers : list[OutgoingTransfer]
            The transfers that this queue item will complete.
        async_transfer_manager : CoreAsyncTransferManager
            The transfer manager to use.
        """

        item = cls(
            priority=priority,
            destination=destination,
            created_time=datetime.datetime.now(datetime.timezone.utc),
            transfers=transfers,
            retries=0,
            async_transfer_manager=async_transfer_manager,
        )

        return item

    def fail(self, session: Session):
        """
        Mark this queue item as failed. This will also try to call up
        the downstream librarian to fail their transfers too.

        Parameters
        ----------
        session : Session
            The database session to use.
        """

        # First, mark all of the transfers as failed (including calling up the
        # downstream librarian).
        for t in self.transfers:
            t.fail_transfer(session, commit=False)

        # Set our own state!
        self.failed = True
        self.completed = True
        self.completed_time = datetime.datetime.now(datetime.timezone.utc)

        session.commit()

        return

    def update_transfer_status(
        self,
        new_status: TransferStatus,
        session: Session,
    ) -> CheckinUpdateResponse:
        """
        Update the status of all of the linked transfers and their remote
        counterparts.

        Parameters
        ----------
        new_status : TransferStatus.ONGOING | TransferStatus.STAGED
            The updated transfer status. We can only update INITIATED -> ONGOING
            and ONGOING -> STAGED.

        Raises
        ------

        AttributeError:
            We cannot find the associated librarian in the database.
        LibrarianError:
            We cannot contact the downstream librarian.
        """

        librarian = (
            session.query(Librarian).filter_by(name=self.destination).one_or_none()
        )

        if librarian is None:
            raise AttributeError(
                f"Librarian {self.destination} cannot be found in the database."
            )

        client = librarian.client()

        destination_ids = [
            t.remote_transfer_id
            for t in self.transfers
            if t.remote_transfer_id is not None
        ]

        request = CheckinUpdateRequest(
            source_transfer_ids=[],
            destination_transfer_ids=destination_ids,
            new_status=new_status,
        )

        try:
            response: CheckinUpdateResponse = client.post(
                endpoint="checkin/update",
                request=request,
                response=CheckinUpdateResponse,
            )
        except Exception as e:
            # Oh no, we can't call up the librarian!
            logger.error(
                f"Unable to communicate with remote librarian for batch "
                f"status update, recieved response {e}."
            )

            raise LibrarianError(
                "Cannot contact the downstream librarian to update status: " f"{e}"
            )

        # Now that this contact was successful, change our own.
        for t in self.transfers:
            t.status = new_status

        session.commit()

        return response
