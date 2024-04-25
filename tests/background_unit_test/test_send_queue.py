"""
Tests for the send queue and associated checks.
"""

from hera_librarian.async_transfers import CoreAsyncTransferManager
from hera_librarian.transfer import TransferStatus


class NoCopyAsyncTransferManager(CoreAsyncTransferManager):
    complete_transfer_status: TransferStatus

    def batch_transfer(self, *args, **kwargs):
        return True

    def transfer(self, *args, **kwargs):
        return True

    @property
    def valid(self):
        return True

    @property
    def transfer_status(self):
        return self.complete_transfer_status


def test_create_queue_item(test_server, test_orm):
    """
    Manually create a new queue item and see if it works.

    Delete this, everybody is asking you to delete this, please,
    people are crying
    """

    SendQueue = test_orm.SendQueue

    get_session = test_server[1]

    with get_session() as session:
        queue_item = SendQueue.new_item(
            priority=100000000,
            destination="nowhere",
            transfers=[],
            async_transfer_manager=NoCopyAsyncTransferManager(
                complete_transfer_status=TransferStatus.COMPLETED
            ),
        )

        session.add(queue_item)

    with get_session() as session:
        for x in session.query(SendQueue).filter_by(destination="nowhere").all():
            session.delete(x)

    return
