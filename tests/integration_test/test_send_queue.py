"""
Tests for the send queue and associated checks.
"""

from pathlib import Path
from socket import gethostname

from hera_librarian.async_transfers import (
    CoreAsyncTransferManager,
    LocalAsyncTransferManager,
)
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


def test_create_simple_queue_item_and_send(
    test_server, test_orm, mocked_admin_client, server
):
    """
    Manually create a new queue item and see if it works when trying
    to send it.

    Delete this, everybody is asking you to delete this, please,
    people are crying
    """

    # Set up downstream librarian
    mocked_admin_client.add_librarian(
        name="live_server",
        url="http://localhost",
        authenticator="admin:password",  # This is the default authenticator.
        port=server.id,
    )

    SendQueue = test_orm.SendQueue

    get_session = test_server[1]

    with get_session() as session:
        queue_item = SendQueue.new_item(
            priority=100000000,
            destination="live_server",
            transfers=[],
            async_transfer_manager=NoCopyAsyncTransferManager(
                complete_transfer_status=TransferStatus.COMPLETED
            ),
        )

        session.add(queue_item)
        session.commit()

    # Now execute the faux-sends
    from librarian_background.queues import check_on_consumed, consume_queue_item

    consume_queue_item(session_maker=get_session)
    check_on_consumed(session_maker=get_session)

    with get_session() as session:
        available_queues = (
            session.query(SendQueue).filter_by(destination="live_server").all()
        )

        assert len(available_queues) > 0

        for x in available_queues:
            assert x.consumed
            assert x.completed

            session.delete(x)

        session.commit()

    # Clean up after ourselves.
    mocked_admin_client.remove_librarian(name="live_server")

    return


def test_simple_real_send(
    test_server_with_valid_file, test_orm, mocked_admin_client, server, tmp_path
):
    """
    Manually create a new queue item and see if it works when trying
    to send it.

    Delete this, everybody is asking you to delete this, please,
    people are crying
    """

    # Set up downstream librarian
    mocked_admin_client.add_librarian(
        name="live_server",
        url="http://localhost",
        authenticator="admin:password",  # This is the default authenticator.
        port=server.id,
    )

    SendQueue = test_orm.SendQueue
    File = test_orm.File
    OutgoingTransfer = test_orm.OutgoingTransfer

    get_session = test_server_with_valid_file[1]

    with get_session() as session:
        file = session.query(File).one()

        transfer = OutgoingTransfer.new_transfer(
            destination="liver_server",
            instance=file.instances[0],
            file=file,
        )

        transfer.source_path = str(file.instances[0].path)
        transfer.dest_path = str(tmp_path / file.name)

        queue_item = SendQueue.new_item(
            priority=100000000,
            destination="live_server",
            transfers=[transfer],
            async_transfer_manager=LocalAsyncTransferManager(hostnames=[gethostname()]),
        )

        session.add_all([transfer, queue_item])
        session.commit()

    # Now execute the faux-sends
    from librarian_background.queues import check_on_consumed, consume_queue_item

    consume_queue_item(session_maker=get_session)
    check_on_consumed(session_maker=get_session)

    with get_session() as session:
        available_queues = (
            session.query(SendQueue).filter_by(destination="live_server").all()
        )

        assert len(available_queues) > 0

        for x in available_queues:
            assert x.consumed
            assert x.completed

            for transfer in x.transfers:
                assert Path(transfer.dest_path).exists()

            session.delete(x)

        session.commit()

    # Clean up after ourselves.
    mocked_admin_client.remove_librarian(name="live_server")

    return
