"""
Tests for the send queue and associated checks.
"""

from datetime import datetime, timedelta, timezone
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

    def valid(self, *args, **kwargs):
        return True

    def transfer_status(self, *args, **kwargs):
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
    check_on_consumed(
        session_maker=get_session,
        timeout_after=datetime.now(timezone.utc) + timedelta(days=7),
    )

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
        # File provided by fixture. If you change that filename this test will fail.
        file = session.get(File, "example_file.txt")

        transfer = OutgoingTransfer.new_transfer(
            destination="live_server",
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
    check_on_consumed(
        session_maker=get_session,
        timeout_after=datetime.now(timezone.utc) + timedelta(days=7),
    )

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
                session.delete(transfer)

            session.delete(x)

        session.commit()

    # Clean up after ourselves.
    mocked_admin_client.remove_librarian(name="live_server")

    return


def test_send_from_existing_file_row(
    test_server_with_many_files_and_errors,
    test_orm,
    mocked_admin_client,
    server,
    admin_client,
    librarian_database_session_maker,
    tmp_path,
):
    # This more complex test actually fully simulates an interaction
    # with the second 'live' server, in a similar way to the sneaker net
    # test.

    # Before starting, register the downstream and upstream librarians.
    assert mocked_admin_client.add_librarian(
        name="live_server",
        url="http://localhost",
        authenticator="admin:password",  # This is the default authenticator.
        port=server.id,
    )

    source_session_maker = test_server_with_many_files_and_errors[1]

    from librarian_background.queues import CheckConsumedQueue, ConsumeQueue
    from librarian_background.send_clone import SendClone

    # First things first - remove the instances from one file, and mark one file
    # as having no available instances.
    with source_session_maker() as session:
        files = session.query(test_orm.File).limit(2)

        for instance in files[0].instances:
            instance.delete(session=session, commit=False)

        for instance in files[1].instances:
            instance.available = False

        session.commit()

    # Execute the send tasks.
    generate_task = SendClone(
        name="generate_queues",
        destination_librarian="live_server",
        age_in_days=7,
        store_preference="local_store",
        # Need to set this to be big to make sure we send _everything_ for ease of testing
        # there are 128 fake files created but there may be more in the test server...
        send_batch_size=512,
    )

    with source_session_maker() as session:
        generate_task.core(session=session)

    # Now there _should_ be a pending task queue item. Let's check up on it,
    # and if we succeed, send it off.

    with source_session_maker() as session:
        assert (
            len(
                session.query(test_orm.SendQueue)
                .filter_by(destination="live_server", completed=False)
                .all()
            )
            > 0
        )

    # Now we try the actual send.
    consume_queue = ConsumeQueue(name="queue_consumer")
    consume_queue.core(session_maker=source_session_maker)

    # Check the queue item to see if it was successfuly consumed.
    # Also check on the associated transfers.

    with source_session_maker() as session:
        queue_item = (
            session.query(test_orm.SendQueue)
            .filter_by(destination="live_server", completed=False)
            .first()
        )

        assert queue_item.consumed

        for transfer in queue_item.transfers:
            assert transfer.status == TransferStatus.ONGOING

            assert Path(transfer.dest_path).exists()

    check_on_consumed = CheckConsumedQueue(name="check_on_consumed")
    check_on_consumed.core(session_maker=source_session_maker)

    with source_session_maker() as session:
        queue_item = (
            session.query(test_orm.SendQueue)
            .filter_by(destination="live_server", completed=True)
            .first()
        )

        assert queue_item.consumed
        assert queue_item.completed

        for transfer in queue_item.transfers:
            assert transfer.status == TransferStatus.STAGED

    # Check on the downstream that they actually got there...
    with librarian_database_session_maker() as session:
        incoming_transfers = (
            session.query(test_orm.IncomingTransfer)
            .filter_by(source="test_server")
            .all()
        )

        for transfer in incoming_transfers:
            assert transfer.status == TransferStatus.STAGED

    # Force downstream to execute their background tasks.
    from librarian_background.recieve_clone import RecieveClone

    task = RecieveClone(
        name="recv_clone",
    )

    with librarian_database_session_maker() as session:
        task.core(session=session)

    # Now check the downstream librarian that it got all those files!
    with source_session_maker() as session:
        sent_filenames = [
            tf.file_name for tf in session.query(test_orm.OutgoingTransfer).all()
        ]

    missing_files = []
    copied_files = []
    with librarian_database_session_maker() as session:
        for file_name in sent_filenames:
            if session.get(test_orm.File, file_name) is None:
                missing_files.append(file_name)
            else:
                copied_files.append(file_name)

        # Check that all the transfers we intiaited are now set to
        # COMPLETE
        incoming_transfers = (
            session.query(test_orm.IncomingTransfer)
            .filter_by(source="test_server")
            .all()
        )

        for transfer in incoming_transfers:
            # They should have been deleted
            assert not Path(transfer.staging_path).exists()
            assert transfer.status == TransferStatus.COMPLETED

    if missing_files != []:
        raise ValueError(f"Missing files: " + str(missing_files))
    else:
        print("All files copied successfully.")

    # Callback can't work, so we need to modify our outgoing transfers
    # to be compelted.
    with source_session_maker() as session:
        queue_item = (
            session.query(test_orm.SendQueue)
            .filter_by(destination="live_server", completed=True)
            .first()
        )

        assert queue_item.consumed
        assert queue_item.completed

        for transfer in queue_item.transfers:
            transfer.status = TransferStatus.COMPLETED

        session.commit()

    # Ok, now try to execute the send loop again. We should 409 and
    # register a new remote instance internally.
    with source_session_maker() as session:
        generate_task.core(session=session)

    # Check we correctly registered remote instances on the source.
    # We should correctly register all files.
    with source_session_maker() as session:
        for file_name in copied_files:
            file = session.get(test_orm.File, file_name)
            if len(file.remote_instances) == 0:
                raise FileNotFoundError

    # We can now use the validation endpoint to check the integrity
    # of the files.
    instance_validations = mocked_admin_client.validate_file(file_name=file_name)

    # Should have _ours_ and _theirs_.
    assert len(instance_validations) == 2

    source_librarians_for_validations = {x.librarian for x in instance_validations}

    assert len(source_librarians_for_validations) == 2  # I.e. they are different

    # Now we can check the checksums.
    checksums_from_validations = {x.current_checksum for x in instance_validations}
    assert len(checksums_from_validations) == 1  # Same file

    # Remove the librarians we added.
    assert mocked_admin_client.remove_librarian(name="live_server")

    # Remove all the files we just copied over!
    with librarian_database_session_maker() as session:
        for file_name in copied_files:
            file = session.get(test_orm.File, file_name)

            file.delete(session=session, commit=False, force=True)

        session.commit()


def test_use_batch_to_call_librarian(
    test_server_with_many_files_and_errors,
    test_orm,
):
    # A specific test to check the scenario where we can't call up a
    # downstream librarian because it's missing.

    # Make a bunch of outgoing transfers.
    session_maker = test_server_with_many_files_and_errors[1]

    from hera_librarian import LibrarianClient

    fake_client = LibrarianClient(
        host="http://localhost",
        port=test_server_with_many_files_and_errors[2].id,
        user="404",
        password="notfound",
    )

    from librarian_background.send_clone import use_batch_to_call_librarian

    with session_maker() as session:
        transfers = []
        outgoing_information = []
        for file in session.query(test_orm.File).limit(32):
            if len(file.instances) == 0:
                continue

            transfers.append(
                test_orm.OutgoingTransfer.new_transfer(
                    destination="nowhere",
                    instance=file.instances[0],
                    file=file,
                )
            )
            outgoing_information.append(
                {
                    "upload_size": file.size,
                    "upload_checksum": file.checksum,
                    "upload_name": file.name,
                    "destination_location": file.name,
                    "uploader": file.uploader,
                }
            )

        session.add_all(transfers)
        session.commit()

        assert (
            use_batch_to_call_librarian(
                outgoing_transfers=transfers,
                outgoing_information=outgoing_information,
                client=fake_client,
                librarian=None,
                session=session,
            )
            is False
        )

        for transfer in transfers:
            assert transfer.status == TransferStatus.FAILED

    return


def test_create_send_queue_item_no_transfer_providers(
    test_server_with_many_files_and_errors,
    test_orm,
):
    # This tests the failure cases of calling up a destination
    # and setting the status as ongoing.

    # Make a bunch of outgoing transfers.
    session_maker = test_server_with_many_files_and_errors[1]

    from hera_librarian import LibrarianClient

    fake_client = LibrarianClient(
        host="http://localhost",
        port=test_server_with_many_files_and_errors[2].id,
        user="404",
        password="notfound",
    )

    from librarian_background.send_clone import use_batch_to_call_librarian

    with session_maker() as session:
        transfers = []
        outgoing_information = []
        for file in session.query(test_orm.File).limit(32):
            if len(file.instances) == 0:
                continue

            transfers.append(
                test_orm.OutgoingTransfer.new_transfer(
                    destination="nowhere",
                    instance=file.instances[0],
                    file=file,
                )
            )
            outgoing_information.append(
                {
                    "upload_size": file.size,
                    "upload_checksum": file.checksum,
                    "upload_name": file.name,
                    "destination_location": file.name,
                    "uploader": file.uploader,
                }
            )

        session.add_all(transfers)
        session.commit()

        # We can now mock up the response from the librarian
        from hera_librarian.models.clone import (
            CloneBatchInitiationResponse,
            CloneBatchInitiationResponseFileItem,
        )

        # But make sure we omit one...
        items = []
        for transfer in transfers[:-1]:
            items.append(
                CloneBatchInitiationResponseFileItem(
                    staging_name=transfer.file_name,
                    staging_location=f"nowhere/{transfer.file_name}",
                    upload_name=transfer.file_name,
                    destination_location=f"nowhere/{transfer.file_name}",
                    source_transfer_id=transfer.id,
                    destination_transfer_id=transfer.id,
                )
            )

        fake_response = CloneBatchInitiationResponse(
            available_bytes_on_store=10000000000,
            store_name="fake_store",
            uploads=items,
            async_transfer_providers={},
        )

        from librarian_background.send_clone import create_send_queue_item

        test, _, _ = create_send_queue_item(
            response=fake_response,
            outgoing_transfers=transfers,
            librarian=test_orm.Librarian(
                name="non_existent",
                url=fake_client.host,
                port=fake_client.port,
                authenticator="none:none",
                last_seen=datetime.now(timezone.utc),
                last_heard=datetime.now(timezone.utc),
            ),
            session=session,
        )

        assert test == False

    return


def test_create_send_queue_item_no_availability_of_transfer_manager(
    test_server_with_many_files_and_errors,
    test_orm,
):
    # This tests the failure cases of calling up a destination
    # and setting the status as ongoing.

    # Make a bunch of outgoing transfers.
    session_maker = test_server_with_many_files_and_errors[1]

    from hera_librarian import LibrarianClient

    fake_client = LibrarianClient(
        host="http://localhost",
        port=test_server_with_many_files_and_errors[2].id,
        user="404",
        password="notfound",
    )

    from librarian_background.send_clone import use_batch_to_call_librarian

    with session_maker() as session:
        transfers = []
        outgoing_information = []
        for file in session.query(test_orm.File).limit(32):
            if len(file.instances) == 0:
                continue

            transfers.append(
                test_orm.OutgoingTransfer.new_transfer(
                    destination="nowhere",
                    instance=file.instances[0],
                    file=file,
                )
            )
            outgoing_information.append(
                {
                    "upload_size": file.size,
                    "upload_checksum": file.checksum,
                    "upload_name": file.name,
                    "destination_location": file.name,
                    "uploader": file.uploader,
                }
            )

        session.add_all(transfers)
        session.commit()

        # We can now mock up the response from the librarian
        from hera_librarian.models.clone import (
            CloneBatchInitiationResponse,
            CloneBatchInitiationResponseFileItem,
        )

        # But make sure we omit one...
        items = []
        for transfer in transfers[:-1]:
            items.append(
                CloneBatchInitiationResponseFileItem(
                    staging_name=transfer.file_name,
                    staging_location=f"nowhere/{transfer.file_name}",
                    upload_name=transfer.file_name,
                    destination_location=f"nowhere/{transfer.file_name}",
                    source_transfer_id=transfer.id,
                    destination_transfer_id=transfer.id,
                )
            )

        from unittest.mock import MagicMock

        from hera_librarian.async_transfers import LocalAsyncTransferManager

        fake_transfer_manager = MagicMock(LocalAsyncTransferManager)
        fake_transfer_manager.valid = False

        fake_response = CloneBatchInitiationResponse(
            available_bytes_on_store=10000000000,
            store_name="fake_store",
            uploads=items,
            async_transfer_providers={"fake": fake_transfer_manager},
        )

        from librarian_background.send_clone import create_send_queue_item

        test, _, _ = create_send_queue_item(
            response=fake_response,
            outgoing_transfers=transfers,
            librarian=test_orm.Librarian(
                name="non_existent",
                url=fake_client.host,
                port=fake_client.port,
                authenticator="none:none",
                last_seen=datetime.now(timezone.utc),
                last_heard=datetime.now(timezone.utc),
            ),
            session=session,
        )

        assert test == False

    return
