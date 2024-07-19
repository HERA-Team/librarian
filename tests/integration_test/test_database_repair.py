"""
Performs an integration test of the database repair script.
"""

from pathlib import Path

from hera_librarian.transfer import TransferStatus


def test_recover_from_disaster(
    test_server_with_many_files_and_errors,
    test_orm,
    mocked_admin_client,
    server,
    admin_client,
    librarian_database_session_maker,
    tmp_path,
):
    from librarian_server_scripts.librarian_server_repair_database import (
        core_destination,
        core_source,
    )

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
            session.delete(transfer)

        # DELETE THEM ALL!!!
        session.commit()

    # Ok, repair the database!
    repair_info = core_source(
        librarian_name="live_server",
        age=1024,
        name="test_server",
        session_maker=source_session_maker,
    )

    core_destination(
        input=repair_info.model_dump_json(),
        store_name="local_store",
        spot_check_every=16,
        track_progress=True,
        session_maker=librarian_database_session_maker,
    )

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
    # There will be only one...
    found_remote_instanace = False
    with source_session_maker() as session:
        for file_name in copied_files:
            file = session.get(test_orm.File, file_name)
            if len(file.remote_instances) > 0:
                found_remote_instanace = True
                remote_file_name = file.remote_instances[0].file_name
                break

    assert found_remote_instanace

    # Now we need to remove the downstream instance associated with this
    # remote instance
    with librarian_database_session_maker() as session:
        file_with_remote_instance = session.get(test_orm.File, remote_file_name)

        stored_properties_file = {
            "size": file_with_remote_instance.size,
            "checksum": file_with_remote_instance.checksum,
            "uploader": file_with_remote_instance.uploader,
            "source": file_with_remote_instance.source,
        }

        stored_properties_instance = {
            "path": file_with_remote_instance.instances[0].path,
            "store_id": file_with_remote_instance.instances[0].store_id,
        }

        file_with_remote_instance.delete(session=session, commit=False)
        session.commit()

    # Ok, now to repair!
    repair_info = core_source(
        librarian_name="live_server",
        age=1024,
        name="test_server",
        session_maker=source_session_maker,
    )

    core_destination(
        input=repair_info.model_dump_json(),
        store_name="local_store",
        spot_check_every=1,
        track_progress=True,
        session_maker=librarian_database_session_maker,
    )

    # Check we have that file!
    with librarian_database_session_maker() as session:
        repaired_file = session.get(test_orm.File, remote_file_name)

        for property, value in stored_properties_instance.items():
            assert getattr(repaired_file.instances[0], property) == value

        for property, value in stored_properties_file.items():
            assert getattr(repaired_file, property) == value

    # Remove the librarians we added.
    assert mocked_admin_client.remove_librarian(name="live_server")

    # Remove all the files we just copied over! And the incoming and
    # outgoing transfers
    with librarian_database_session_maker() as session:
        for file_name in copied_files:
            file = session.get(test_orm.File, file_name)

            file.delete(session=session, commit=False, force=True)

            transfer = (
                session.query(test_orm.IncomingTransfer)
                .filter_by(upload_name=file_name)
                .first()
            )
            session.delete(transfer)

        session.commit()

    with source_session_maker() as session:
        for file_name in sent_filenames:
            transfer = (
                session.query(test_orm.OutgoingTransfer)
                .filter_by(file_name=file_name)
                .first()
            )
            session.delete(transfer)

        session.commit()
