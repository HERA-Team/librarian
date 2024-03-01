"""
A full integration test of a sneakernet workflow.
"""

import random
from pathlib import Path

from hera_librarian.exceptions import LibrarianError


def test_sneakernet_workflow(
    test_server_with_many_files_and_errors,
    test_orm,
    mocked_admin_client,
    server,
    admin_client,
    librarian_database_session_maker,
    tmp_path,
):
    # Representing the destination in this test:
    # test_server_with_many_files_and_errors, test_orm, mocked_admin_client
    # Representing the source in this test:
    # server, admin_client, librarian_database_session_maker

    # We have to have it this way aruond because we need to be able
    # to make a callback to the source server, and we don't have
    # a mocking capability for that yet.

    # Before starting, register the downstream and upstream librarians.
    with test_server_with_many_files_and_errors[1]() as session:
        live_server = test_orm.Librarian.new_librarian(
            name="live_server",
            url="http://localhost",
            port=server.id,
            check_connection=False,
        )

        live_server.authenticator = "admin:password"

        live_server.client().ping()

        session.add(live_server)
        session.commit()

    with librarian_database_session_maker() as session:
        # Note we will never actually access this.
        test_server = test_orm.Librarian.new_librarian(
            name="test_server",
            url="http://localhost",
            port=test_server_with_many_files_and_errors[2].id,
            check_connection=False,
        )

        test_server.authenticator = "admin:password"

        session.add(test_server)
        session.commit()

    # First, we need to upload a bunch of files to the source server.

    file_names = [f"sneakernet_test_item_{x}.txt" for x in range(2)]

    for file in file_names:
        with open(tmp_path / file, "w") as handle:
            handle.write(str(random.randbytes(1024)))

        admin_client.upload(tmp_path / file, Path(f"sneakernet_test/{file}"))

    # Now we can use the clone tasks.
    from librarian_background.create_clone import CreateLocalClone

    task = CreateLocalClone(
        name="sneakernet_test_clone_job",
        clone_from="local_store",
        clone_to="local_sneaker",
        age_in_days=100,
    )

    with librarian_database_session_maker() as session:
        task.core(session=session)

    # Now we should have a complete clone on the sneaker clone.
    # We can generate our manifest.

    manifest = admin_client.get_store_manifest(
        "local_sneaker",
        create_outgoing_transfers=True,
        destination_librarian="test_server",
        disable_store=True,
        mark_local_instances_as_unavailable=True,
    )

    # Now we can use the manifest to ingest the files into the destination librarian.
    ingested_entries = []

    for entry in manifest.store_files:
        # Only do this for our actual sneaker netted files. The other ones
        # that were present on the store we don't care about
        local_path = tmp_path / Path(entry.name).name

        if not local_path.exists():
            # This is another file (not one that we actually wanted to sneaker
            # in this test). Because we didn't actually make a copy of the store,
            # we can't actually check these files; Let's just lie and say we
            # ingested it.

            admin_client.complete_outgoing_transfer(
                outgoing_transfer_id=entry.outgoing_transfer_id,
                store_id=1,
            )
            continue

        mocked_admin_client.ingest_manifest_entry(
            name=Path(entry.name),
            create_time=entry.create_time,
            size=entry.size,
            checksum=entry.checksum,
            uploader=entry.uploader,
            source=manifest.librarian_name,
            deletion_policy=entry.deletion_policy,
            source_transfer_id=entry.outgoing_transfer_id,
            local_path=tmp_path / Path(entry.name).name,
        )

        ingested_entries.append(entry)

    # Now we need to run the ingest job on the destination server.
    from librarian_background.recieve_clone import RecieveClone

    task = RecieveClone(
        name="sneakernet_recieve_clone_job",
    )

    with test_server_with_many_files_and_errors[1]() as session:
        task.core(session=session)

    # Now we should have ingested everything. We have a few things to check:
    # - The transfer status' of the outgoing and inbound transfers.
    # - The availability of the instances.
    # - The remote instances of the files.
    # - The availability of the store.
    # - The new copies of the files.

    # Check the transfer status.

    # Outgoing
    with librarian_database_session_maker() as session:
        for transfer in (x.outgoing_transfer_id for x in manifest.store_files):
            outgoing_transfer = session.get(test_orm.OutgoingTransfer, transfer)

            assert outgoing_transfer.status == test_orm.TransferStatus.COMPLETED

    # Incoming
    with test_server_with_many_files_and_errors[1]() as session:
        for transfer in (x.outgoing_transfer_id for x in ingested_entries):
            incoming_transfer = (
                session.query(test_orm.IncomingTransfer)
                .filter_by(source_transfer_id=transfer)
                .one_or_none()
            )

            assert incoming_transfer.status == test_orm.TransferStatus.COMPLETED

    # Now the availability of the instances.

    # Source
    with librarian_database_session_maker() as session:
        # Get source store
        source_store = (
            session.query(test_orm.StoreMetadata)
            .filter_by(name=manifest.store_name)
            .one()
        )

        for entry in ingested_entries:
            # Local Instance

            instance = (
                session.query(test_orm.Instance)
                .filter_by(file_name=entry.name, store_id=source_store.id)
                .one_or_none()
            )

            assert instance.available is False

            # Remote instance
            assert len(instance.file.remote_instances) > 0
            assert "test_server" in [
                x.librarian.name for x in instance.file.remote_instances
            ]

    # Destination
    used_store = None
    with test_server_with_many_files_and_errors[1]() as session:
        for entry in ingested_entries:
            # Only have local Instance
            instance = (
                session.query(test_orm.Instance)
                .filter_by(file_name=entry.name)
                .one_or_none()
            )

            used_store = instance.store.name

            assert instance.available is True

    # Check that we correctly disabled the store.
    with librarian_database_session_maker() as session:
        store = (
            session.query(test_orm.StoreMetadata)
            .filter_by(name=manifest.store_name)
            .one()
        )

        assert store.enabled is False

    # Check that the new copies of the files are actually there.
    from librarian_background.check_integrity import CheckIntegrity

    task = CheckIntegrity(
        name="sneakernet_check_integrity_job",
        age_in_days=100,
        store_name=used_store,
    )

    with test_server_with_many_files_and_errors[1]() as session:
        task.core(session=session)
