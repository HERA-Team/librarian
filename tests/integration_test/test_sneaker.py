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
            name="librarian_server",
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
            name="librarian_server",
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
        destination_librarian="librarian_server",
        disable_store=True,
        mark_local_instances_as_unavailable=True,
    )

    # Now we can use the manifest to ingest the files into the destination librarian.
    for entry in manifest.store_files:
        # Only do this for our actual sneaker netted files. The other ones
        # that were present on the store we don't care about
        local_path = tmp_path / Path(entry.name).name

        if not local_path.exists():
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
    # - The availability of the store.
    # - The new copies of the files.
    # - The remote instances of the files.
