"""
Unit tests for the RecieveClone background task.
"""

import shutil
from pathlib import Path

def test_recieve_clone_with_valid_no_clones(test_client, test_server, test_orm):
    """
    Test the null case where we have no incoming clones.
    """

    from librarian_background.recieve_clone import RecieveClone

    clone_task = RecieveClone(
        name="Recieve clone",
    )

    assert clone_task()

def test_recieve_clone_with_valid(test_client, test_server, test_orm, garbage_file):
    """
    Tests that we can recieve a clone where we have a valid incoming transfer.
    """

    from librarian_background.recieve_clone import RecieveClone

    # Get a store to use
    _, get_session, _ = test_server

    session = get_session()

    store = session.query(test_orm.StoreMetadata).filter_by(ingestable=True).first()

    # Create the fake incoming transfer
    stage_path, resolved_path = store.store_manager.stage(1024, garbage_file.name)
    shutil.copy2(garbage_file, resolved_path)

    info = store.store_manager.path_info(resolved_path)

    incoming_transfer = test_orm.IncomingTransfer.new_transfer(
        uploader="test_fake_librarian",
        source="test_user",
        upload_name=garbage_file.name,
        transfer_size=info.size,
        transfer_checksum=info.md5,
    )

    incoming_transfer.status = test_orm.TransferStatus.ONGOING
    incoming_transfer.store = store
    incoming_transfer.staging_path = str(resolved_path)
    incoming_transfer.store_path = str(store.store_manager.store(Path(garbage_file.name)))
    incoming_transfer.upload_name = garbage_file.name

    session.add(incoming_transfer)
    session.commit()

    incoming_transfer_id = incoming_transfer.id

    session.close()

    clone_task = RecieveClone(
        name="Recieve clone",
    )

    assert clone_task()

    # Now check in the DB to see if we marked the status as correct and moved the file to
    # the right place.

    session = get_session()

    incoming_transfer = session.get(test_orm.IncomingTransfer, incoming_transfer_id)

    assert incoming_transfer.status == test_orm.TransferStatus.COMPLETED

    # Check the file is in the right place.

    assert Path(incoming_transfer.store_path).exists()

    # Check the file is in the store.

    assert store.store_manager.path_info(Path(incoming_transfer.store_path)).md5 == info.md5

    session.close()
