"""
Tests for the rolling deletion task.
"""

import shutil
from datetime import datetime, timedelta
from pathlib import Path

from hera_librarian.deletion import DeletionPolicy


def prep_file(garbage_file, test_orm, session, FILE_NAME="path/for/rolling/deletion"):

    store = session.query(test_orm.StoreMetadata).filter_by(ingestable=True).first()

    info = store.store_manager.path_info(garbage_file)

    store_path = store.store_manager.store(Path(FILE_NAME))

    shutil.copy(garbage_file, store_path)

    # Create file and instances
    file = test_orm.File.new_file(
        filename=FILE_NAME,
        size=info.size,
        checksum=info.checksum,
        uploader="test_user",
        source="test_source",
    )

    instance = test_orm.Instance.new_instance(
        path=store_path, file=file, store=store, deletion_policy=DeletionPolicy.ALLOWED
    )

    session.add_all([file, instance])
    session.commit()

    return store, file, instance


def test_rolling_deletion_with_single_instance(
    test_client, test_server, test_orm, garbage_file
):
    """
    Delete a single instance.
    """
    from librarian_background.rolling_deletion import RollingDeletion

    _, get_session, _ = test_server

    session = get_session()

    store, file, instance = prep_file(garbage_file, test_orm, session)

    FILE_NAME = file.name
    INSTANCE_ID = instance.id

    # Run the task
    task = RollingDeletion(
        name="Rolling deletion",
        soft_timeout="6:00:00",
        store_name=store.name,
        age_in_days=0.0000000000000000001,
        number_of_remote_copies=0,
        verify_downstream_checksums=False,
        mark_unavailable=False,
        force_deletion=False,
    )()

    assert task

    session.close()

    session = get_session()

    # Check that the instance is gone
    assert (
        session.query(test_orm.Instance).filter_by(id=INSTANCE_ID).one_or_none() is None
    )

    # Delete the file we created
    session.get(test_orm.File, FILE_NAME).delete(
        session=session, commit=True, force=True
    )

    return


def test_rolling_deletion_with_single_instance_unavailable(
    test_client, test_server, test_orm, garbage_file
):
    """
    Delete a single instance.
    """
    from librarian_background.rolling_deletion import RollingDeletion

    _, get_session, _ = test_server

    session = get_session()

    store, file, instance = prep_file(garbage_file, test_orm, session)

    FILE_NAME = file.name
    INSTANCE_ID = instance.id

    # Run the task
    task = RollingDeletion(
        name="Rolling deletion",
        soft_timeout="6:00:00",
        store_name=store.name,
        age_in_days=0.0000000000000000001,
        number_of_remote_copies=0,
        verify_downstream_checksums=False,
        mark_unavailable=True,
        force_deletion=False,
    )()

    assert task

    # bgtask uses a different session
    session.close()

    session = get_session()

    # Check that the instance is gone
    re_queried = (
        session.query(test_orm.Instance).filter_by(id=INSTANCE_ID).one_or_none()
    )
    assert not re_queried.available

    # Delete the file we created
    session.get(test_orm.File, FILE_NAME).delete(
        session=session, commit=True, force=True
    )

    return


def test_rolling_deletion_with_multiple_files_age_out(
    test_client, test_server, test_orm, garbage_file
):
    """
    See if we correctly age out several files
    """
    from librarian_background.rolling_deletion import RollingDeletion

    _, get_session, _ = test_server

    session = get_session()

    file_names = []
    file_ages = []
    instance_ids = []

    for file_id in range(1, 10):
        store, file, instance = prep_file(
            garbage_file, test_orm, session, f"TEST_FILE/{file_id}.txt"
        )
        file.create_time = file.create_time - timedelta(days=file_id)
        instance.created_time = file.create_time

        file_names.append(file.name)
        file_ages.append(file_id)
        instance_ids.append(instance.id)

    session.commit()

    # Run the task
    task = RollingDeletion(
        name="Rolling deletion",
        soft_timeout="6:00:00",
        store_name=store.name,
        age_in_days=5.0,
        number_of_remote_copies=0,
        verify_downstream_checksums=False,
        mark_unavailable=True,
        force_deletion=False,
    )()

    assert task

    session.close()

    session = get_session()

    # Check that the older instances are gone

    instances = [
        session.query(test_orm.Instance).filter_by(id=id).one_or_none()
        for id in instance_ids
    ]

    for name, age, instance in zip(file_names, file_ages, instances):
        if age >= 5:
            assert not instance.available
        else:
            assert instance.available

        # Delete the file we created
        session.get(test_orm.File, name).delete(
            session=session, commit=True, force=True
        )

    return


def test_rolling_deletion_with_multiple_files_age_out_no_deletion_due_to_policy(
    test_client, test_server, test_orm, garbage_file
):
    """
    See if we correctly age out several files, but don't actually delete them because
    we can't find remote instances.
    """
    from librarian_background.rolling_deletion import RollingDeletion

    _, get_session, _ = test_server

    session = get_session()

    file_names = []
    file_ages = []
    instance_ids = []

    for file_id in range(1, 10):
        store, file, instance = prep_file(
            garbage_file, test_orm, session, f"TEST_FILE/{file_id}.txt"
        )
        file.create_time = file.create_time - timedelta(days=file_id)
        instance.created_time = file.create_time

        file_names.append(file.name)
        file_ages.append(file_id)
        instance_ids.append(instance.id)

    session.commit()

    # Run the task
    task = RollingDeletion(
        name="Rolling deletion",
        soft_timeout="6:00:00",
        store_name=store.name,
        age_in_days=5.0,
        number_of_remote_copies=1,
        verify_downstream_checksums=True,
        mark_unavailable=True,
        force_deletion=False,
    )()

    # Task officially fails; it could not delete the required number of instances
    assert not task

    session.close()

    session = get_session()

    # Check that the older instances are gone

    instances = [
        session.query(test_orm.Instance).filter_by(id=id).one_or_none()
        for id in instance_ids
    ]

    for name, age, instance in zip(file_names, file_ages, instances):
        assert instance.available

        # Delete the file we created
        session.get(test_orm.File, name).delete(
            session=session, commit=True, force=True
        )

    return
