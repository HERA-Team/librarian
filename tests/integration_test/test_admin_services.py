"""
Test the admin endpoints from the client.
"""

import shutil

import pytest

from hera_librarian.deletion import DeletionPolicy
from hera_librarian.exceptions import LibrarianError
from hera_librarian.utils import get_md5_from_path, get_size_from_path


def test_add_file(
    server, admin_client, garbage_file, librarian_database_session_maker, test_orm
):
    """
    Tests that we can add a file with no row in database.
    """

    # First, create the file in the store.
    store_location = server.store_directory

    full_path = store_location / "test_upload_without_uploading.txt"

    # Create the file in the store.
    shutil.copy2(garbage_file, full_path)

    response = admin_client.add_file_row(
        name="test_upload_without_uploading.txt",
        create_time=garbage_file.stat().st_ctime,
        size=garbage_file.stat().st_size,
        checksum=get_md5_from_path(full_path),
        uploader="test",
        path=str(full_path),
        store_name="local_store",
    )

    # Need to check in the actual db..

    with librarian_database_session_maker() as session:
        # Check we got the correct file.
        file = session.get(test_orm.File, "test_upload_without_uploading.txt")

        assert file is not None

        # Now check the instance.
        instance = file.instances[0]

        assert instance.path == str(full_path)
        assert instance.store.name == "local_store"
        assert instance.deletion_policy == DeletionPolicy.DISALLOWED
        assert instance.file == file

    assert response.success
    assert response.already_exists is False

    # Try uplaoding again.

    response = admin_client.add_file_row(
        name="test_upload_without_uploading.txt",
        create_time=garbage_file.stat().st_ctime,
        size=garbage_file.stat().st_size,
        checksum=get_md5_from_path(full_path),
        uploader="test",
        path=str(full_path),
        store_name="local_store",
    )

    assert response.already_exists
    assert response.success

    with pytest.raises(LibrarianError):
        response = admin_client.add_file_row(
            name="test_upload_without_uploading.txt",
            create_time=garbage_file.stat().st_ctime,
            size=garbage_file.stat().st_size,
            checksum=get_md5_from_path(full_path),
            uploader="test",
            path=str(full_path),
            store_name="fake_store",
        )

    with pytest.raises(LibrarianError):
        response = admin_client.add_file_row(
            name="test_upload_without_uploading_but_doesnt_exist.txt",
            create_time=garbage_file.stat().st_ctime,
            size=garbage_file.stat().st_size,
            checksum=get_md5_from_path(full_path),
            uploader="test",
            path=str(
                store_location / "test_upload_without_uploading_but_doesnt_exist.txt"
            ),
            store_name="fake_store",
        )
