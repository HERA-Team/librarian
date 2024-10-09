"""
Test the admin endpoints from the client.
"""

import shutil
import subprocess

import pytest

from hera_librarian.deletion import DeletionPolicy
from hera_librarian.exceptions import LibrarianError
from hera_librarian.utils import get_md5_from_path, get_size_from_path


def test_add_file(
    server,
    admin_client,
    garbage_file,
    librarian_database_session_maker,
    test_orm,
    librarian_client_command_line,
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
        # Leave this as-is to test 'auto selection' of md5 checksums.
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

    table = subprocess.check_output(
        [
            "librarian",
            "validate-file",
            librarian_client_command_line,
            "test_upload_without_uploading.txt",
        ],
    )

    # Try uplaoding again.

    response = admin_client.add_file_row(
        name="test_upload_without_uploading.txt",
        create_time=garbage_file.stat().st_ctime,
        size=garbage_file.stat().st_size,
        # Leave this as-is to test 'auto selection' of md5 checksums.
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
            # Leave this as-is to test 'auto selection' of md5 checksums.
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
            # Leave this as-is to test 'auto selection' of md5 checksums.
            checksum=get_md5_from_path(full_path),
            uploader="test",
            path=str(
                store_location / "test_upload_without_uploading_but_doesnt_exist.txt"
            ),
            store_name="local_store",
        )

    # Clean up after ourselves.
    with librarian_database_session_maker() as session:
        # Check we got the correct file.
        file = session.get(test_orm.File, "test_upload_without_uploading.txt")

        file.delete(session=session, commit=False, force=True)

        session.commit()


def test_store_list(server, admin_client):
    store_list = admin_client.get_store_list()


def test_store_manifest(server, admin_client):
    store_list = admin_client.get_store_list()

    for store in store_list:
        manifest = admin_client.get_store_manifest(store.name)

        assert manifest.store_name == store.name

        for entry in manifest.store_files:
            assert entry.name is not None
            assert entry.create_time is not None
            assert entry.size is not None
            assert entry.checksum is not None
            assert entry.uploader is not None
            assert entry.source is not None
            assert entry.instance_path is not None
            assert entry.deletion_policy is not None
            assert entry.instance_create_time is not None
            assert entry.instance_available is not None

            assert entry.size == get_size_from_path(entry.instance_path)


def test_set_store_state(
    server, admin_client, librarian_database_session_maker, test_orm
):
    store_list = admin_client.get_store_list()

    for store in store_list:
        response = admin_client.set_store_state(store.name, enabled=False)

        assert response is False

        with librarian_database_session_maker() as session:
            store = (
                session.query(test_orm.StoreMetadata)
                .filter_by(name=store.name)
                .one_or_none()
            )

            assert not store.enabled

        response = admin_client.set_store_state(store.name, enabled=True)

        assert response is True

        with librarian_database_session_maker() as session:
            store = (
                session.query(test_orm.StoreMetadata)
                .filter_by(name=store.name)
                .one_or_none()
            )

            assert store.enabled


def test_add_search_remove_librarian(admin_client, librarian_client):
    response = admin_client.add_librarian(
        name="test_server",
        url="http://localhost",
        port=5000,
        authenticator="admin:test",
        check_connection=False,
    )

    assert response is True

    response = admin_client.get_librarian_list()

    assert "test_server" in [x.name for x in response.librarians]

    response = admin_client.remove_librarian(name="test_server")

    assert response == (True, 0)

    with pytest.raises(LibrarianError):
        response = admin_client.remove_librarian(name="test_server")


def test_add_remove_user_cli(librarian_client_command_line):
    assert subprocess.call(
        [
            "librarian",
            "create-user",
            librarian_client_command_line,
            "--username=test_created_user",
            "--password=test_password",
            "--authlevel=ADMIN",
        ]
    )

    assert subprocess.call(
        [
            "librarian",
            "remove-user",
            librarian_client_command_line,
            "--username=test_created_user",
        ]
    )
