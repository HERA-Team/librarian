"""
Tests for admin endpoints.
"""

import shutil

from hera_librarian.deletion import DeletionPolicy
from hera_librarian.models.admin import (
    AdminCreateFileRequest,
    AdminCreateFileResponse,
    AdminRequestFailedResponse,
)
from hera_librarian.utils import get_md5_from_path, get_size_from_path


def test_add_file(test_client, test_server, garbage_file, test_orm):
    """
    Tests that we can add a file with no row in database.
    """

    # First, create the file in the store.
    setup = test_server[2]

    store = setup.store_directory

    full_path = store / "test_upload_without_uploading.txt"

    # Create the file in the store.
    shutil.copy2(garbage_file, full_path)

    request = AdminCreateFileRequest(
        name="test_upload_without_uploading.txt",
        create_time=garbage_file.stat().st_ctime,
        size=garbage_file.stat().st_size,
        checksum=get_md5_from_path(full_path),
        uploader="test",
        source="test",
        path=str(full_path),
        store_name="local_store",
    )

    response = test_client.post_with_auth(
        "/api/v2/admin/add_file", content=request.model_dump_json()
    )

    assert response.status_code == 200

    response = AdminCreateFileResponse.model_validate_json(response.content)

    assert response.success

    # Now can check what happens if we upload the file again...

    response = test_client.post_with_auth(
        "/api/v2/admin/add_file", content=request.model_dump_json()
    )

    assert response.status_code == 200

    response = AdminCreateFileResponse.model_validate_json(response.content)

    assert response.already_exists

    # Ok, now validate the actual db.

    get_session = test_server[1]

    with get_session() as session:
        file = session.get(test_orm.File, "test_upload_without_uploading.txt")

        assert file is not None

        instance = file.instances[0]

        assert instance is not None

        assert instance.path == str(full_path)
        assert instance.store.name == "local_store"


def test_add_file_no_file_exists(test_client, test_server, test_orm):
    """
    Tests that we can't add a file if the file doesn't exist.
    """

    request = AdminCreateFileRequest(
        name="non_existent_file.txt",
        create_time=0,
        size=0,
        checksum="",
        uploader="test",
        source="test",
        path="/this/file/does/not/exist",
        store_name="local_store",
    )

    response = test_client.post_with_auth(
        "/api/v2/admin/add_file", content=request.model_dump_json()
    )

    assert response.status_code == 400

    response = AdminRequestFailedResponse.model_validate_json(response.content)

    assert response.reason == "File /this/file/does/not/exist does not exist."
    assert (
        response.suggested_remedy
        == "Create the file first, or make sure that you are using a local store."
    )


def test_add_file_no_store_exists(test_client):
    """
    Tests the case where the store does not exist and we try to add a file.
    """

    request = AdminCreateFileRequest(
        name="non_existent_file.txt",
        create_time=0,
        size=0,
        checksum="",
        uploader="test",
        source="test",
        path="/this/file/does/not/exist",
        store_name="not_a_store",
    )

    response = test_client.post_with_auth(
        "/api/v2/admin/add_file", content=request.model_dump_json()
    )

    assert response.status_code == 400

    response = AdminRequestFailedResponse.model_validate_json(response.content)

    assert response.reason == "Store not_a_store does not exist."
