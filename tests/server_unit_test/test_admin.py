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


def test_add_file(test_client, test_server, garbage_file):
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
