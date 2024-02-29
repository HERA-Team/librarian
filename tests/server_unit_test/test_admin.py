"""
Tests for admin endpoints.
"""

import shutil

from hera_librarian.deletion import DeletionPolicy
from hera_librarian.models.admin import (
    AdminCreateFileRequest,
    AdminCreateFileResponse,
    AdminRequestFailedResponse,
    AdminStoreListResponse,
    AdminStoreManifestRequest,
    AdminStoreManifestResponse,
    AdminStoreStateChangeRequest,
    AdminStoreStateChangeResponse,
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


def test_add_flie_no_file_exists(test_client, test_server, test_orm):
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


def test_search_stores_and_manifest(test_client):
    """
    Tests that we can search for stores.
    """

    response = test_client.post_with_auth("/api/v2/admin/stores/list", content="")

    assert response.status_code == 200

    response = AdminStoreListResponse.model_validate_json(response.content).root

    # Now we can try the manifest!

    new_response = test_client.post_with_auth(
        "/api/v2/admin/stores/manifest",
        content=AdminStoreManifestRequest(
            store_name=response[0].name
        ).model_dump_json(),
    )

    assert new_response.status_code == 200

    new_response = AdminStoreManifestRequest.model_validate_json(new_response.content)

    assert new_response.store_name == response[0].name


def test_search_manifest_no_store(test_client):
    response = test_client.post_with_auth(
        "/api/v2/admin/stores/manifest",
        content=AdminStoreManifestRequest(store_name="not_a_store").model_dump_json(),
    )

    assert response.status_code == 400


def test_store_state_change(test_client):
    # First, search the stores.
    response = test_client.post_with_auth("/api/v2/admin/stores/list", content="")
    response = AdminStoreListResponse.model_validate_json(response.content).root

    response = test_client.post_with_auth(
        "/api/v2/admin/stores/state_change",
        content=AdminStoreStateChangeRequest(
            store_name=response[0].name, enabled=False
        ).model_dump_json(),
    )

    assert response.status_code == 200

    response = AdminStoreStateChangeResponse.model_validate_json(response.content)

    assert response.enabled == False

    # Get them again from search endpoint.
    response = test_client.post_with_auth("/api/v2/admin/stores/list", content="")

    response = AdminStoreListResponse.model_validate_json(response.content).root

    assert response[0].enabled == False

    # Enable it again.

    response = test_client.post_with_auth(
        "/api/v2/admin/stores/state_change",
        content=AdminStoreStateChangeRequest(
            store_name=response[0].name, enabled=True
        ).model_dump_json(),
    )

    assert response.status_code == 200


def test_store_state_change_no_store(test_client):
    response = test_client.post_with_auth(
        "/api/v2/admin/stores/state_change",
        content=AdminStoreStateChangeRequest(
            store_name="not_a_store", enabled=False
        ).model_dump_json(),
    )

    assert response.status_code == 400
    response = AdminRequestFailedResponse.model_validate_json(response.content)


def test_manifest_generation_and_extra_opts(
    test_client,
    test_server_with_many_files_and_errors,
    test_orm,
):
    """
    Tests that we can generate a manifest and that we can use extra options.
    """

    get_session = test_server_with_many_files_and_errors[1]

    # First, search the stores.
    response = test_client.post_with_auth("/api/v2/admin/stores/list", content="")
    response = AdminStoreListResponse.model_validate_json(response.content).root

    # Add in a librarian
    with get_session() as session:
        librarian = test_orm.Librarian.new_librarian(
            "our_closest_friend",
            "http://localhost",
            80,
            check_connection=False,
        )

        librarian.authenticator = "password"

        session.add(librarian)
        session.commit()

    # Now we can try the manifest!

    new_response = test_client.post_with_auth(
        "/api/v2/admin/stores/manifest",
        content=AdminStoreManifestRequest(
            store_name=response[0].name,
            create_outgoing_transfers=True,
            destination_librarian="our_closest_friend",
            disable_store=True,
            mark_local_instances_as_unavailable=True,
        ).model_dump_json(),
    )

    assert new_response.status_code == 200

    new_response = AdminStoreManifestResponse.model_validate_json(new_response.content)

    assert new_response.store_name == response[0].name
    assert new_response.librarian_name == "librarian_server"

    session = get_session()

    for entry in new_response.store_files:
        assert entry.outgoing_transfer_id >= 0

        instance = (
            session.query(test_orm.Instance).filter_by(path=entry.instance_path).first()
        )

        assert instance.available == False

        transfer = session.get(test_orm.OutgoingTransfer, entry.outgoing_transfer_id)

        assert transfer is not None
        assert transfer.destination == "our_closest_friend"

        session.delete(transfer)

    store = (
        session.query(test_orm.StoreMetadata)
        .filter_by(name=response[0].name)
        .one_or_none()
    )

    assert store.enabled == False
    store.enabled = True

    session.commit()
