"""
Tests the endpoints in librarian_server/api/upload.py.
"""

import shutil
from hashlib import md5
from pathlib import Path
from typing import Any

from fastapi.applications import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.orm.session import Session

from hera_librarian.models.uploads import (
    UploadCompletionRequest,
    UploadFailedResponse,
    UploadInitiationRequest,
    UploadInitiationResponse,
)
from hera_librarian.utils import get_checksum_from_path, get_size_from_path

from ..server import Server


def test_negative_upload_size(test_client: TestClient):
    """
    Tests that a negative upload size results in an error.
    """

    request = UploadInitiationRequest(
        destination_location="test.txt",
        upload_size=-1,
        upload_checksum="",
        uploader="test",
        upload_name="test.txt",
    )

    response = test_client.post_with_auth(
        "/api/v2/upload/stage", content=request.model_dump_json()
    )

    assert response.status_code == 400
    assert response.json() == {
        "reason": "Upload size must be positive.",
        "suggested_remedy": "Check you are trying to upload a valid file.",
    }


def test_intermediate_upload_size(
    test_client: TestClient,
    test_server: tuple[FastAPI, callable, Server],
    test_orm: Any,
):
    """
    Tests an upload larger than the server settings maximum size,
    but small enough that it should still fit on disk.
    """

    request = UploadInitiationRequest(
        destination_location="test.txt",
        upload_size=10 * test_server[2].LIBRARIAN_SERVER_MAXIMAL_UPLOAD_SIZE_BYTES,
        upload_checksum="",
        uploader="test",
        upload_name="test.txt",
    )

    response = test_client.post_with_auth(
        "/api/v2/upload/stage", content=request.model_dump_json()
    )

    assert response.status_code == 413

    assert "is too large" in str(response.content)


def test_extreme_upload_size(
    test_client: TestClient,
    test_server: tuple[FastAPI, callable, Server],
    test_orm: Any,
):
    """
    Tests that an upload size that is too large results in an error.
    """

    request = UploadInitiationRequest(
        destination_location="test.txt",
        upload_size=1000000000000000000,
        upload_checksum="",
        uploader="test",
        upload_name="test.txt",
    )

    response = test_client.post_with_auth(
        "/api/v2/upload/stage", content=request.model_dump_json()
    )

    # Check we can decode the response
    decoded_response = UploadFailedResponse.model_validate_json(response.content)

    assert response.status_code == 413

    # Check we put the stuff in the database!
    _, get_session, _ = test_server

    with get_session() as session:
        assert (
            session.query(test_orm.IncomingTransfer).first().status
            == test_orm.TransferStatus.FAILED
        )


def test_valid_stage(
    test_client: TestClient,
    test_server: tuple[FastAPI, callable, Server],
    test_orm: Any,
):
    """
    Tests that a valid stage works.
    """

    # TODO: Implement FAIL like in clone.

    request = UploadInitiationRequest(
        destination_location="test.txt",
        upload_size=100,
        upload_checksum="hello_world_fake_checksum",
        uploader="test",
        upload_name="test.txt",
    )

    response = test_client.post_with_auth(
        "/api/v2/upload/stage", content=request.model_dump_json()
    )

    assert response.status_code == 201

    decoded_response = UploadInitiationResponse.model_validate_json(response.content)

    # Check we got this thing in the database.

    _, get_session, _ = test_server

    with get_session() as session:
        assert (
            session.query(test_orm.IncomingTransfer)
            .filter_by(id=decoded_response.transfer_id)
            .first()
            .status
            == test_orm.TransferStatus.INITIATED
        )

    # Now we can check what happens when we try to upload the same file.
    response = test_client.post_with_auth(
        "/api/v2/upload/stage", content=request.model_dump_json()
    )

    assert response.status_code == 201

    decoded_new_response = UploadInitiationResponse.model_validate_json(
        response.content
    )

    assert decoded_new_response.transfer_id != decoded_response.transfer_id


def helper_generate_transfer(
    client, server, orm, garbage_file, garbage_filename
) -> UploadInitiationResponse:
    """
    Generate a new outbound transfer.
    """

    with open(garbage_file, "rb") as handle:
        data = handle.read()

    request = UploadInitiationRequest(
        destination_location=str(garbage_filename),
        upload_size=len(data),
        upload_checksum=md5(data).hexdigest(),
        uploader="test",
        upload_name=str(garbage_filename),
    )

    response = client.post_with_auth(
        "/api/v2/upload/stage", content=request.model_dump_json()
    )

    assert response.status_code == 201

    decoded_response = UploadInitiationResponse.model_validate_json(response.content)

    # Need to actually copy the data to where it needs to be.
    shutil.copy2(garbage_file, decoded_response.staging_location)

    return decoded_response


def test_full_upload(
    test_client: TestClient,
    test_server: tuple[FastAPI, callable, Server],
    test_orm: Any,
    garbage_file: Path,
    garbage_filename: Path,
):
    """
    Tests that a full upload works.
    """

    stage_response = helper_generate_transfer(
        test_client, test_server, test_orm, garbage_file, garbage_filename
    )

    # Now we can actually test the commit endpoint.

    request = UploadCompletionRequest(
        store_name=stage_response.store_name,
        staging_name=stage_response.staging_name,
        staging_location=stage_response.staging_location,
        upload_name=stage_response.upload_name,
        destination_location=stage_response.destination_location,
        transfer_provider_name=list(stage_response.transfer_providers.keys())[0],
        transfer_provider=list(stage_response.transfer_providers.values())[0],
        meta_mode="infer",
        deletion_policy="DISALLOWED",
        uploader="test",
        transfer_id=stage_response.transfer_id,
    )

    response = test_client.post_with_auth(
        "/api/v2/upload/commit",
        content=request.model_dump_json(),
    )

    assert response.status_code == 200

    # Check we got this thing in the database.
    _, get_session, _ = test_server

    with get_session() as session:
        incoming_transfer = (
            session.query(test_orm.IncomingTransfer)
            .filter_by(id=stage_response.transfer_id)
            .first()
        )

        assert incoming_transfer.status == test_orm.TransferStatus.COMPLETED

        # Find the file in the store.
        instance = (
            session.query(test_orm.Instance)
            .filter_by(file_name=str(garbage_filename))
            .first()
        )

        # Check the file is where it should be.
        assert Path(instance.path).exists()

    # Now highjack this test to see what happens if we try to upload again!

    with open(garbage_file, "rb") as handle:
        data = handle.read()

    response = test_client.post_with_auth(
        "/api/v2/upload/stage",
        content=UploadInitiationRequest(
            destination_location=str(garbage_filename),
            upload_size=len(data),
            upload_checksum=md5(data).hexdigest(),
            uploader="test",
            upload_name=str(garbage_filename),
        ).model_dump_json(),
    )

    assert response.status_code == 409


def test_commit_no_file_uploaded(
    test_client, test_server, test_orm, garbage_file, garbage_filename
):
    """
    Tests that we can handle the case where we did not upload the file.
    """

    stage_response = helper_generate_transfer(
        test_client, test_server, test_orm, garbage_file, garbage_filename
    )

    # Delete the file that stage_response puts there.
    stage_response.staging_location.unlink()

    # Now we can actually test the commit endpoint.

    request = UploadCompletionRequest(
        store_name=stage_response.store_name,
        staging_name=stage_response.staging_name,
        staging_location=stage_response.staging_location,
        upload_name=stage_response.upload_name,
        destination_location=stage_response.destination_location,
        transfer_provider_name=list(stage_response.transfer_providers.keys())[0],
        transfer_provider=list(stage_response.transfer_providers.values())[0],
        meta_mode="infer",
        deletion_policy="DISALLOWED",
        uploader="test",
        transfer_id=stage_response.transfer_id,
    )

    response = test_client.post_with_auth(
        "/api/v2/upload/commit",
        content=request.model_dump_json(),
    )

    assert response.status_code == 404

    # Check we got this thing in the database.
    _, get_session, _ = test_server

    with get_session() as session:
        incoming_transfer = (
            session.query(test_orm.IncomingTransfer)
            .filter_by(id=stage_response.transfer_id)
            .first()
        )

        assert incoming_transfer.status == test_orm.TransferStatus.FAILED


def test_commit_wrong_file_uploaded(
    test_client, test_server, test_orm, garbage_file, garbage_filename
):
    """
    Tests that we can handle the case where we did not upload the file.
    """

    stage_response = helper_generate_transfer(
        test_client, test_server, test_orm, garbage_file, garbage_filename
    )

    # Delete the file that stage_response puts there and replace with garbage.
    with open(stage_response.staging_location, "w") as handle:
        handle.write("hello world")

    # Now we can actually test the commit endpoint.

    request = UploadCompletionRequest(
        store_name=stage_response.store_name,
        staging_name=stage_response.staging_name,
        staging_location=stage_response.staging_location,
        upload_name=stage_response.upload_name,
        destination_location=stage_response.destination_location,
        transfer_provider_name=list(stage_response.transfer_providers.keys())[0],
        transfer_provider=list(stage_response.transfer_providers.values())[0],
        meta_mode="infer",
        deletion_policy="DISALLOWED",
        uploader="test",
        transfer_id=stage_response.transfer_id,
    )

    response = test_client.post_with_auth(
        "/api/v2/upload/commit",
        content=request.model_dump_json(),
    )

    assert response.status_code == 406

    # Check we got this thing in the database.
    _, get_session, _ = test_server

    with get_session() as session:
        incoming_transfer = (
            session.query(test_orm.IncomingTransfer)
            .filter_by(id=stage_response.transfer_id)
            .first()
        )

        assert incoming_transfer.status == test_orm.TransferStatus.FAILED

    # Check we deleted the file
    assert not Path(stage_response.staging_location).exists()


def test_commit_file_exists(
    test_client, test_server, test_orm, garbage_file, garbage_filename
):
    """
    Tests that we can handle the case where the file already exists in the store area.
    """

    stage_response = helper_generate_transfer(
        test_client, test_server, test_orm, garbage_file, garbage_filename
    )

    _, get_session, _ = test_server

    with get_session() as session:
        store_metadata = (
            session.query(test_orm.StoreMetadata)
            .filter_by(name=stage_response.store_name)
            .first()
        )

        # Copy the file to the store area manually.
        shutil.copy2(
            garbage_file,
            store_metadata.store_manager._resolved_path_store(
                stage_response.destination_location
            ),
        )

    # Now we can actually test the commit endpoint.

    request = UploadCompletionRequest(
        store_name=stage_response.store_name,
        staging_name=stage_response.staging_name,
        staging_location=stage_response.staging_location,
        upload_name=stage_response.upload_name,
        destination_location=stage_response.destination_location,
        transfer_provider_name=list(stage_response.transfer_providers.keys())[0],
        transfer_provider=list(stage_response.transfer_providers.values())[0],
        meta_mode="infer",
        deletion_policy="DISALLOWED",
        uploader="test",
        transfer_id=stage_response.transfer_id,
    )

    response = test_client.post_with_auth(
        "/api/v2/upload/commit",
        content=request.model_dump_json(),
    )

    assert response.status_code == 409

    # Check we got this thing in the database.

    with get_session() as session:
        incoming_transfer = (
            session.query(test_orm.IncomingTransfer)
            .filter_by(id=stage_response.transfer_id)
            .first()
        )

        assert incoming_transfer.status == test_orm.TransferStatus.FAILED

    assert not Path(stage_response.staging_location).exists()


def test_directory_upload(test_client, test_server, test_orm, tmp_path):
    """
    Tests we can upload a directory.
    """

    path = Path(tmp_path)

    (path / "test.txt").write_text("hello world")
    (path / "test2.txt").write_text("hello world")

    request = UploadInitiationRequest(
        destination_location="test_directory",
        upload_size=get_size_from_path(path),
        upload_checksum=get_checksum_from_path(path),
        uploader="test",
        upload_name="test",
    )

    response = test_client.post_with_auth(
        "/api/v2/upload/stage", content=request.model_dump_json()
    )

    assert response.status_code == 201

    decoded_response = UploadInitiationResponse.model_validate_json(response.content)

    # Need to actually copy the data to where it needs to be.

    decoded_response.transfer_providers["local"].transfer(
        path, decoded_response.staging_location
    )

    # Now we can actually test the commit endpoint.

    request = UploadCompletionRequest(
        store_name=decoded_response.store_name,
        staging_name=decoded_response.staging_name,
        staging_location=decoded_response.staging_location,
        upload_name=decoded_response.upload_name,
        destination_location=decoded_response.destination_location,
        transfer_provider_name=list(decoded_response.transfer_providers.keys())[0],
        transfer_provider=list(decoded_response.transfer_providers.values())[0],
        meta_mode="infer",
        # Also use this as a chance to test a code path in DeletionPolicy
        deletion_policy="random",
        uploader="test",
        transfer_id=decoded_response.transfer_id,
    )

    response = test_client.post_with_auth(
        "/api/v2/upload/commit",
        content=request.model_dump_json(),
    )

    assert response.status_code == 200

    # Check we got this thing in the database.

    _, get_session, _ = test_server

    with get_session() as session:
        incoming_transfer = (
            session.query(test_orm.IncomingTransfer)
            .filter_by(id=decoded_response.transfer_id)
            .first()
        )

        assert incoming_transfer.status == test_orm.TransferStatus.COMPLETED
