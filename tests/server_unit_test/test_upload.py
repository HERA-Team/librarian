"""
Tests the endpoints in librarian_server/api/upload.py.
"""

from fastapi.applications import FastAPI
from fastapi.testclient import TestClient
from pathlib import Path
from sqlalchemy.orm.session import Session
from hera_librarian.models.uploads import (
    UploadInitiationRequest,
    UploadInitiationResponse,
    UploadCompletionRequest,
    UploadFailedResponse,
)

from hashlib import md5

import shutil

from .conftest import Server
from typing import Any


def test_negative_upload_size(client: TestClient):
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

    response = client.post("/api/v2/upload/stage", content=request.model_dump_json())

    assert response.status_code == 400
    assert response.json() == {
        "reason": "Upload size must be positive.",
        "suggested_remedy": "Check you are trying to upload a valid file.",
    }


def test_extreme_upload_size(
    client: TestClient, server: tuple[FastAPI, Session, Server], orm: Any
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

    response = client.post("/api/v2/upload/stage", content=request.model_dump_json())

    # Check we can decode the response
    decoded_response = UploadFailedResponse.model_validate_json(response.content)

    assert response.status_code == 413

    # Check we put the stuff in the database!
    _, session, _ = server

    assert (
        session.query(orm.IncomingTransfer).first().status == orm.TransferStatus.FAILED
    )


def test_valid_stage(
    client: TestClient, server: tuple[FastAPI, Session, Server], orm: Any
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

    response = client.post("/api/v2/upload/stage", content=request.model_dump_json())

    assert response.status_code == 201

    decoded_response = UploadInitiationResponse.model_validate_json(response.content)

    # Check we got this thing in the database.

    _, session, _ = server

    assert (
        session.query(orm.IncomingTransfer)
        .filter_by(id=decoded_response.transfer_id)
        .first()
        .status
        == orm.TransferStatus.INITIATED
    )

    # Now we can check what happens when we try to upload the same file.
    response = client.post("/api/v2/upload/stage", content=request.model_dump_json())

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

    response = client.post("/api/v2/upload/stage", content=request.model_dump_json())

    assert response.status_code == 201

    decoded_response = UploadInitiationResponse.model_validate_json(response.content)

    # Need to actually copy the data to where it needs to be.
    shutil.copy2(garbage_file, decoded_response.staging_location)

    return decoded_response


def test_full_upload(
    client: TestClient,
    server: tuple[FastAPI, Session, Server],
    orm: Any,
    garbage_file: Path,
    garbage_filename: Path,
):
    """
    Tests that a full upload works.
    """

    stage_response = helper_generate_transfer(
        client, server, orm, garbage_file, garbage_filename
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

    response = client.post(
        "/api/v2/upload/commit",
        content=request.model_dump_json(),
    )

    assert response.status_code == 200

    # Check we got this thing in the database.
    _, session, _ = server
    incoming_transfer = (
        session.query(orm.IncomingTransfer)
        .filter_by(id=stage_response.transfer_id)
        .first()
    )

    assert incoming_transfer.status == orm.TransferStatus.COMPLETED

    # Find the file in the store.
    instance = (
        session.query(orm.Instance).filter_by(file_name=str(garbage_filename)).first()
    )

    # Check the file is where it should be.
    assert Path(instance.path).exists()


def test_commit_no_file_uploaded(client, server, orm, garbage_file, garbage_filename):
    """
    Tests that we can handle the case where we did not upload the file.
    """


    stage_response = helper_generate_transfer(
            client, server, orm, garbage_file, garbage_filename
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

    response = client.post(
        "/api/v2/upload/commit",
        content=request.model_dump_json(),
    )

    assert response.status_code == 404

    # Check we got this thing in the database.
    _, session, _ = server
    incoming_transfer = (
        session.query(orm.IncomingTransfer)
        .filter_by(id=stage_response.transfer_id)
        .first()
    )

    assert incoming_transfer.status == orm.TransferStatus.FAILED
