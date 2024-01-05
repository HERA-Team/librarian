"""
Tests the endpoints in librarian_server/api/upload.py.
"""

from hera_librarian.models.uploads import (
    UploadInitiationRequest,
    UploadInitiationResponse,
    UploadCompletionRequest,
    UploadFailedResponse,
)


def test_negative_upload_size(client):
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


def test_extreme_upload_size(client, server, orm):
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


def test_valid_stage(client, server, orm):
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

    decoded_new_response = UploadInitiationResponse.model_validate_json(response.content)

    assert decoded_new_response.transfer_id != decoded_response.transfer_id
