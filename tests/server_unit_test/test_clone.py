"""
Unit tests for endpoints in librarian_server/api/clone.py.
"""

from hashlib import md5
import shutil
from hera_librarian.models.clone import (
    CloneInitiationRequest,
    CloneInitiationResponse,
    CloneOngoingRequest,
    CloneOngoingResponse,
    CloneCompleteRequest,
    CloneCompleteResponse,
    CloneFailedResponse,
    CloneFailResponse,
    CloneFailRequest,
)


def test_stage_negative_clone(client):
    """
    Tests that a negative upload size results in an error.
    """

    request = CloneInitiationRequest(
        destination_location="test_stage_negative_clone.txt",
        upload_size=-1,
        upload_checksum="",
        uploader="test",
        upload_name="test_stage_negative_clone.txt",
        source="test_librarian",
        source_transfer_id=-1,
    )

    response = client.post("/api/v2/clone/stage", content=request.model_dump_json())

    assert response.status_code == 400

    decoded_response = CloneFailedResponse.model_validate_json(response.content)


def test_extreme_clone_size(client, server, orm):
    """
    Tests that an upload size that is too large results in an error.
    """

    request = CloneInitiationRequest(
        destination_location="test_extreme_clone_size.txt",
        upload_size=1000000000000000000,
        upload_checksum="",
        uploader="test",
        upload_name="test_extreme_clone_size.txt",
        source="test_librarian",
        source_transfer_id=-1,
    )

    response = client.post("/api/v2/clone/stage", content=request.model_dump_json())

    assert response.status_code == 413

    # Check we can decode the response
    decoded_response = CloneFailedResponse.model_validate_json(response.content)


def test_valid_stage_and_fail(client, server, orm):
    request = CloneInitiationRequest(
        destination_location="test_valid_stage_clone.txt",
        upload_size=100,
        upload_checksum="",
        uploader="test",
        upload_name="test_valid_stage_clone.txt",
        source="test_librarian",
        source_transfer_id=-1,
    )

    response = client.post("/api/v2/clone/stage", content=request.model_dump_json())

    assert response.status_code == 201

    decoded_response = CloneInitiationResponse.model_validate_json(response.content)

    # Check we got this thing in the database.

    _, session, _ = server

    assert (
        session.query(orm.IncomingTransfer)
        .filter_by(id=decoded_response.destination_transfer_id)
        .first()
        .status
        == orm.TransferStatus.INITIATED
    )

    # Now see what happens if we try to clone again.

    response = client.post("/api/v2/clone/stage", content=request.model_dump_json())

    assert response.status_code == 406

    decoded_response_new = CloneFailedResponse.model_validate_json(response.content)

    # Now fail that original transfer.

    request = CloneFailRequest(
        source_transfer_id=decoded_response.source_transfer_id,
        destination_transfer_id=decoded_response.destination_transfer_id,
        reason="test",
    )

    response = client.post("/api/v2/clone/fail", content=request.model_dump_json())

    assert response.status_code == 200

    decoded_response = CloneFailResponse.model_validate_json(response.content)

    assert (
        session.query(orm.IncomingTransfer)
        .filter_by(id=decoded_response.destination_transfer_id)
        .first()
        .status
        == orm.TransferStatus.FAILED
    )


def test_try_to_fail_non_existent_transfer(client, server, orm):
    """
    Tests that trying to fail a transfer that doesn't exist results in an error.
    """
    request = CloneFailRequest(
        source_transfer_id=-100000,
        destination_transfer_id=-10000,
        reason="test",
    )

    response = client.post("/api/v2/clone/fail", content=request.model_dump_json())

    assert response.status_code == 404

    decoded_response = CloneFailedResponse.model_validate_json(response.content)


def test_ongoing_transfer_nonexistent(client, server, orm):
    """
    Tests that we get a 404 when trying to set status of a fake transfer.
    """

    request = CloneOngoingRequest(
        source_transfer_id=-100000,
        destination_transfer_id=-10000,
    )

    response = client.post("/api/v2/clone/ongoing", content=request.model_dump_json())

    assert response.status_code == 404

    decoded_response = CloneFailedResponse.model_validate_json(response.content)

    return


def test_ongoing_transfer(client, server, orm, garbage_file, garbage_filename):
    """
    Tests that we can set a transfer to actively ongoing.

    We can't test that the transfer can be committed!
    """

    with open(garbage_file, "rb") as f:
        data = f.read()
        checksum = md5(data).hexdigest()
        size = len(data)

    request = CloneInitiationRequest(
        destination_location=garbage_filename,
        upload_size=size,
        upload_checksum=checksum,
        uploader="test",
        upload_name=garbage_filename,
        source="test_librarian",
        source_transfer_id=-1,
    )

    response = client.post("/api/v2/clone/stage", content=request.model_dump_json())

    assert response.status_code == 201

    decoded_response = CloneInitiationResponse.model_validate_json(response.content)

    # Now call the ongoing endpoint

    request_ongoing = CloneOngoingRequest(
        source_transfer_id=decoded_response.source_transfer_id,
        destination_transfer_id=decoded_response.destination_transfer_id,
    )

    response = client.post("/api/v2/clone/ongoing", content=request_ongoing.model_dump_json())

    assert response.status_code == 200

    decoded_response_ongoing = CloneOngoingResponse.model_validate_json(response.content)

    # Check it's in the database with correct status

    _, session, _ = server

    assert (
        session.query(orm.IncomingTransfer)
        .filter_by(id=decoded_response.destination_transfer_id)
        .first()
        .status
        == orm.TransferStatus.ONGOING
    )

    # If we try to upload again with the same source and destination, it should fail.

    response = client.post("/api/v2/clone/stage", content=request.model_dump_json())

    assert response.status_code == 425

    decoded_response_ongoing_fail = CloneFailedResponse.model_validate_json(response.content)

    # Let's fail the transfer

    request_fail = CloneFailRequest(
        source_transfer_id=decoded_response.source_transfer_id,
        destination_transfer_id=decoded_response.destination_transfer_id,
        reason="test",
    )

    response = client.post("/api/v2/clone/fail", content=request_fail.model_dump_json())

    assert response.status_code == 200

    decoded_response_fail = CloneFailResponse.model_validate_json(response.content)


def test_incoming_transfer_endpoints(
    client, server, orm, garbage_file, garbage_filename
):
    """
    Tests the inbound transfer endpoints (we are playing as the server
    that is having stuff sent to it, not the client that is sending)
    """

    _, session, _ = server

    # First we need to create fake files and instances.

    file = orm.File.new_file(
        filename=garbage_filename,
        size=100,
        checksum="abcd",
        uploader="test",
        source="test",
    )

    store = session.query(orm.StoreMetadata).first()

    instance = orm.Instance.new_instance(
        path=garbage_file,
        file=file,
        store=store,
        deletion_policy="DISALLOWED",
    )

    # Add first to get IDs 
    session.add_all([file, instance])
    session.commit()

    transfer = orm.OutgoingTransfer.new_transfer(
        destination="test2",
        instance=instance,
        file=file
    )

    session.add(transfer)
    session.commit()

    # We will first test the failure case where we have not set the transfer to be ongoing

    # Now call the endpoint

    request = CloneCompleteRequest(
        source_transfer_id=transfer.id,
        destination_transfer_id=transfer.id,
    )

    response = client.post("/api/v2/clone/complete", content=request.model_dump_json())

    assert response.status_code == 406

    decoded_response = CloneFailedResponse.model_validate_json(response.content)

    # Now try again but set the transfer to be ongoing
    transfer.status = orm.TransferStatus.ONGOING
    session.commit()

    response = client.post("/api/v2/clone/complete", content=request.model_dump_json())

    assert response.status_code == 200

    decoded_response = CloneCompleteResponse.model_validate_json(response.content)

    # Check it's in the database with correct status

    assert transfer.status == orm.TransferStatus.COMPLETED


def test_complete_no_transfer(client, server, orm):
    """
    Test the case where we try to complete a non-existent transfer.
    """

    request = CloneCompleteRequest(
        source_transfer_id=-1,
        destination_transfer_id=-1,
    )

    response = client.post("/api/v2/clone/complete", content=request.model_dump_json())

    assert response.status_code == 404

    decoded_response = CloneFailedResponse.model_validate_json(response.content)


def test_set_ongoing_with_different_status(client, server, orm):
    """
    Test the case when we try to set a transfer to ongoing when it's already
    completed (or has some other status).
    """

    _, session, _ = server

    transfer = orm.IncomingTransfer.new_transfer(
        uploader="test",
        source="test",
        transfer_size=100,
        transfer_checksum="",
    )

    transfer.status = orm.TransferStatus.COMPLETED

    session.add(transfer)
    session.commit()

    request = CloneOngoingRequest(
        source_transfer_id=transfer.id,
        destination_transfer_id=transfer.id,
    )

    response = client.post("/api/v2/clone/ongoing", content=request.model_dump_json())

    assert response.status_code == 406

    decoded_response = CloneFailedResponse.model_validate_json(response.content)


def test_clone_file_exists(client, server, orm, garbage_filename):
    """
    Test what happens if we try to upload a file that already exists.
    """

    file = orm.File.new_file(
        filename=garbage_filename,
        size=100,
        checksum="abcd",
        uploader="test",
        source="test",
    )

    _, session, _ = server
    session.add(file)
    session.commit()

    request = CloneInitiationRequest(
        destination_location=garbage_filename,
        upload_size=100,
        upload_checksum="abcd",
        uploader="test",
        upload_name=garbage_filename,
        source="test_librarian",
        source_transfer_id=-1,
    )

    response = client.post("/api/v2/clone/stage", content=request.model_dump_json())

    assert response.status_code == 409

    # Check we can decode the response
    decoded_response = CloneFailedResponse.model_validate_json(response.content)
