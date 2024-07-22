"""
Unit tests for endpoints in librarian_server/api/clone.py.
"""

import shutil
from hashlib import md5
from pathlib import Path

from hera_librarian.models.clone import (
    CloneCompleteRequest,
    CloneCompleteResponse,
    CloneFailedResponse,
    CloneFailRequest,
    CloneFailResponse,
    CloneInitiationRequest,
    CloneInitiationResponse,
    CloneOngoingRequest,
    CloneOngoingResponse,
)


def test_stage_negative_clone(test_client):
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

    response = test_client.post_with_auth(
        "/api/v2/clone/stage", content=request.model_dump_json()
    )

    assert response.status_code == 400

    decoded_response = CloneFailedResponse.model_validate_json(
        response.json()["detail"]
    )


def test_extreme_clone_size(test_client, test_server, test_orm):
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

    response = test_client.post_with_auth(
        "/api/v2/clone/stage", content=request.model_dump_json()
    )

    assert response.status_code == 413

    # Check we can decode the response
    decoded_response = CloneFailedResponse.model_validate_json(
        response.json()["detail"]
    )


def test_valid_stage_and_fail(test_client, test_server, test_orm):
    request = CloneInitiationRequest(
        destination_location="test_valid_stage_clone.txt",
        upload_size=100,
        upload_checksum="",
        uploader="test",
        upload_name="test_valid_stage_clone.txt",
        source="test_librarian",
        source_transfer_id=-1,
    )

    response = test_client.post_with_auth(
        "/api/v2/clone/stage", content=request.model_dump_json()
    )

    assert response.status_code == 201

    decoded_response = CloneInitiationResponse.model_validate_json(response.content)

    # Check we got this thing in the database.

    _, get_session, _ = test_server

    with get_session() as session:
        assert (
            session.query(test_orm.IncomingTransfer)
            .filter_by(id=decoded_response.destination_transfer_id)
            .first()
            .status
            == test_orm.TransferStatus.INITIATED
        )

    # Now see what happens if we try to clone again.

    response = test_client.post_with_auth(
        "/api/v2/clone/stage", content=request.model_dump_json()
    )

    assert response.status_code == 406

    decoded_response_new = CloneFailedResponse.model_validate_json(
        response.json()["detail"]
    )

    # Now fail that original transfer.

    request = CloneFailRequest(
        source_transfer_id=decoded_response.source_transfer_id,
        destination_transfer_id=decoded_response.destination_transfer_id,
        reason="test",
    )

    response = test_client.post_with_auth(
        "/api/v2/clone/fail", content=request.model_dump_json()
    )

    assert response.status_code == 200

    decoded_response = CloneFailResponse.model_validate_json(response.content)

    with get_session() as session:
        assert (
            session.query(test_orm.IncomingTransfer)
            .filter_by(id=decoded_response.destination_transfer_id)
            .first()
            .status
            == test_orm.TransferStatus.FAILED
        )


def test_try_to_fail_non_existent_transfer(test_client, test_server, test_orm):
    """
    Tests that trying to fail a transfer that doesn't exist results in an error.
    """
    request = CloneFailRequest(
        source_transfer_id=-100000,
        destination_transfer_id=-10000,
        reason="test",
    )

    response = test_client.post_with_auth(
        "/api/v2/clone/fail", content=request.model_dump_json()
    )

    assert response.status_code == 404

    decoded_response = CloneFailedResponse.model_validate_json(response.content)


def test_ongoing_transfer_nonexistent(test_client, test_server, test_orm):
    """
    Tests that we get a 404 when trying to set status of a fake transfer.
    """

    request = CloneOngoingRequest(
        source_transfer_id=-100000,
        destination_transfer_id=-10000,
    )

    response = test_client.post_with_auth(
        "/api/v2/clone/ongoing", content=request.model_dump_json()
    )

    assert response.status_code == 404

    decoded_response = CloneFailedResponse.model_validate_json(response.content)

    return


def test_ongoing_transfer(
    test_client, test_server, test_orm, garbage_file, garbage_filename
):
    """
    Tests that we can set a transfer to actively ongoing.

    We can't test that the transfer can be committed!
    """

    with open(garbage_file, "rb") as f:
        data = f.read()
        # Leave as-is to test auto-selection of md5
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

    response = test_client.post_with_auth(
        "/api/v2/clone/stage", content=request.model_dump_json()
    )

    assert response.status_code == 201

    decoded_response = CloneInitiationResponse.model_validate_json(response.content)

    # Now call the ongoing endpoint

    request_ongoing = CloneOngoingRequest(
        source_transfer_id=decoded_response.source_transfer_id,
        destination_transfer_id=decoded_response.destination_transfer_id,
    )

    response = test_client.post_with_auth(
        "/api/v2/clone/ongoing", content=request_ongoing.model_dump_json()
    )

    assert response.status_code == 200

    decoded_response_ongoing = CloneOngoingResponse.model_validate_json(
        response.content
    )

    # Check it's in the database with correct status

    _, get_session, _ = test_server

    with get_session() as session:
        assert (
            session.query(test_orm.IncomingTransfer)
            .filter_by(id=decoded_response.destination_transfer_id)
            .first()
            .status
            == test_orm.TransferStatus.ONGOING
        )

    # If we try to upload again with the same source and destination, it should fail.

    response = test_client.post_with_auth(
        "/api/v2/clone/stage", content=request.model_dump_json()
    )

    assert response.status_code == 425

    decoded_response_ongoing_fail = CloneFailedResponse.model_validate_json(
        response.json()["detail"]
    )

    # Let's fail the transfer

    request_fail = CloneFailRequest(
        source_transfer_id=decoded_response.source_transfer_id,
        destination_transfer_id=decoded_response.destination_transfer_id,
        reason="test",
    )

    response = test_client.post_with_auth(
        "/api/v2/clone/fail", content=request_fail.model_dump_json()
    )

    assert response.status_code == 200

    decoded_response_fail = CloneFailResponse.model_validate_json(response.content)


def test_incoming_transfer_endpoints(
    test_client, test_server, test_orm, garbage_file, garbage_filename
):
    """
    Tests the inbound transfer endpoints (we are playing as the server
    that is having stuff sent to it, not the client that is sending)
    """

    _, get_session, _ = test_server

    # First we need to create fake files and instances.

    with get_session() as session:
        file = test_orm.File.new_file(
            filename=garbage_filename,
            size=100,
            checksum="abcd",
            uploader="test",
            source="test",
        )

        store = session.query(test_orm.StoreMetadata).first()

        # Move the file into the destination area

        store_path = store.store_manager.store(
            Path("garbage_file_test_incoming_transfer_endpoints.txt")
        )
        shutil.copy2(garbage_file, store_path)

        instance = test_orm.Instance.new_instance(
            path=store_path,
            file=file,
            store=store,
            deletion_policy="DISALLOWED",
        )

        # Add first to get IDs
        session.add_all([file, instance])
        session.commit()

        transfer = test_orm.OutgoingTransfer.new_transfer(
            destination="test2", instance=instance, file=file
        )

        session.add(transfer)
        session.commit()

        librarian = test_orm.Librarian.new_librarian(
            name="test2",
            url="http://localhost",
            port=5000,
            check_connection=False,
            authenticator="admin:password",
        )
        librarian.authenticator = "does_not_authenticate"
        session.add(librarian)
        session.commit()

        transfer_id = transfer.id
        instance_id = instance.id
        store_id = store.id

    # We will first test the failure case where we have not set the transfer to be ongoing

    # Now call the endpoint

    request = CloneCompleteRequest(
        source_transfer_id=transfer_id,
        destination_transfer_id=transfer_id,
        store_id=store_id,
    )

    response = test_client.post_with_auth(
        "/api/v2/clone/complete", content=request.model_dump_json()
    )

    assert response.status_code == 406

    decoded_response = CloneFailedResponse.model_validate_json(response.content)

    # Now try again but set the transfer to be ongoing
    with get_session() as session:
        transfer = session.get(test_orm.OutgoingTransfer, transfer_id)

        transfer.status = test_orm.TransferStatus.ONGOING
        session.commit()

    response = test_client.post_with_auth(
        "/api/v2/clone/complete", content=request.model_dump_json()
    )

    assert response.status_code == 200

    decoded_response = CloneCompleteResponse.model_validate_json(response.content)

    # Check it's in the database with correct status

    # Clean up that garbage
    with get_session() as session:
        transfer = session.get(test_orm.OutgoingTransfer, transfer_id)

        assert transfer.status == test_orm.TransferStatus.COMPLETED

        file = session.get(test_orm.File, str(garbage_filename))

        file.delete(session=session, commit=False, force=True)

        session.commit()


def test_complete_no_transfer(test_client, test_server, test_orm):
    """
    Test the case where we try to complete a non-existent transfer.
    """

    request = CloneCompleteRequest(
        source_transfer_id=-1,
        destination_transfer_id=-1,
        store_id=-1,
    )

    response = test_client.post_with_auth(
        "/api/v2/clone/complete", content=request.model_dump_json()
    )

    assert response.status_code == 400

    decoded_response = CloneFailedResponse.model_validate_json(response.content)


def test_set_ongoing_with_different_status(test_client, test_server, test_orm):
    """
    Test the case when we try to set a transfer to ongoing when it's already
    completed (or has some other status).
    """

    _, get_session, _ = test_server

    with get_session() as session:
        transfer = test_orm.IncomingTransfer.new_transfer(
            uploader="test",
            source="admin",
            upload_name="test",
            transfer_size=100,
            transfer_checksum="",
        )

        transfer.status = test_orm.TransferStatus.COMPLETED

        session.add(transfer)

        transfer_fail = test_orm.IncomingTransfer.new_transfer(
            uploader="test",
            source="NOTADMIN",
            upload_name="test",
            transfer_size=100,
            transfer_checksum="",
        )

        session.add(transfer_fail)

        session.commit()

        transfer_id = transfer.id
        transfer_fail_id = transfer_fail.id

    request = CloneOngoingRequest(
        source_transfer_id=transfer_id,
        destination_transfer_id=transfer_id,
    )

    response = test_client.post_with_auth(
        "/api/v2/clone/ongoing", content=request.model_dump_json()
    )

    assert response.status_code == 406

    decoded_response = CloneFailedResponse.model_validate_json(response.content)

    # TODO: Fix this. For sneakernet, we no longer test that you are trying
    #       to modify your own transfers...
    # response = test_client.post_with_auth(
    #     "/api/v2/clone/ongoing",
    #     content=CloneOngoingRequest(
    #         source_transfer_id=transfer_fail_id,
    #         destination_transfer_id=transfer_fail_id,
    #     ).model_dump_json(),
    # )

    # assert response.status_code == 404


def test_clone_file_exists(test_client, test_server, test_orm, garbage_filename):
    """
    Test what happens if we try to upload a file that already exists.
    """

    file = test_orm.File.new_file(
        filename=garbage_filename,
        size=100,
        checksum="abcd",
        uploader="test",
        source="test",
    )

    _, get_session, _ = test_server

    with get_session() as session:
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

    response = test_client.post_with_auth(
        "/api/v2/clone/stage", content=request.model_dump_json()
    )

    assert response.status_code == 409

    # Check we can decode the response
    decoded_response = CloneFailedResponse.model_validate_json(
        response.json()["detail"]
    )

    # Clean up that garbage
    with get_session() as session:
        file = session.get(test_orm.File, str(garbage_filename))
        file.delete(session=session, commit=True, force=True)
