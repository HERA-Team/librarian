"""
Tests the checkin endpoints.
"""

from hera_librarian.models.checkin import (
    CheckinStatusRequest,
    CheckinStatusResponse,
    CheckinUpdateRequest,
    CheckinUpdateResponse,
)
from hera_librarian.transfer import TransferStatus


def test_checkin_endpoints(test_client, test_server_with_valid_file, test_orm):
    """
    Tests both the update and status endpoints.
    """

    # Create some incoming and outgoing transfers
    OutgoingTransfer = test_orm.OutgoingTransfer
    IncomingTransfer = test_orm.IncomingTransfer
    File = test_orm.File

    with test_server_with_valid_file[1]() as session:
        # Grab a useful file...
        file = session.query(File).one()

        out = OutgoingTransfer.new_transfer(
            destination="nowhere",
            instance=file.instances[0],
            file=file,
        )

        inc = IncomingTransfer.new_transfer(
            uploader="test",
            upload_name="my_best_friend",
            source="test",
            transfer_size=1024,
            transfer_checksum="nosum",
        )

        session.add_all([out, inc])
        session.commit()

        outgoing_id = out.id
        incoming_id = inc.id

    # Now use endpoints to check on our friends.
    request = CheckinStatusRequest(
        source_transfer_ids=[outgoing_id], destination_transfer_ids=[incoming_id]
    )
    response = CheckinStatusResponse.model_validate_json(
        test_client.post_with_auth(
            "/api/v2/checkin/status", content=request.model_dump_json()
        ).content
    )

    assert response.source_transfer_status == {outgoing_id: TransferStatus.INITIATED}
    assert response.destination_transfer_status == {
        incoming_id: TransferStatus.INITIATED
    }

    # Try to update them then
    for new_status in [
        TransferStatus.ONGOING,
        TransferStatus.STAGED,
        TransferStatus.CANCELLED,
    ]:
        request = CheckinUpdateRequest(
            source_transfer_ids=[outgoing_id],
            destination_transfer_ids=[incoming_id],
            new_status=new_status,
        )

        response = CheckinUpdateResponse.model_validate_json(
            test_client.post_with_auth(
                "/api/v2/checkin/update", content=request.model_dump_json()
            ).content
        )

        assert response.modified_source_transfer_ids == [outgoing_id]
        assert response.modified_destination_transfer_ids == [incoming_id]

        assert response.unmodified_destination_transfer_ids == []
        assert response.unmodified_source_transfer_ids == []

    # Now try an illegal update.
    for new_status in [TransferStatus.INITIATED, TransferStatus.COMPLETED]:
        request = CheckinUpdateRequest(
            source_transfer_ids=[outgoing_id],
            destination_transfer_ids=[incoming_id],
            new_status=new_status,
        )

        response = CheckinUpdateResponse.model_validate_json(
            test_client.post_with_auth(
                "/api/v2/checkin/update", content=request.model_dump_json()
            ).content
        )

        assert response.unmodified_source_transfer_ids == [outgoing_id]
        assert response.unmodified_destination_transfer_ids == [incoming_id]

        assert response.modified_destination_transfer_ids == []
        assert response.modified_source_transfer_ids == []

    with test_server_with_valid_file[1]() as session:
        session.delete(session.get(OutgoingTransfer, outgoing_id))
        session.delete(session.get(IncomingTransfer, incoming_id))
        session.commit()
