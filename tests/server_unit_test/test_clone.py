"""
Unit tests for endpoints in librarian_server/api/clone.py.
"""

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
        destination_location="test.txt",
        upload_size=-1,
        upload_checksum="",
        uploader="test",
        upload_name="test.txt",
        source="test_librarian",
        source_transfer_id=-1,
    )

    response = client.post("/api/v2/upload/stage", content=request.model_dump_json())

    assert response.status_code == 400
    assert response.json() == {
        "reason": "Upload size must be positive.",
        "suggested_remedy": "Check you are trying to upload a valid file.",
    }
