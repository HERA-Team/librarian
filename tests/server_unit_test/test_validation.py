"""
Tests the endpoints in librarian_server/api/validate.py.
"""

from hera_librarian.models.validate import (
    FileValidationFailedResponse,
    FileValidationRequest,
    FileValidationResponse,
    FileValidationResponseItem,
)


def test_validate_file(test_server_with_valid_file, test_client):
    request = FileValidationRequest(file_name="example_file.txt")

    response = test_client.post_with_auth(
        "/api/v2/validate/file", content=request.model_dump_json()
    )

    assert response.status_code == 200

    response = FileValidationResponse.model_validate_json(response.content).root

    assert len(response) == 1

    response = response[0]

    assert isinstance(response, FileValidationResponseItem)

    assert response.librarian == "test_server"

    assert response.computed_same_checksum

    # Modern checksums come with a hash function prefix
    assert (
        response.current_checksum.split(":")[-1]
        == response.original_checksum.split(":")[-1]
    )


def test_validate_file_invalid(
    test_server_with_invalid_file,
    test_client,
):
    request = FileValidationRequest(file_name="example_file.txt")

    response = test_client.post_with_auth(
        "/api/v2/validate/file", content=request.model_dump_json()
    )

    assert response.status_code == 200

    response = FileValidationResponse.model_validate_json(response.content).root

    assert len(response) == 1

    response = response[0]

    assert isinstance(response, FileValidationResponseItem)

    assert response.librarian == "test_server"

    assert not response.computed_same_checksum

    # Modern checksums come with a hash function prefix
    assert (
        response.current_checksum.split(":")[-1]
        != response.original_checksum.split(":")[-1]
    )


def test_validate_file_not_found(test_server, test_client):
    request = FileValidationRequest(file_name="not-an-existing-file.txt")

    response = test_client.post_with_auth(
        "/api/v2/validate/file", content=request.model_dump_json()
    )

    assert response.status_code == 400
