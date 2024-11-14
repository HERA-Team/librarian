"""
Tests we can log errors.
"""

from hera_librarian.errors import ErrorCategory, ErrorSeverity
from hera_librarian.models.errors import (
    ErrorClearRequest,
    ErrorClearResponse,
    ErrorSearchFailedResponse,
)


def test_clear_endpoint(test_server_with_many_files_and_errors, test_client, test_orm):
    """
    Test the clear endpoint.
    """

    request = ErrorClearRequest(id=-1)

    response = test_client.post_with_auth(
        "/api/v2/error/clear",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
    )

    assert response.status_code == 404

    response = ErrorSearchFailedResponse.model_validate_json(response.content)

    # Find an un-cleared item in the database.
    with test_server_with_many_files_and_errors[1]() as session:
        error_id = session.query(test_orm.Error).filter_by(cleared=False).first().id

    request = ErrorClearRequest(id=error_id)

    response = test_client.post_with_auth(
        "/api/v2/error/clear",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
    )

    assert response.status_code == 200

    response = ErrorClearResponse.model_validate_json(response.content)

    assert response.cleared is True

    # Check it was cleared in the database.

    with test_server_with_many_files_and_errors[1]() as session:
        error = session.get(test_orm.Error, error_id)

        assert error.cleared is True
        assert error.cleared_time is not None

    # Check we can't clear it again.

    request = ErrorClearRequest(id=error_id)

    response = test_client.post_with_auth(
        "/api/v2/error/clear",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
    )

    assert response.status_code == 400

    response = ErrorSearchFailedResponse.model_validate_json(response.content)
