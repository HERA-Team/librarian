"""
Tests we can log errors.
"""

from hera_librarian.errors import ErrorCategory, ErrorSeverity
from hera_librarian.models.errors import (
    ErrorClearRequest,
    ErrorClearResponse,
    ErrorSearchFailedResponse,
)


def test_error_to_db(test_server, test_orm):
    # Don't import until we've set up server settings for logging.
    from librarian_server.logger import log_to_database

    _, session_maker, _ = test_server

    with session_maker() as session:
        log_to_database(
            ErrorSeverity.CRITICAL, ErrorCategory.DATA_AVAILABILITY, "test", session
        )
        log_to_database(
            ErrorSeverity.INFO, ErrorCategory.DATA_AVAILABILITY, "test", session
        )
        log_to_database(
            ErrorSeverity.WARNING, ErrorCategory.DATA_AVAILABILITY, "test", session
        )
        log_to_database(
            ErrorSeverity.CRITICAL, ErrorCategory.DATA_INTEGRITY, "test", session
        )

    # Check that they were logged correctly
    with session_maker() as session:
        errors = session.query(test_orm.Error).all()

        assert len(errors) == 4

        for error in errors:
            assert error.message == "test"
            assert error.cleared is False
            assert error.cleared_time is None

        assert errors[0].severity == ErrorSeverity.CRITICAL
        assert errors[0].category == ErrorCategory.DATA_AVAILABILITY

        assert errors[1].severity == ErrorSeverity.INFO
        assert errors[1].category == ErrorCategory.DATA_AVAILABILITY

        assert errors[2].severity == ErrorSeverity.WARNING
        assert errors[2].category == ErrorCategory.DATA_AVAILABILITY

        assert errors[3].severity == ErrorSeverity.CRITICAL
        assert errors[3].category == ErrorCategory.DATA_INTEGRITY

    # Check we can clear them

    with session_maker() as session:
        errors = session.query(test_orm.Error).all()

        for error in errors:
            error.clear(session)

    # Check that they were cleared correctly
    with session_maker() as session:
        errors = session.query(test_orm.Error).all()

        assert len(errors) == 4

        for error in errors:
            assert error.message == "test"
            assert error.cleared is True
            assert error.cleared_time is not None

        assert errors[0].severity == ErrorSeverity.CRITICAL
        assert errors[0].category == ErrorCategory.DATA_AVAILABILITY

        assert errors[1].severity == ErrorSeverity.INFO
        assert errors[1].category == ErrorCategory.DATA_AVAILABILITY

        assert errors[2].severity == ErrorSeverity.WARNING
        assert errors[2].category == ErrorCategory.DATA_AVAILABILITY

        assert errors[3].severity == ErrorSeverity.CRITICAL
        assert errors[3].category == ErrorCategory.DATA_INTEGRITY


def test_clear_endpoint(test_server_with_many_files_and_errors, test_client, test_orm):
    """
    Test the clear endpoint.
    """

    request = ErrorClearRequest(id=-1)

    response = test_client.post(
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

    response = test_client.post(
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

    response = test_client.post(
        "/api/v2/error/clear",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
    )

    assert response.status_code == 400

    response = ErrorSearchFailedResponse.model_validate_json(response.content)
