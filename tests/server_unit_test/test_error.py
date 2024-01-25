"""
Tests we can log errors.
"""

from hera_librarian.errors import ErrorCategory, ErrorSeverity


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
