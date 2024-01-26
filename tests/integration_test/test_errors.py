"""
Tests that the client can handle communicating with the librarian about errors correctly.
"""

import pytest

from hera_librarian.errors import ErrorCategory, ErrorSeverity


@pytest.fixture(scope="function")
def server_with_fake_errors(server, test_orm, librarian_database_session_maker):
    """
    Starts a server with a fake error.
    """

    with librarian_database_session_maker() as session:
        error = test_orm.Error.new_error(
            severity=ErrorSeverity.CRITICAL,
            category=ErrorCategory.CONFIGURATION,
            message="This is a fake error.",
        )

        session.add(error)
        session.commit()

        error_id = error.id

    yield server

    with librarian_database_session_maker() as session:
        error = session.get(test_orm.Error, error_id)

        session.delete(error)
        session.commit()


def test_error_search(server_with_fake_errors, librarian_client):
    """
    Tests that the client can search for errors correctly.
    """

    all_errors = librarian_client.search_errors()

    assert len(all_errors) > 0

    assert all_errors[0].id is not None
    assert all_errors[0].severity is not None
    assert all_errors[0].category is not None
    assert all_errors[0].message is not None
    assert all_errors[0].raised_time is not None
    assert all_errors[0].cleared_time is None
    assert all_errors[0].cleared is False

    # See if we can clear this error.

    error_to_clear = all_errors[0].id

    cleared_error = librarian_client.clear_error(error_to_clear)

    all_errors = librarian_client.search_errors(
        id=error_to_clear, include_resolved=True
    )

    assert all_errors[0].cleared

    # Check what happens if we clear it again

    with pytest.raises(ValueError):
        cleared_error = librarian_client.clear_error(error_to_clear)


def test_error_search_missing(server_with_fake_errors, librarian_client):
    """
    Tests that the client can handle searching for errors that don't exist.
    """

    all_errors = librarian_client.search_errors(id=-1)

    assert len(all_errors) == 0

    # Try to clear it

    with pytest.raises(ValueError):
        cleared_error = librarian_client.clear_error(-1)
