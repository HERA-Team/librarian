"""
Tests that the client can handle communicating with the librarian about errors correctly.
"""

import subprocess

import pytest

from hera_librarian.errors import ErrorCategory, ErrorSeverity


@pytest.fixture(scope="function")
def server_with_fake_errors(server, test_orm, librarian_database_session_maker):
    """
    Starts a server with a fake error.
    """

    error_ids = []

    with librarian_database_session_maker() as session:
        for error in range(32):
            error = test_orm.Error.new_error(
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.CONFIGURATION,
                message="This is a fake error.",
            )

            session.add(error)
            session.commit()

            error_ids.append(error.id)

    yield server

    with librarian_database_session_maker() as session:
        for error_id in error_ids:
            error = session.get(test_orm.Error, error_id)
            session.delete(error)

        session.commit()


def test_error_search(server_with_fake_errors, admin_client):
    """
    Tests that the client can search for errors correctly.
    """

    all_errors = admin_client.search_errors()

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

    cleared_error = admin_client.clear_error(error_to_clear)

    all_errors = admin_client.search_errors(id=error_to_clear, include_resolved=True)

    assert all_errors[0].cleared

    # Check what happens if we clear it again

    with pytest.raises(ValueError):
        cleared_error = admin_client.clear_error(error_to_clear)


def test_error_search_missing(server_with_fake_errors, admin_client):
    """
    Tests that the client can handle searching for errors that don't exist.
    """

    all_errors = admin_client.search_errors(id=-1)

    assert len(all_errors) == 0

    # Try to clear it

    with pytest.raises(ValueError):
        cleared_error = admin_client.clear_error(-1)


def test_error_search_cli_path(server_with_fake_errors, librarian_client_command_line):
    """
    Tests that the CLI can search for errors correctly.
    """

    captured = subprocess.check_output(
        [
            "librarian",
            "search-errors",
            librarian_client_command_line,
            "--id=30",
            "--include-resolved",
        ]
    )

    assert "This is a fake error." in str(captured)

    captured = subprocess.check_output(
        [
            "librarian",
            "clear-error",
            librarian_client_command_line,
            "30",
        ]
    )

    assert captured == b""

    captured = subprocess.check_output(
        [
            "librarian",
            "search-errors",
            librarian_client_command_line,
            "--include-resolved",
        ]
    )

    assert "True" in str(captured)
