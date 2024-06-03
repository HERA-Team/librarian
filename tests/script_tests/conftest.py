"""
Fixtures for testing librarian scripts.
"""

import os

import pytest

from ..server import Server, server_setup

DATABASE_PATH = None


@pytest.fixture
def test_database_reconstruction_server(tmp_path_factory):
    setup = server_setup(tmp_path_factory)

    env_vars = {
        "LIBRARIAN_CONFIG_PATH": None,
        "LIBRARIAN_SERVER_DATABASE_DRIVER": None,
        "LIBRARIAN_SERVER_DATABASE": None,
        "LIBRARIAN_SERVER_PORT": None,
        "LIBRARIAN_SERVER_ADD_STORES": None,
    }

    for env_var in list(env_vars.keys()):
        env_vars[env_var] = os.environ.get(env_var, None)
        os.environ[env_var] = getattr(setup, env_var)

    import librarian_server
    import librarian_server.database
    import librarian_server.orm as orm

    get_session = librarian_server.database.get_session

    global DATABASE_PATH
    DATABASE_PATH = setup.database

    yield setup, get_session, orm


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    terminalreporter.section("script unit test temporary files")
    terminalreporter.write_line(
        "\033[1m" + "Database: " + "\033[0m" + str(DATABASE_PATH)
    )
