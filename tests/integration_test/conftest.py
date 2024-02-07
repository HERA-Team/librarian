"""
Fixtures for integration testing of the servers and client.
"""

import json
import os
import shutil
import sys

import pytest
from sqlalchemy import URL
from xprocess import ProcessStarter

from hera_librarian import LibrarianClient

from ..server import Server, run_background_tasks, server_setup

DATABASE_PATH = None
SERVER_LOG_PATH = None


@pytest.fixture(scope="package")
def server(xprocess, tmp_path_factory, request) -> Server:
    """
    Starts a single server with pytest-xprocess.
    """

    setup = server_setup(tmp_path_factory)

    class Starter(ProcessStarter):
        pattern = "Uvicorn running on"
        args = [sys.executable, shutil.which("librarian-server-start"), "--setup"]
        timeout = 10
        env = setup.env

    for label, key in setup.env.items():
        if key is None:
            raise ValueError(f"Environment variable {label} is None.")

    xprocess.ensure("server", Starter)

    setup.process = "server"
    yield setup

    global DATABASE_PATH, SERVER_LOG_PATH
    DATABASE_PATH = str(setup.database)
    SERVER_LOG_PATH = str(xprocess.getinfo("server").logpath)

    # Before terminating, let's make sure to run the background tasks at
    # least once!

    assert run_background_tasks(setup) == 0

    xprocess.getinfo("server").terminate()


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    terminalreporter.section("integration test temporary files")
    terminalreporter.write_line(
        "\033[1m" + "Server log: " + "\033[0m" + str(SERVER_LOG_PATH)
    )
    terminalreporter.write_line(
        "\033[1m" + "Database: " + "\033[0m" + str(DATABASE_PATH)
    )


@pytest.fixture
def librarian_client(server) -> LibrarianClient:
    """
    Returns a LibrarianClient connected to the server.
    """

    connections = json.dumps(
        {
            "test-A": {
                "user": "admin",
                "port": server.id,
                "host": "http://localhost",
                "password": "password",
            }
        }
    )

    os.environ["LIBRARIAN_CLIENT_CONNECTIONS"] = connections

    client = LibrarianClient(
        host="http://localhost",
        port=server.id,
        user="admin",
        password="password",
    )

    yield client

    del client


@pytest.fixture
def librarian_client_command_line(server):
    """
    Sets up the required environment variables for the command line client.
    """

    connections = json.dumps(
        {
            "test-A": {
                "user": "admin",
                "port": server.id,
                "host": "http://localhost",
                "password": "password",
            }
        }
    )

    os.environ["LIBRARIAN_CLIENT_CONNECTIONS"] = connections

    yield "test-A"


@pytest.fixture(scope="package")
def librarian_database_session_maker(server: Server):
    """
    Generates a session maker for the database for the librarian
    running in the other process. Use this to make database changes
    behind the librarian's back (sparingly!).

    If using this, ask yourself if there should be a client API
    endpoint for this instead.
    """

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(
        URL.create(
            server.LIBRARIAN_SERVER_DATABASE_DRIVER,
            database=server.LIBRARIAN_SERVER_DATABASE,
        ),
        connect_args={"check_same_thread": False},
    )

    SessionMaker = sessionmaker(bind=engine, autocommit=False, autoflush=False)

    yield SessionMaker

    del SessionMaker

    engine.dispose()
