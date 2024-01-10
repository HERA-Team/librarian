"""
Fixtures for integration testing of the servers and client.
"""

import json
import os
import random
import shutil
import socket
import subprocess
import sys
from pathlib import Path
from socket import gethostname
from subprocess import run

import pytest
from pydantic import BaseModel
from xprocess import ProcessStarter

from hera_librarian import LibrarianClient

from ..server import Server, run_background_tasks, server_setup

DATABASE_PATH = None
SERVER_LOG_PATH = None


@pytest.fixture(scope="package")
def server(xprocess, tmp_path_factory, request):
    """
    Starts a single server with pytest-xprocess.
    """

    setup = server_setup(tmp_path_factory)

    class Starter(ProcessStarter):
        pattern = "Uvicorn running on"
        args = [sys.executable, shutil.which("librarian-server-start")]
        timeout = 10
        env = setup.env

    raise Exception([sys.executable, shutil.which("librarian-server-start")])

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

    client = LibrarianClient(
        conn_name="test",
        conn_config={
            "url": f"http://localhost:{server.id}/",
            "authenticator": None,
        },
    )

    yield client

    del client
