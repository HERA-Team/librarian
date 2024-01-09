"""
Shared fixtures amongst all tests.
"""

import json
import os
import random
from pathlib import Path
from subprocess import run

import pytest

from .server import Server, server_setup


@pytest.fixture
def garbage_file(tmp_path) -> Path:
    """
    Returns a file filled with garbage at the path.
    """

    data = random.randbytes(1024)

    path = tmp_path / "garbage_file.txt"

    with open(path, "wb") as handle:
        handle.write(data)

    yield path

    # Delete the file for good measure.
    path.unlink()


@pytest.fixture
def garbage_filename() -> Path:
    """
    Returns a random valid filename.
    """

    yield Path(f"garbage_file_{random.randint(0, 1000000)}.txt")


DATABASE_PATH = None


@pytest.fixture(scope="package")
def test_server(tmp_path_factory):
    """
    Starts a single 'server' using the test client.
    """

    setup = server_setup(tmp_path_factory)

    env_vars = {
        "LIBRARIAN_CONFIG_PATH": None,
        "SQLALCHEMY_DATABASE_URI": None,
        "PORT": None,
        "ADD_STORES": None,
    }

    for env_var in list(env_vars.keys()):
        env_vars[env_var] = os.environ.get(env_var, None)
        os.environ[env_var] = getattr(setup, env_var)

    global DATABASE_PATH
    DATABASE_PATH = str(setup.database)

    # Before starting, create the DB schema
    run(["alembic", "upgrade", "head"])

    import importlib

    import librarian_server

    importlib.reload(librarian_server)

    app = librarian_server.app
    session = librarian_server.session

    # Need to add our stores...
    from librarian_server.orm import StoreMetadata
    from librarian_server.settings import StoreSettings

    for store_config in json.loads(setup.ADD_STORES):
        store_config = StoreSettings(**store_config)

        store = StoreMetadata(
            name=store_config.store_name,
            store_type=store_config.store_type,
            ingestable=store_config.ingestable,
            store_data={**store_config.store_data, "name": store_config.store_name},
            transfer_manager_data=store_config.transfer_manager_data,
        )

        session.add(store)

    session.commit()

    yield app, session, setup

    for env_var in list(env_vars.keys()):
        if env_vars[env_var] is None:
            del os.environ[env_var]
        else:
            os.environ[env_var] = env_vars[env_var]

    session.close()


@pytest.fixture(scope="package")
def test_client(test_server):
    """
    Returns a test client for the server.
    """

    app, session, setup = test_server

    from fastapi.testclient import TestClient

    client = TestClient(app)

    yield client

    del client


@pytest.fixture(scope="package")
def test_orm(test_server):
    """
    Returns the ORM for this server. You have to use this
    instead of directly importing because of the dependence
    on the global settings variable.
    """

    from librarian_server import orm

    yield orm


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    terminalreporter.section("server unit test temporary files")
    terminalreporter.write_line(
        "\033[1m" + "Database: " + "\033[0m" + str(DATABASE_PATH)
    )
