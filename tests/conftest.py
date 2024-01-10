"""
Shared fixtures amongst all tests.
"""

import hashlib
import json
import os
import random
import shutil
from pathlib import Path
from subprocess import run

import pytest

from hera_librarian.utils import get_md5_from_path, get_size_from_path

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


@pytest.fixture(scope="function")
def test_server_with_valid_file(test_server, test_orm):
    """
    Test server with a valid file and instance in the store.
    """

    store = test_server[1].query(test_orm.StoreMetadata).first()

    data = random.randbytes(1024)

    file = test_orm.File.new_file(
        filename="example_file.txt",
        size=len(data),
        checksum=hashlib.md5(data).hexdigest(),
        uploader="test",
        source="test",
    )

    # Create the file in the store
    path = store.store_manager._resolved_path_store(Path(file.name))

    with open(path, "wb") as handle:
        handle.write(data)

    instance = test_orm.Instance.new_instance(
        path=path,
        file=file,
        store=store,
        deletion_policy="ALLOWED",
    )

    test_server[1].add_all([file, instance])

    test_server[1].commit()

    yield test_server

    # Now delete those items from the database.

    test_server[1].delete(instance)
    test_server[1].delete(file)

    test_server[1].commit()

    path.unlink()


@pytest.fixture(scope="function")
def test_server_with_invalid_file(test_server, test_orm):
    """
    Test server with a invalid file and instance in the store.
    """

    store = test_server[1].query(test_orm.StoreMetadata).first()

    data = random.randbytes(1024)

    file = test_orm.File.new_file(
        filename="example_file.txt",
        size=len(data),
        checksum="not_the_checksum",
        uploader="test",
        source="test",
    )

    # Create the file in the store
    path = store.store_manager._resolved_path_store(Path(file.name))

    with open(path, "wb") as handle:
        handle.write(data)

    instance = test_orm.Instance.new_instance(
        path=path,
        file=file,
        store=store,
        deletion_policy="ALLOWED",
    )

    test_server[1].add_all([file, instance])

    test_server[1].commit()

    yield test_server

    # Now delete those items from the database.

    test_server[1].delete(instance)
    test_server[1].delete(file)

    test_server[1].commit()

    path.unlink()


@pytest.fixture(scope="function")
def test_server_with_missing_file(test_server, test_orm):
    """
    Test server with a missing file and instance in the store.
    """

    store = test_server[1].query(test_orm.StoreMetadata).first()

    data = random.randbytes(1024)

    file = test_orm.File.new_file(
        filename="example_file.txt",
        size=len(data),
        checksum="not_the_checksum",
        uploader="test",
        source="test",
    )

    # Don't! Create the file in the store
    path = store.store_manager._resolved_path_store(Path(file.name))

    # I.e. these are purposefully commented out!
    # with open(path, "wb") as handle:
    #     handle.write(data)

    instance = test_orm.Instance.new_instance(
        path=path,
        file=file,
        store=store,
        deletion_policy="ALLOWED",
    )

    test_server[1].add_all([file, instance])

    test_server[1].commit()

    yield test_server

    # Now delete those items from the database.

    test_server[1].delete(instance)
    test_server[1].delete(file)

    test_server[1].commit()
