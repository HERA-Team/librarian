"""
Shared fixtures amongst all tests.
"""

import datetime
import hashlib
import json
import os
import random
import shutil
import sys
from pathlib import Path
from subprocess import run

import pytest

from hera_librarian.client import AdminClient
from hera_librarian.errors import ErrorCategory, ErrorSeverity
from hera_librarian.exceptions import LibrarianHTTPError

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
    try:
        path.unlink()
    except FileNotFoundError:
        pass


@pytest.fixture
def garbage_filename() -> Path:
    """
    Returns a random valid filename.
    """

    yield Path(f"garbage_file_{random.randint(0, 1000000)}.txt")


DATABASE_PATH = None


@pytest.fixture(scope="session")
def test_server(tmp_path_factory):
    """
    Starts a single 'server' using the test client.
    """

    setup = server_setup(tmp_path_factory, name="test_server")

    env_vars = {x: None for x in setup.env.keys()}

    for env_var in list(env_vars.keys()):
        env_vars[env_var] = os.environ.get(env_var, None)
        if setup.env[env_var] is not None:
            os.environ[env_var] = setup.env[env_var]

    global DATABASE_PATH
    DATABASE_PATH = str(setup.database)

    # Before starting, create the DB schema
    run([sys.executable, shutil.which("librarian-server-setup")], env=setup.env)

    import librarian_server
    import librarian_server.database

    app = librarian_server.main()
    get_session = librarian_server.database.get_session

    yield app, get_session, setup

    for env_var in list(env_vars.keys()):
        if env_vars[env_var] is None:
            del os.environ[env_var]
        else:
            os.environ[env_var] = env_vars[env_var]


@pytest.fixture(scope="package")
def test_client(test_server):
    """
    Returns a test client for the server.
    """

    app, session, setup = test_server

    from fastapi.testclient import TestClient

    client = TestClient(app)

    def client_post_with_auth(endpoint: str, content=None, **kwargs):
        """
        A request with the test client that includes the auth header.
        """

        if "auth" not in kwargs:
            kwargs["auth"] = ("admin", "password")

        if "headers" not in kwargs:
            kwargs["headers"] = {"Content-Type": "application/json"}

        return client.post(
            endpoint,
            content=content,
            **kwargs,
        )

    client.post_with_auth = client_post_with_auth

    yield client

    del client


@pytest.fixture(scope="package")
def mocked_admin_client(test_client):
    """
    Returns an instance of AdminClient that is actually mocked
    to use the test client.
    """

    client = AdminClient(
        host=str(test_client.base_url),
        port=80,
        user="admin",
        password="password",
    )

    # Now need to replace post

    def new_post(endpoint: str, request, response=None):
        endpoint = f"/api/v2/{endpoint}"
        r = test_client.post_with_auth(endpoint, content=request.model_dump_json())

        if str(r.status_code)[0] != "2":
            try:
                json = r.json()
            except:
                json = {}

            raise LibrarianHTTPError(
                url=endpoint,
                status_code=r.status_code,
                reason=json.get("reason", "<no reason provided>"),
                suggested_remedy=json.get(
                    "suggested_remedy", "<no suggested remedy provided>"
                ),
            )

        if response is not None:
            return response.model_validate_json(r.content)

        return r

    client.post = new_post

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

    session = test_server[1]()

    store = session.query(test_orm.StoreMetadata).first()

    data = random.randbytes(1024)

    file = test_orm.File.new_file(
        filename="example_file.txt",
        size=len(data),
        # Leave as-is to test auto-selection of md5
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

    session.add_all([file, instance])
    session.commit()

    instance_id = instance.id

    session.close()

    yield test_server

    # Now delete those items from the database.

    session = test_server[1]()

    instance = session.get(test_orm.Instance, instance_id)
    file = session.get(test_orm.File, "example_file.txt")

    file.delete(session=session, commit=False, force=True)

    session.commit()
    session.close()


@pytest.fixture(scope="function")
def test_server_with_invalid_file(test_server, test_orm):
    """
    Test server with a invalid file and instance in the store.
    """

    session = test_server[1]()

    store = session.query(test_orm.StoreMetadata).first()

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

    session.add_all([file, instance])

    session.commit()

    instance_id = instance.id

    session.close()

    yield test_server

    # Now delete those items from the database.

    session = test_server[1]()

    instance = session.get(test_orm.Instance, instance_id)
    file = session.get(test_orm.File, "example_file.txt")

    file.delete(session=session, commit=False, force=True)

    session.commit()
    session.close()


@pytest.fixture(scope="function")
def test_server_with_missing_file(test_server, test_orm):
    """
    Test server with a missing file and instance in the store.
    """

    session = test_server[1]()

    store = session.query(test_orm.StoreMetadata).first()

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

    session.add_all([file, instance])

    session.commit()

    instance_id = instance.id

    session.close()

    yield test_server

    # Now delete those items from the database.

    session = test_server[1]()

    instance = session.get(test_orm.Instance, instance_id)
    file = session.get(test_orm.File, "example_file.txt")

    file.delete(session=session, commit=False, force=True)

    session.commit()
    session.close()


@pytest.fixture(scope="function")
def test_server_with_many_files_and_errors(test_server, test_orm):
    """
    Test server with many valid files and some errors in the database.
    """

    session = test_server[1]()

    store = session.query(test_orm.StoreMetadata).first()

    data = random.randbytes(1024)

    # Add many files
    file_names = [f"test_files/many_server_example_file_{x}.txt" for x in range(64)]
    file_names += [f"many_server_example_file_{x}.txt" for x in range(64)]

    # Create the folder in the store
    store.store_manager._resolved_path_store(Path("test_files")).mkdir()

    for file_name in file_names:
        file = test_orm.File.new_file(
            filename=file_name,
            size=len(data),
            # Leave as-is to test auto-selection of md5
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

        session.add_all([file, instance])

    session.commit()

    # Add errors
    error_ids = []

    for x in range(128):
        error = test_orm.Error.new_error(
            severity=random.choice([e for e in ErrorSeverity]),
            category=random.choice([e for e in ErrorCategory]),
            message=f"Test error {x}",
            caller="test",
        )

        if x % 2:
            error.cleared = True
            error.cleared_time = datetime.datetime.now(datetime.timezone.utc)

        session.add(error)
        session.commit()

        error_ids.append(error.id)

    session.close()

    yield test_server

    # Now delete those items from the database.
    session = test_server[1]()

    for file_name in file_names:
        file = session.get(test_orm.File, file_name)
        file.delete(session=session, commit=False, force=True)

    for error_id in error_ids:
        error = session.get(test_orm.Error, error_id)

        session.delete(error)

    session.commit()
    session.close()
