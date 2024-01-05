"""
Unit testing conftest. Contains fixtures for the librarian server.
We can't just import app and session from our main __init__.py file
because they contain state that depends on configuraiton variables...
Ugh.
"""

import pytest
import os
from pathlib import Path
from pydantic import BaseModel
import json
import random
from socket import gethostname
import socket


class Server(BaseModel):
    id: int
    base_path: Path
    staging_directory: Path
    store_directory: Path
    database: Path
    LIBRARIAN_CONFIG_PATH: str
    SQLALCHEMY_DATABASE_URI: str
    PORT: str
    ADD_STORES: str
    process: str | None = None


def server_setup(tmp_path_factory) -> Server:
    """
    Sets up a server.
    """

    librarian_config_path = str(Path("./integrationtest/mock_config.json").resolve())

    server_id_and_port = random.randint(1000, 20000)

    # Check if the port is available. If not, increment until it is.
    while (
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect_ex(
            (gethostname(), server_id_and_port)
        )
        == 0
    ):
        server_id_and_port += 1

    tmp_path = tmp_path_factory.mktemp(f"server_{server_id_and_port}")

    database = tmp_path / f"database_{server_id_and_port}.sqlite"

    # Create the other server settings
    staging_directory = tmp_path / f"staging_{server_id_and_port}"
    staging_directory.mkdir()

    store_directory = tmp_path / f"store_{server_id_and_port}"
    store_directory.mkdir()

    store_config = [
        {
            "store_name": "test_store",
            "store_type": "local",
            "ingestable": True,
            "store_data": {
                "staging_path": str(staging_directory),
                "store_path": str(store_directory),
            },
            "transfer_manager_data": {
                "local": {
                    "available": "true",
                    "hostname": gethostname(),
                }
            },
        }
    ]

    add_stores = json.dumps(store_config)

    return Server(
        id=server_id_and_port,
        base_path=tmp_path,
        staging_directory=staging_directory,
        store_directory=store_directory,
        database=database,
        LIBRARIAN_CONFIG_PATH=librarian_config_path,
        SQLALCHEMY_DATABASE_URI=f"sqlite:///{database}",
        PORT=str(server_id_and_port),
        ADD_STORES=add_stores,
    )


@pytest.fixture(scope="session")
def server(tmp_path_factory):
    """
    Starts a single server with pytest-xprocess.
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

    from librarian_server import app, session

    yield app, session, setup

    for env_var in list(env_vars.keys()):
        if env_vars[env_var] is None:
            del os.environ[env_var]
        else:
            os.environ[env_var] = env_vars[env_var]


@pytest.fixture(scope="session")
def client(server):
    """
    Returns a test client for the server.
    """

    app, session, setup = server

    from fastapi.testclient import TestClient

    client = TestClient(app)

    yield client

    del client
