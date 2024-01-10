"""
"Server" object and generation function.
"""

import json
import os
import random
import shutil
import socket
import sys
from pathlib import Path
from subprocess import run

from pydantic import BaseModel


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

    @property
    def env(self) -> dict[str, str]:
        return {
            "LIBRARIAN_CONFIG_PATH": self.LIBRARIAN_CONFIG_PATH,
            "SQLALCHEMY_DATABASE_URI": self.SQLALCHEMY_DATABASE_URI,
            "PORT": self.PORT,
            "ADD_STORES": self.ADD_STORES,
            "ALEMBIC_CONFIG_PATH": str(Path(__file__).parent.parent),
            "ALEMBIC_PATH": shutil.which("alembic"),
        }


def server_setup(tmp_path_factory) -> Server:
    """
    Sets up a server.
    """

    librarian_config_path = str(Path("./tests/mock_config.json").resolve())

    server_id_and_port = random.randint(1000, 20000)

    # Check if the port is available. If not, increment until it is.
    while (
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect_ex(
            (socket.gethostname(), server_id_and_port)
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

    staging_directory_clone = tmp_path / f"staging_clone_{server_id_and_port}"
    staging_directory_clone.mkdir()

    store_directory_clone = tmp_path / f"store_clone_{server_id_and_port}"
    store_directory_clone.mkdir()

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
                    "hostname": socket.gethostname(),
                }
            },
        },
        {
            "store_name": "test_store_clone",
            "store_type": "local",
            "ingestable": False,
            "store_data": {
                "staging_path": str(staging_directory_clone),
                "store_path": str(store_directory_clone),
            },
            "transfer_manager_data": {
                "local": {
                    "available": "true",
                    "hostname": socket.gethostname(),
                }
            },
        },
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


def run_background_tasks(server: Server) -> int:
    """
    Runs all the background tasks for the given server setup.

    You must have already added all the stores, etc. that are required.
    """

    return run(
        [sys.executable, shutil.which("librarian-background-only"), "--once"],
        env=server.env,
    ).returncode
