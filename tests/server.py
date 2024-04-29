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

from cryptography.fernet import Fernet
from pydantic import BaseModel


class Server(BaseModel):
    id: int
    base_path: Path
    staging_directory: Path
    store_directory: Path
    database: Path
    LIBRARIAN_SERVER_NAME: str
    LIBRARIAN_SERVER_DISPLAYED_SITE_NAME: str
    LIBRARIAN_CONFIG_PATH: str
    LIBRARIAN_SERVER_MAXIMAL_UPLOAD_SIZE_BYTES: int
    LIBRARIAN_SERVER_DATABASE_DRIVER: str
    LIBRARIAN_SERVER_ENCRYPTION_KEY: str
    LIBRARIAN_SERVER_DATABASE: str
    LIBRARIAN_SERVER_PORT: str
    LIBRARIAN_SERVER_ADD_STORES: str
    LIBRARIAN_BACKGROUND_CHECK_INTEGRITY: str
    LIBRARIAN_BACKGROUND_CREATE_LOCAL_CLONE: str
    LIBRARIAN_BACKGROUND_SEND_CLONE: str
    LIBRARIAN_BACKGROUND_RECIEVE_CLONE: str
    process: str | None = None

    @property
    def env(self) -> dict[str, str]:
        return {
            "LIBRARIAN_SERVER_NAME": self.LIBRARIAN_SERVER_NAME,
            "LIBRARIAN_SERVER_DISPLAYED_SITE_NAME": self.LIBRARIAN_SERVER_DISPLAYED_SITE_NAME,
            "LIBRARIAN_CONFIG_PATH": self.LIBRARIAN_CONFIG_PATH,
            "LIBRARIAN_SERVER_MAXIMAL_UPLOAD_SIZE_BYTES": str(
                self.LIBRARIAN_SERVER_MAXIMAL_UPLOAD_SIZE_BYTES
            ),
            "LIBRARIAN_SERVER_ENCRYPTION_KEY": self.LIBRARIAN_SERVER_ENCRYPTION_KEY,
            "LIBRARIAN_SERVER_DATABASE_DRIVER": self.LIBRARIAN_SERVER_DATABASE_DRIVER,
            "LIBRARIAN_SERVER_DATABASE": self.LIBRARIAN_SERVER_DATABASE,
            "LIBRARIAN_SERVER_PORT": self.LIBRARIAN_SERVER_PORT,
            "LIBRARIAN_SERVER_ADD_STORES": self.LIBRARIAN_SERVER_ADD_STORES,
            "LIBRARIAN_SERVER_ALEMBIC_CONFIG_PATH": str(Path(__file__).parent.parent),
            "LIBRARIAN_SERVER_ALEMBIC_PATH": shutil.which("alembic"),
            "LIBRARIAN_BACKGROUND_CHECK_INTEGRITY": self.LIBRARIAN_BACKGROUND_CHECK_INTEGRITY,
            "LIBRARIAN_BACKGROUND_CREATE_LOCAL_CLONE": self.LIBRARIAN_BACKGROUND_CREATE_LOCAL_CLONE,
            "LIBRARIAN_BACKGROUND_SEND_CLONE": self.LIBRARIAN_BACKGROUND_SEND_CLONE,
            "LIBRARIAN_BACKGROUND_RECIEVE_CLONE": self.LIBRARIAN_BACKGROUND_RECIEVE_CLONE,
        }


def server_setup(tmp_path_factory, name="librarian_server") -> Server:
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

    staging_directory_empty = tmp_path / f"staging_empty_{server_id_and_port}"
    staging_directory_empty.mkdir()

    store_directory_empty = tmp_path / f"store_empty_{server_id_and_port}"
    store_directory_empty.mkdir()

    staging_directory_sneaker = tmp_path / f"staging_sneaker_{server_id_and_port}"
    staging_directory_sneaker.mkdir()

    store_directory_sneaker = tmp_path / f"store_sneaker_{server_id_and_port}"
    store_directory_sneaker.mkdir()

    store_config = [
        {
            "store_name": "local_store",
            "store_type": "local",
            "ingestable": True,
            "store_data": {
                "staging_path": str(staging_directory),
                "store_path": str(store_directory),
            },
            "transfer_manager_data": {
                "local": {
                    "available": "true",
                    "hostnames": [socket.gethostname()],
                }
            },
            "async_transfer_manager_data": {
                "local": {
                    "available": "true",
                    "hostnames": [socket.gethostname()],
                }
            },
        },
        {
            "store_name": "local_clone",
            "store_type": "local",
            "ingestable": False,
            "store_data": {
                "staging_path": str(staging_directory_clone),
                "store_path": str(store_directory_clone),
            },
            "transfer_manager_data": {
                "local": {
                    "available": "true",
                    "hostnames": [socket.gethostname()],
                }
            },
        },
        {
            # A store that will _always_ report as empty!
            "store_name": "local_empty",
            "store_type": "local",
            "ingestable": False,
            "store_data": {
                "staging_path": str(staging_directory_empty),
                "store_path": str(store_directory_empty),
                "report_full_fraction": 0.0,
            },
            "transfer_manager_data": {
                "local": {
                    "available": "true",
                    "hostnames": [socket.gethostname()],
                }
            },
        },
        {
            # A store that we will use for sneaker transfers.
            "store_name": "local_sneaker",
            "store_type": "local",
            "ingestable": False,
            "store_data": {
                "staging_path": str(staging_directory_sneaker),
                "store_path": str(store_directory_sneaker),
                "report_full_fraction": 0.9,
            },
            "transfer_manager_data": {
                "local": {
                    "available": "true",
                    "hostnames": [socket.gethostname()],
                }
            },
        },
    ]

    add_stores = json.dumps(store_config)

    check_integrity = json.dumps(
        [
            {
                "task_name": "check",
                "every": "00:01:00",
                "age_in_days": 7,
                "store_name": "local_store",
            }
        ]
    )

    create_local_clone = json.dumps(
        [
            {
                "task_name": "clone",
                "every": "00:01:00",
                "age_in_days": 7,
                "clone_from": "local_store",
                "clone_to": "local_clone",
            }
        ]
    )

    return Server(
        id=server_id_and_port,
        base_path=tmp_path,
        staging_directory=staging_directory,
        store_directory=store_directory,
        database=database,
        LIBRARIAN_SERVER_NAME=name,
        LIBRARIAN_SERVER_DISPLAYED_SITE_NAME=name.replace("_", " ").title(),
        LIBRARIAN_SERVER_ENCRYPTION_KEY=Fernet.generate_key().decode(),
        LIBRARIAN_SERVER_MAXIMAL_UPLOAD_SIZE_BYTES=1_000_000,  # 1 MB for testing
        LIBRARIAN_CONFIG_PATH=librarian_config_path,
        LIBRARIAN_SERVER_DATABASE_DRIVER="sqlite",
        LIBRARIAN_SERVER_DATABASE=str(database),
        LIBRARIAN_SERVER_PORT=str(server_id_and_port),
        LIBRARIAN_SERVER_ADD_STORES=add_stores,
        LIBRARIAN_BACKGROUND_CHECK_INTEGRITY=check_integrity,
        LIBRARIAN_BACKGROUND_CREATE_LOCAL_CLONE=create_local_clone,
        LIBRARIAN_BACKGROUND_SEND_CLONE="[]",
        LIBRARIAN_BACKGROUND_RECIEVE_CLONE="[]",
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
