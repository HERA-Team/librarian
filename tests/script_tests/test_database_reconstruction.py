"""
Tests we are able to reconstruct a database.
"""

import shutil
import subprocess
import sys

from librarian_server_scripts.librarian_server_rebuild_database import run_migration


def test_database_reconstruction(test_database_reconstruction_server):
    setup, get_session, orm = test_database_reconstruction_server

    # Generate some files in the store directory.
    file_paths = [
        "test_A/test_G",
        "test_A/test_D",
        "test_A/test_B/test_E",
        "test_A/test_B/test_C/test_F",
    ]

    for file_path in file_paths:
        full_path = setup.store_directory / file_path
        full_path.mkdir(parents=True)
        (full_path / "test.txt").touch()

    subprocess.run(
        [
            sys.executable,
            shutil.which("librarian-server-rebuild-database"),
            "--directories",
            f"--store=local_store",
            "--directory",
        ],
        env=setup.env,
    )

    run_migration(store_name="local_store", directory=True)

    # Now check we ingested all those files.

    with get_session() as session:
        for file_path in file_paths:
            assert session.get(orm.File, file_path)
