"""
Rebuild the librarian database from scratch. Note that you WILL
lose all:

- Remote instances
- Checksums
- User information
"""

import argparse as ap

parser = ap.ArgumentParser(
    description=(
        "Rebuild the librarian database. If you don't know what you're doing, "
        "absolutely under no circumstances should you ever run this script. "
        "This script only works with local stores."
    )
)

parser.add_argument(
    "--store",
    help="Name of the store to re-build.",
    type=str,
    required=True,
)

parser.add_argument(
    "--directories",
    help=(
        "Treat all potential files to ingest at the final directory level, "
        "and NOT the file level."
    ),
    action="store_true",
)

parser.add_argument(
    "--i-know-what-i-am-doing",
    help=(
        "If you use this flag, it will skip the text input that we require at "
        "the start. Useful for testing. Don't use this in practice. Even if "
        "you know what you're doing."
    ),
    action="store_true",
)

import os
from pathlib import Path

try:
    from tqdm import tqdm
except (ModuleNotFoundError, ImportError):
    tqdm = lambda x: x

from hera_librarian.deletion import DeletionPolicy
from librarian_server.database import get_session
from librarian_server.orm import File, Instance, StoreMetadata
from librarian_server.stores import LocalStore, StoreNames


def get_store(store_name: str) -> StoreMetadata:
    with get_session() as session:
        possible_store = session.filter_by(name=store_name).first()

    if possible_store is None:
        print(f"Unable to find store {store_name} in database.")
        exit(1)

    possible_store: StoreMetadata

    if possible_store.store_type != StoreNames["local"]:
        print(f"Store {store_name} is not a local store.")
        print("Read the code and figure out what it does before continuing.")
        exit(1)

    return possible_store


def get_file_list_from_top_level(top_level: Path, directory: bool) -> dict[str, Path]:
    all_unique_files: dict[str, Path] = {}

    for dir, subdirs, files in os.walk(top_level):
        if directory:
            if subdirs == []:
                # No more subdirs. This is the dir for me.
                full_directory_path = Path(dir)
                relative_path = str(full_directory_path.relative_to(top_level))

                all_unique_files[relative_path] = full_directory_path
        else:
            if len(files) > 0:
                for file in files:
                    full_file_path = Path(dir) / file
                    relative_path = str(full_file_path.relative_to(top_level))

                    all_unique_files[relative_path] = full_file_path

    return all_unique_files


def get_file_list(store: StoreMetadata, directory: bool) -> dict[str, Path]:
    """
    Starts at the top level of the store and iterates through to find
    the bottom level files to ingest into the database.

    If directory is set, we stop at the bottom directory level. Otherwise,
    we ingest each file individually.
    """

    store_manager: LocalStore = store.store_manager

    store_path = store_manager._resolved_path_store(".")

    return get_file_list_from_top_level(top_level=store_path, directory=directory)


def ingest_files(file_list: dict[str, Path], store_name: str):
    with get_session() as session:
        store = session.filter_by(name=store_name).first()
        store_manager: LocalStore = store.store_manager

        for file_name, file_path in tqdm(file_list.items(), desc="Ingesting files"):
            file_path_info = store_manager.path_info(path=file_path)

            file = File.new_file(
                filename=file_name,
                size=file_path_info.size,
                checksum=file_path_info.checksum,
                uploader="unknown",
                source="unknown",
            )

            instance = Instance.new_instance(
                path=file_path,
                file=file,
                store=store,
                deletion_policy=DeletionPolicy.DISALLOWED,
            )

            session.add_all([file, instance])
            session.commit()

    return


def run_migration(store_name: str, directory: bool):
    store = get_store(store_name=store_name)
    file_list = get_file_list(store=store, directory=directory)
    ingest_files(file_list=file_list, store_name=store_name)


args = parser.parse_args()


def main():
    if not args.i_know_what_i_am_doing:
        input(
            "Have you read the entirety of this script, or did you write it? (Yes/No)"
        )

        if input != "Yes":
            print("Thank you. Pleaase read the script before continuing.")
            exit(0)

        input(
            "Do you understand that this script can potentially "
            "corrupt your entire librarian database? (Yes/No)"
        )

        if input != "Yes":
            print("Thank you. Please read the script before continuing.")
            exit(0)

        input(f"Are you ready to migrate the store: {parser.store}? (Yes/No)")

        if input != "Yes":
            print(
                "Thank you. Please either return once you have prepared your "
                "mind, body, and soul for this potentially damaging experience."
            )
            exit(0)

    run_migration(store_name=args.store_name, directory=args.directory)
