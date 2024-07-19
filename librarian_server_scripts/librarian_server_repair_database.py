"""
Repairs a downstream librarian database by using remote instance and
outgoing transfer data from an upstream librarian.

This script is best used when your downstream database is missing
information. You should recover it from a backup, and then use this
to 'migrate' any missing items.

Other steps to take:

1. Stop the upstream librarian from generating new outgoing transfers
   but allow it to complete those in-flight. Do not delete hanging
   staging directories until the transfers have all 'gone through'.
2. Review your backup schedule.
"""

# This script recreates three major sets of rows:
# 1. The file rows
# 2. The instance rows
# 3. The IncomingTransfer rows
#
# The first two are generated from RemoteInstances on the source
# and the latter is re-created from the OutgoingTransfer rows on
# the source.


import argparse as ap
import datetime
import json
import sys
from pathlib import Path

from pydantic import BaseModel
from sqlalchemy import select

from hera_librarian.deletion import DeletionPolicy
from hera_librarian.transfer import TransferStatus
from hera_librarian.utils import get_md5_from_path, get_size_from_path
from librarian_server.orm import (
    File,
    IncomingTransfer,
    Instance,
    Librarian,
    OutgoingTransfer,
    RemoteInstance,
    StoreMetadata,
)

parser = ap.ArgumentParser(
    description=(
        "Repair the librarian database. This script runs in two modes: source, "
        "and destination. At the source, you will produce a file that must be "
        "out-of-band transferred to the destination (or you can use unix pipes...) "
        "and can be ingested using 'destination'"
    )
)

# SOURCE Arguments
parser.add_argument(
    "--source",
    help="Run the script in source mode. Produces a dump of databases",
    action="store_true",
)

parser.add_argument(
    "--librarian-name",
    help="The librarian name that you would like to extract data for",
    type=str,
)

parser.add_argument(
    "--age",
    help="The age (in HOURS) to go back in time and select remote instances "
    "and outbound transfers for.",
    type=float,
)

# DESTINATION Arguments
parser.add_argument(
    "--destination",
    help="Name of the store to re-build",
    action="store_true",
)

parser.add_argument(
    "--store-name",
    help="The name of the store to re-build",
    type=str,
)

parser.add_argument(
    "--spot-check",
    help="The number of files to spot-check, to ensure that they are correct",
    type=int,
    default=32,
)

parser.add_argument(
    "--track-progress",
    help="If set, the script will print out the progress of the ingest",
    action="store_true",
)


class FileInfo(BaseModel):
    name: str
    store_id: int
    copy_time: datetime.datetime
    size: int
    checksum: str
    uploader: str
    source: str

    @classmethod
    def from_file(
        cls, file: File, remote_instance: RemoteInstance, source: str
    ) -> "FileInfo":
        return FileInfo(
            name=file.name,
            store_id=remote_instance.store_id,
            copy_time=remote_instance.copy_time,
            size=file.size,
            checksum=file.checksum,
            uploader=file.uploader,
            source=source,
        )

    def to_file(self, store: StoreMetadata) -> tuple[File, Instance]:
        instance = Instance(
            path=str(store.store_manager.resolve_path_store(Path(self.name))),
            file_name=self.name,
            store=store,
            deletion_policy=DeletionPolicy.DISALLOWED,
            created_time=self.copy_time,
            available=True,
        )

        file = File(
            name=self.name,
            create_time=self.copy_time,
            size=self.size,
            checksum=self.checksum,
            uploader=self.uploader,
            source=self.source,
            instances=[instance],
        )

        return file, instance


class TransferInfo(BaseModel):
    source_id: int
    destination_id: int
    status: TransferStatus
    transfer_size: int
    transfer_checksum: str
    transfer_manager_name: str | None
    start_time: datetime.datetime
    file_name: str
    source: str
    uploader: str
    dest_path: str

    @classmethod
    def from_transfer(
        cls, file: File, outgoing_transfer: OutgoingTransfer, source: str
    ) -> "TransferInfo":
        return TransferInfo(
            source_id=outgoing_transfer.id,
            destination_id=outgoing_transfer.remote_transfer_id,
            status=outgoing_transfer.status,
            transfer_size=outgoing_transfer.transfer_size,
            transfer_checksum=outgoing_transfer.transfer_checksum,
            transfer_manager_name=outgoing_transfer.transfer_manager_name,
            start_time=outgoing_transfer.start_time,
            file_name=outgoing_transfer.file_name,
            source=source,
            uploader=file.uploader,
            dest_path=outgoing_transfer.dest_path,
        )

    def get_uuid(self, store: StoreMetadata) -> str:
        """
        Gets the UUID of the staging area from `dest_path`.
        """
        # OUTGOING stores DEST_PATH which is the FULL PATH
        # including /path/to/top/UUID/file/name.
        # So we need to remove both components of the path.

        destination_path = Path(self.dest_path)
        staging_path = store.store_manager.resolve_path_staging(
            "non_existent_file.txt"
        ).parent

        # Another round of relative_to doesn't work as both paths
        # are relative and aren't anywhere we can see them
        path_under_staging = set(destination_path.relative_to(staging_path).parts)
        file_name = set(Path(self.file_name).parts)

        uuid = path_under_staging.difference(file_name).pop()
        potential_path = store.store_manager.resolve_path_staging(Path(uuid))
        assert potential_path.exists()

        return uuid

    def to_transfer(self, store: StoreMetadata) -> IncomingTransfer:
        # STORE PATH is RELATIVE to TOP so is JUST FILE NAME
        # STAGING PATH is JUST the UUID.

        return IncomingTransfer(
            id=self.destination_id,
            status=self.status,
            uploader=self.uploader,
            upload_name=self.file_name,
            source=self.source,
            transfer_size=self.transfer_size,
            transfer_checksum=self.transfer_checksum,
            store=store,
            transfer_manager_name=self.transfer_manager_name,
            start_time=self.start_time,
            staging_path=self.get_uuid(store=store),
            store_path=self.file_name,
            source_transfer_id=self.source_id,
        )


class AllInfo(BaseModel):
    file_info: list[FileInfo]
    transfer_info: list[TransferInfo]


def main():
    from librarian_server import database, server_settings

    args = parser.parse_args()

    if args.source and args.destination:
        raise ValueError(
            "Can not have both source and destination mode activated at the same time"
        )

    if (not args.source) and (not args.destination):
        raise ValueError("Please select one, destination or source.")

    if args.source:
        core_source(
            librarian_name=args.librarian_name,
            age=args.age,
            session_maker=database.get_session,
            name=server_settings.name,
        )

    if args.destination:
        core_destination(
            input=sys.stdin.read(),
            store_name=args.store_name,
            spot_check_every=args.spot_check,
            track_progress=args.track_progress,
            session_maker=database.get_session,
        )


def core_source(librarian_name: str, age: float, name: str, session_maker: callable):
    """
    Generates (and prints) a JSON representaion remote instances and
    outgoing transfers for ingest on destination side.
    """

    time_cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        hours=age
    )

    librarian_statement = select(Librarian).where(Librarian.name == librarian_name)

    with session_maker() as session:
        librarian = session.scalars(librarian_statement).one_or_none()

        if librarian is None:
            raise ValueError(f"Could not find librarian {librarian_name} in database.")

        librarian_id = librarian.id

    remote_instance_statement = (
        select(RemoteInstance)
        .where(RemoteInstance.librarian_id == librarian_id)
        .where(RemoteInstance.copy_time >= time_cutoff)
    )

    with session_maker() as session:
        remote_instances = session.scalars(remote_instance_statement).all()
        file_info = [
            FileInfo.from_file(file=ri.file, remote_instance=ri, source=name)
            for ri in remote_instances
        ]

    transfer_statement = (
        select(OutgoingTransfer)
        .where(OutgoingTransfer.destination == librarian_name)
        .where(OutgoingTransfer.start_time >= time_cutoff)
        .where(
            OutgoingTransfer.status.in_(
                [
                    TransferStatus.ONGOING,
                    TransferStatus.STAGED,
                    TransferStatus.INITIATED,
                ]
            )
        )
    )

    with session_maker() as session:
        outgoing_transfers = session.scalars(transfer_statement).all()
        transfer_info = [
            TransferInfo.from_transfer(file=ot.file, outgoing_transfer=ot, source=name)
            for ot in outgoing_transfers
        ]

    summary_info = AllInfo(
        file_info=file_info,
        transfer_info=transfer_info,
    )

    print(summary_info.model_dump_json())

    return summary_info


def core_destination(
    input: str,
    store_name: str,
    spot_check_every: int,
    track_progress: bool,
    session_maker: callable,
):
    """
    Core function for the destination librarian. Takes in the input,
    de-serializes it, and adds the items to the downstream database.

    Spot checking calcualtes the checksum of the downstream items
    to make sure everything is going ok.
    """

    summary_info = AllInfo.model_validate_json(input)

    number_of_files = len(summary_info.file_info)
    number_of_transfers = len(summary_info.transfer_info)

    # Ingest files first, then ingest transfers.
    # Do this all in ONE session. Make sure it's all correct.

    with session_maker() as session:
        store = session.query(StoreMetadata).filter_by(name=store_name).one_or_none()

        if store is None:
            raise ValueError(f"Store {store_name} is not available in the database.")

        addables = []

        for i, file_info in enumerate(summary_info.file_info):
            # First: check if this exists.
            potential_file = session.get(File, file_info.name)

            # That's ok - it must have been present in the backup. We expect
            # some level of overlap!
            if not potential_file is None:
                continue

            file, instance = file_info.to_file(store=store)

            if (i % spot_check_every) == 0:
                if track_progress:
                    print(f"Checking file, have ingested {i}/{number_of_files} files")
                checksum = get_md5_from_path(instance.path)
                size = get_size_from_path(instance.path)

                if not ((checksum == file.checksum) and (size == file.size)):
                    raise RuntimeError(f"Checksum does not match for file {file}")

            addables += [file, instance]

        for i, transfer_info in enumerate(summary_info.transfer_info):
            # First: check if this transfer exists.
            potential_transfer = session.get(
                IncomingTransfer, transfer_info.destination_id
            )

            # That's ok - it must have been present in the backup. We expect
            # some level of overlap!
            if not potential_transfer is None:
                continue

            incoming_transfer = transfer_info.to_transfer(store=store)

            if (i % spot_check_every) == 0:
                if track_progress:
                    print(
                        f"Checking transfer, have ingested {i}/{number_of_files} files"
                    )
                # Check these by seeing if the folder exists.

                full_path = store.store_manager.resolve_path_staging(
                    incoming_transfer.staging_path
                )

                if not full_path.exists():
                    raise RuntimeError(
                        f"Staging location {incoming_transfer.staging_path} does not exist"
                    )

            addables += [incoming_transfer]

        if track_progress:
            print("Completed the ingest process. Committing to database.")

        session.add_all(addables)
        session.commit()

    return


if __name__ == "__main__":
    main()
