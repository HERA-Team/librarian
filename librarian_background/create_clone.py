"""
Task that takes a store and clones all files uploaded to it to another store
within some time-frame.
"""

import datetime
import time
from pathlib import Path
from typing import Optional

from loguru import logger
from schedule import CancelJob
from sqlalchemy import select
from sqlalchemy.orm import Session

from hera_librarian.utils import compare_checksums, get_hash_function_from_hash
from librarian_server.database import get_session
from librarian_server.orm import CloneTransfer, Instance, StoreMetadata, TransferStatus

from .task import Task


class CreateLocalClone(Task):
    """
    A background task that creates a local clone instance.
    """

    clone_from: str
    "Name of the store to clone from."
    clone_to: str | list[str]
    "Name of the store(s) to create copies on. If multiple are provided, the task will go through each one in order in case one or more stores are full."
    age_in_days: int
    "Age in days of the files to check. I.e. only check files younger than this (we assume older files are fine as they've been checked before)"
    files_per_run: int = 1024
    "Maximum number of files to clone in any one run. If there are more files than this, we will put off cloning them until the next run."
    disable_store_on_full: bool = False
    "If true, will disable the store if it cannot fit a new file. If false, will just skip the file and keep trying."

    # TODO: In the future, we could implement a _rolling_ n day clone here, i.e. only keep the last n days of files on the clone_to store.

    def get_store(self, name: str, session: Session) -> StoreMetadata:
        possible_metadata = session.query(StoreMetadata).filter_by(name=name).first()

        if not possible_metadata:
            raise ValueError(f"Store {name} does not exist.")

        return possible_metadata

    def on_call(self):
        with get_session() as session:
            return self.core(session=session)

    def core(self, session: Session):
        core_begin = datetime.datetime.now(datetime.timezone.utc)

        try:
            store_from = self.get_store(self.clone_from, session)
        except ValueError:
            # Store doesn't exist. Cancel this job.
            logger.error(
                "Store {} does not exist, cancelling job: please update configuration",
                self.clone_from,
            )
            return CancelJob

        try:
            if isinstance(self.clone_to, list):
                stores_to = [
                    self.get_store(clone_to, session) for clone_to in self.clone_to
                ]
            else:
                stores_to = [self.get_store(self.clone_to, session)]
        except ValueError:
            # Store doesn't exist. Cancel this job.
            logger.error(
                "Store {} does not exist, cancelling job: please update configuration",
                self.clone_from,
            )
            return CancelJob

        all_disabled = False
        for store in stores_to:
            all_disabled = all_disabled and not store.enabled

        if all_disabled:
            logger.error(
                "All stores in {} are disabled. It is likely that all stores are full.",
                self.clone_to,
            )
            return

        # Now figure out what files were uploaded in the past age_in_days days.
        start_time = datetime.datetime.now() - datetime.timedelta(days=self.age_in_days)

        # Now we can query the database for all files that were uploaded in the past age_in_days days,
        # and do not live on one of our stores.

        # Step-by-step:
        # 1. Get all filenames instances on the source store.
        # 2. Get all filenames of instances on destination store.
        # 3. Get the difference between the two.
        # 4. Get all instances on the source store that are in the difference.
        # 5. Get all instances on the source store that are in the difference and are younger than start_time.
        # 6. Clone all of these instances to the destination store.

        query_start = time.perf_counter()

        source_store_id = store_from.id
        destination_store_ids = [store.id for store in stores_to]

        query_all_source_instances = select(Instance.file_name).where(
            Instance.store_id == source_store_id
        )

        query_all_destination_instances = select(Instance.file_name).where(
            Instance.store_id.in_(destination_store_ids)
        )

        query_bisection = query_all_source_instances.except_(
            query_all_destination_instances
        )

        query_all_instances = select(Instance).where(
            Instance.file_name.in_(query_bisection)
        )

        query_all_local_instances = query_all_instances.where(
            Instance.store_id == source_store_id
        )

        query = query_all_local_instances.where(Instance.created_time > start_time)

        instances: list[Instance] = session.execute(query).scalars().all()

        query_end = time.perf_counter()

        logger.info(
            "Queried database for local clone instances created since {} in {} seconds",
            start_time,
            query_end - query_start,
        )

        successful_clones = 0
        unnecessary_clones = 0
        all_transfers_successful = True

        for instance in instances:
            # First, check if we have gone over time:
            if (
                (
                    datetime.datetime.now(datetime.timezone.utc) - core_begin
                    > self.soft_timeout
                )
                if self.soft_timeout
                else False
            ):
                logger.info(
                    "CreateLocalClone task has gone over time; will reschedule for later"
                )
                break

            if successful_clones > self.files_per_run:
                logger.info(
                    "CreateLocalClone has cloned {} files, which is over the limit of {}; "
                    "will reschedule for later",
                    successful_clones,
                    self.files_per_run,
                )
                break

            # Check if there is a matching instance already on our clone_to store.
            # If there is, we don't need to clone it.
            for secondary_instance in instance.file.instances:
                if secondary_instance.store in stores_to:
                    unnecessary_clones += 1
                    logger.debug(
                        "File instance {} already exists on clone_to store. Skipping.",
                        instance,
                    )
                    continue

            store_available = False
            store_to: Optional[StoreMetadata] = None

            for store in stores_to:
                if not (store.store_manager.available and store.enabled):
                    continue

                if not store.store_manager.free_space >= instance.file.size:
                    # Store is full.
                    if self.disable_store_on_full:
                        store.enabled = False
                        session.commit()
                        logger.warning(
                            "Store {} is full. Disabling; please replace the disk",
                            store,
                        )
                    continue

                store_available = True
                store_to = store

                break

            if not store_available:
                logger.error(
                    "File {} is too large to fit on any store in {}; skipping",
                    instance.file.name,
                    self.clone_to,
                )

                all_transfers_successful = False

                break

            transfer = CloneTransfer.new_transfer(
                source_store_id=store_from.id,
                destination_store_id=store_to.id,
                source_instance_id=instance.id,
            )

            session.add(transfer)
            session.commit()

            # TODO: Check if there is an already existing transfer! Maybe it is running asynchronously? Maybe we need to check the status?

            # Now we can clone the file to the clone_to store.
            try:
                staging_name, staged_path = store_to.store_manager.stage(
                    file_size=instance.file.size,
                    file_name=Path(instance.file.name).name,
                )
            except ValueError:
                logger.error(
                    "File {} is too large to fit on store {}; skipping, but should already have been"
                    "caught, check the logic",
                    instance.file.name,
                    store_to,
                )

                transfer.fail_transfer(session=session)

                all_transfers_successful = False

                continue

            success = False

            for tm_name, transfer_manager in store_to.transfer_managers.items():
                try:
                    if not store_from.store_manager.can_transfer(
                        using=transfer_manager
                    ):
                        continue

                    success = store_from.store_manager.transfer_out(
                        store_path=Path(instance.path),
                        destination_path=staged_path,
                        using=transfer_manager,
                    )

                    if success:
                        break

                    logger.debug(
                        "Failed to transfer file {} to store {} using transfer manager {}",
                        instance.path,
                        store_to,
                        tm_name,
                    )
                except FileNotFoundError as e:
                    logger.error(
                        "File {} does not exist when trying to clone from {}, skipping",
                        e.filename,
                        store_from,
                    )

                    transfer.fail_transfer(session=session)

                    all_transfers_successful = False

                    continue

            if not success:
                # Fail the transfer _here_, not after trying every transfer manager.
                logger.error(
                    "Failed to transfer file {} to store {}, skipping",
                    instance.path,
                    store_to,
                )

                transfer.fail_transfer(session=session)
                all_transfers_successful = False

                continue

            transfer.transfer_manager_name = tm_name
            transfer.status = TransferStatus.STAGED

            session.commit()

            # Now we can commit the file to the store.
            try:
                hash_function = get_hash_function_from_hash(instance.file.checksum)
                path_info = store_to.store_manager.path_info(
                    staged_path, hash_function=hash_function
                )

                if not compare_checksums(path_info.checksum, instance.file.checksum):
                    logger.error(
                        "File {} on store {} has an incorrect checksum. Expected {}, got {}. (Instance {})",
                        instance.path,
                        store_to,
                        instance.file.checksum,
                        path_info.checksum,
                        instance.id,
                    )

                    # TODO: Use the corrupt file table here.

                    transfer.fail_transfer(session=session)
                    store_to.store_manager.unstage(staged_path)
                    all_transfers_successful = False

                    continue

                resolved_store_path = store_to.store_manager.store(
                    Path(instance.file.name)
                )

                store_to.store_manager.commit(
                    staging_path=staged_path, store_path=resolved_store_path
                )
            except FileExistsError:
                logger.error(
                    "File {} already exists on store {}. Skipping. (Instance {})",
                    instance.path,
                    store_to,
                    instance.id,
                )

                store_to.store_manager.unstage(staging_name)
                transfer.fail_transfer(session=session)
                all_transfers_successful = False

                continue
            except ValueError as e:
                logger.error(
                    "Failed to commit file {} to store {}: {}. Skipping. (Instance {})",
                    instance.path,
                    store_to,
                    e,
                    instance.id,
                )

                transfer.fail_transfer(session=session)
                store_to.store_manager.unstage(staged_path)
                all_transfers_successful = False

                continue

            store_to.store_manager.unstage(staging_name)

            # Everything is good! We can create a new instance.
            new_instance = Instance.new_instance(
                path=resolved_store_path,
                file=instance.file,
                store=store_to,
                deletion_policy=instance.deletion_policy,
            )

            session.add(new_instance)

            # Need to commit to get a valid id.
            session.commit()

            transfer.destination_instance_id = new_instance.id
            transfer.status = TransferStatus.COMPLETED
            transfer.end_time = datetime.datetime.now()

            session.commit()
            successful_clones += 1

        logger.info(
            f"Cloned {successful_clones}/{len(instances)} files from store {store_from} "
            f"to store(s) {stores_to}. {unnecessary_clones}/{len(instances)} files were already "
            f"present on the clone_to store. All successful: "
            f"{successful_clones + unnecessary_clones}/{len(instances)}."
        )

        return all_transfers_successful
