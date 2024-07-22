"""
Task that takes a store and clones all files uploaded to it to another store
within some time-frame.
"""

import datetime
import logging
from pathlib import Path
from typing import Optional

from schedule import CancelJob
from sqlalchemy import select
from sqlalchemy.orm import Session

from hera_librarian.utils import compare_checksums, get_hash_function_from_hash
from librarian_server.database import get_session
from librarian_server.logger import ErrorCategory, ErrorSeverity, log_to_database
from librarian_server.orm import CloneTransfer, Instance, StoreMetadata, TransferStatus

from .task import Task

logger = logging.getLogger("schedule")


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
        core_begin = datetime.datetime.utcnow()

        try:
            store_from = self.get_store(self.clone_from, session)
        except ValueError:
            # Store doesn't exist. Cancel this job.
            log_to_database(
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.CONFIGURATION,
                message=f"Store {self.clone_from} does not exist. Cancelling job. Please update the configuration.",
                session=session,
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
            log_to_database(
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.CONFIGURATION,
                message=f"Store {self.clone_to} does not exist. Cancelling job. Please update the configuration.",
                session=session,
            )
            return CancelJob

        all_disabled = False
        for store in stores_to:
            all_disabled = all_disabled and not store.enabled

        if all_disabled:
            log_to_database(
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.CONFIGURATION,
                message=f"All stores in {self.clone_to} are disabled. It is likely that all stores are full.",
                session=session,
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

        successful_clones = 0
        unnecessary_clones = 0
        all_transfers_successful = True

        for instance in instances:
            # First, check if we have gone over time:
            if (
                (datetime.datetime.utcnow() - core_begin > self.soft_timeout)
                if self.soft_timeout
                else False
            ):
                logger.info(
                    "CreateLocalClone task has gone over time. Will reschedule for later."
                )
                break

            if successful_clones > self.files_per_run:
                logger.info(
                    f"CreateLocalClone task has cloned {successful_clones} files, which is over "
                    f"the limit of {self.files_per_run}. Will reschedule for later."
                )
                break

            # Check if there is a matching instance already on our clone_to store.
            # If there is, we don't need to clone it.
            for secondary_instance in instance.file.instances:
                if secondary_instance.store in stores_to:
                    unnecessary_clones += 1
                    logger.debug(
                        f"File instance {instance} already exists on clone_to store. Skipping."
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
                        log_to_database(
                            severity=ErrorSeverity.WARNING,
                            category=ErrorCategory.STORE_FULL,
                            message=f"Store {store} is full. Disabling; please replace the disk.",
                            session=session,
                        )
                    continue

                store_available = True
                store_to = store

                break

            if not store_available:
                log_to_database(
                    severity=ErrorSeverity.ERROR,
                    category=ErrorCategory.STORE_FULL,
                    message=(
                        f"File {instance.file.name} is too large to fit on any store in "
                        f"{self.clone_to}. Skipping. (Instance {instance.id})"
                    ),
                    session=session,
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
                log_to_database(
                    severity=ErrorSeverity.ERROR,
                    category=ErrorCategory.STORE_FULL,
                    message=(
                        f"File {instance.file.name} is too large to fit on store {store_to}. "
                        f"Skipping, but this should have already have been caught. (Instance {instance.id})"
                    ),
                    session=session,
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
                        f"Failed to transfer file {instance.path} to store {store_to} using transfer manager {transfer_manager}."
                    )
                except FileNotFoundError as e:
                    log_to_database(
                        severity=ErrorSeverity.ERROR,
                        category=ErrorCategory.DATA_AVAILABILITY,
                        message=f"File {e.filename} does not exist when trying to clone from {store_from}. Skipping. (Instance {instance.id})",
                        session=session,
                    )

                    transfer.fail_transfer(session=session)

                    all_transfers_successful = False

                    continue

            if not success:
                # Fail the transfer _here_, not after trying every transfer manager.

                log_to_database(
                    severity=ErrorSeverity.ERROR,
                    category=ErrorCategory.DATA_AVAILABILITY,
                    message=f"Failed to transfer file {instance.path} to store {store_to}. Skipping. (Instance {instance.id})",
                    session=session,
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
                    log_to_database(
                        severity=ErrorSeverity.ERROR,
                        category=ErrorCategory.DATA_INTEGRITY,
                        message=f"File {instance.path} on store {store_to} has an incorrect checksum. "
                        f"Expected {instance.file.checksum}, got {path_info.checksum}. (Instance {instance.id})",
                        session=session,
                    )

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
                log_to_database(
                    severity=ErrorSeverity.CRITICAL,
                    category=ErrorCategory.PROGRAMMING,
                    message=f"File {instance.path} already exists on store {store_to}. Skipping. (Instance {instance.id})",
                    session=session,
                )

                store_to.store_manager.unstage(staging_name)

                transfer.fail_transfer(session=session)

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
