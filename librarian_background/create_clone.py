"""
Task that takes a store and clones all files uploaded to it to another store
within some time-frame.
"""

import datetime
import logging
from pathlib import Path

from schedule import CancelJob
from sqlalchemy.orm import Session

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
    clone_to: str
    "Name of the store to create copies on."
    age_in_days: int
    "Age in days of the files to check. I.e. only check files younger than this (we assume older files are fine as they've been checked before)"

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
            store_to = self.get_store(self.clone_to, session)
        except ValueError:
            # Store doesn't exist. Cancel this job.
            log_to_database(
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.CONFIGURATION,
                message=f"Store {self.clone_to} does not exist. Cancelling job. Please update the configuration.",
                session=session,
            )
            return CancelJob

        # Now figure out what files were uploaded in the past age_in_days days.
        start_time = datetime.datetime.now() - datetime.timedelta(days=self.age_in_days)

        # Now we can query the database for all files that were uploaded in the past age_in_days days.
        instances: list[Instance] = (
            session.query(Instance)
            .filter(Instance.store == store_from)
            .filter(Instance.created_time > start_time)
            .all()
        )

        successful_clones = 0
        unnecessary_clones = 0
        all_transfers_successful = True

        for instance in instances:
            # Check if there is a matching instance already on our clone_to store.
            # If there is, we don't need to clone it.
            if (
                session.query(Instance)
                .filter(Instance.store == store_to)
                .filter(Instance.file == instance.file)
                .first()
            ):
                unnecessary_clones += 1
                logger.debug(
                    f"File instance {Instance} already exists on clone_to store. Skipping."
                )
                continue

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
                # TODO: In the future where we have multiple potential clone stores for SneakerNet we should
                # automatically fail-over to the next store.
                log_to_database(
                    severity=ErrorSeverity.ERROR,
                    category=ErrorCategory.STORE_FULL,
                    message=f"File {instance.file.name} is too large to fit on store {store_to}. Skipping. (Instance {instance.id})",
                    session=session,
                )

                transfer.fail_transfer(session=session)

                all_transfers_successful = False

                continue

            success = False

            # TODO: Isn't this logic faulty? We should simply try all the transfer managers until
            #       one works, right?
            for tm_name, transfer_manager in store_to.transfer_managers.items():
                try:
                    success = store_from.store_manager.transfer_out(
                        store_path=Path(instance.path),
                        destination_path=staged_path,
                        using=transfer_manager,
                    )

                    if not success:
                        logger.debug(
                            f"Failed to transfer file {instance.path} to store {store_to} using transfer manager {transfer_manager}."
                        )

                        transfer.fail_transfer(session=session)

                        continue
                    else:
                        break

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
                path_info = store_to.store_manager.path_info(staged_path)

                if path_info.md5 != instance.file.checksum:
                    log_to_database(
                        severity=ErrorSeverity.ERROR,
                        category=ErrorCategory.DATA_INTEGRITY,
                        message=f"File {instance.path} on store {store_to} has an incorrect checksum. "
                        f"Expected {instance.file.checksum}, got {path_info.md5}. (Instance {instance.id})",
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
            # TODO: Fix this - the instance.path is not the correct path; that's the path on the
            #       OLD store not our new store...
            new_instance = Instance.new_instance(
                path=instance.path,
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
            f"to store {store_to}. {unnecessary_clones}/{len(instances)} files were already "
            f"present on the clone_to store. All successful: "
            f"{successful_clones + unnecessary_clones}/{len(instances)}."
        )

        return all_transfers_successful