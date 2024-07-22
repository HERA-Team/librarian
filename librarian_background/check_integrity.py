"""
Task for checking the integrity of the store.
"""

import datetime
import logging

from schedule import CancelJob
from sqlalchemy.orm import Session

from hera_librarian.utils import compare_checksums, get_hash_function_from_hash
from librarian_server.database import get_session
from librarian_server.logger import ErrorCategory, ErrorSeverity, log_to_database
from librarian_server.orm import Instance, StoreMetadata

from .task import Task

logger = logging.getLogger("schedule")


class CheckIntegrity(Task):
    """
    A background task that checks the integrity of a given store.
    """

    store_name: str
    "Name of the store to check."
    age_in_days: int
    "Age in days of the files to check. I.e. only check files younger than this (we assume older files are fine as they've been checked before)"

    def get_store(self, session: Session) -> StoreMetadata:
        possible_metadata = (
            session.query(StoreMetadata).filter_by(name=self.store_name).first()
        )

        if not possible_metadata:
            raise ValueError(f"Store {self.store_name} does not exist.")

        return possible_metadata

    def on_call(self):
        with get_session() as session:
            return self.core(session=session)

    def core(self, session: Session):
        """
        Frame this out with the session so that it is automatically closed.
        """
        try:
            store = self.get_store(session=session)
        except ValueError:
            # Store doesn't exist. Cancel this job.
            log_to_database(
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.CONFIGURATION,
                message=f"Store {self.store_name} does not exist. Cancelling job. Please update the configuration.",
                session=session,
            )
            return CancelJob

        # Now figure out what files were uploaded in the past age_in_days days.
        start_time = datetime.datetime.now() - datetime.timedelta(days=self.age_in_days)

        # Now we can query the database for all files that were uploaded in the past age_in_days days.
        files = (
            session.query(Instance)
            .filter(Instance.store == store and Instance.created_time > start_time)
            .all()
        )

        all_files_fine = True

        for file in files:
            # Now we can check the integrity of each file.
            try:
                hash_function = get_hash_function_from_hash(file.file.checksum)
                path_info = store.store_manager.path_info(
                    file.path, hash_function=hash_function
                )
            except FileNotFoundError:
                all_files_fine = False
                log_to_database(
                    severity=ErrorSeverity.ERROR,
                    category=ErrorCategory.DATA_AVAILABILITY,
                    message=f"File {file.path} on store {store.name} is missing. (Instance: {file.id})",
                    session=session,
                )
                continue

            # Compare checksum to database
            expected_checksum = file.file.checksum

            if compare_checksums(expected_checksum, path_info.checksum):
                # File is fine.
                logger.info(
                    f"File {file.path} on store {store.name} has been validated."
                )
                continue
            else:
                # File is not fine. Log it.
                all_files_fine = False
                log_to_database(
                    severity=ErrorSeverity.ERROR,
                    category=ErrorCategory.DATA_INTEGRITY,
                    message=f"File {file.path} on store {store.name} has an incorrect checksum. Expected {expected_checksum}, got {path_info.checksum}. (Instance: {file.id})",
                    session=session,
                )

        if all_files_fine:
            logger.info(
                f"All files uploaded since {start_time} on store {store.name} have been validated."
            )
        else:
            logger.error(
                f"Some files uploaded since {start_time} on store {store.name} have not been validated. Please check the logs."
            )

        return all_files_fine
