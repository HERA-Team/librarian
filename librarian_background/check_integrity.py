"""
Task for checking the integrity of the store.
"""

import datetime
import time

from loguru import logger
from schedule import CancelJob
from sqlalchemy.orm import Session

from hera_librarian.utils import compare_checksums, get_hash_function_from_hash
from librarian_server.database import get_session
from librarian_server.orm import Instance, StoreMetadata

from .task import Task


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
            logger.info(
                "Checking integrity of store {}, age_in_days={}",
                self.store_name,
                self.age_in_days,
            )
            store = self.get_store(session=session)
        except ValueError:
            logger.error(
                "Store {} does not exist, cancelling job: please update configuration",
                self.store_name,
            )
            return CancelJob

        # Now figure out what files were uploaded in the past age_in_days days.
        start_time = datetime.datetime.now() - datetime.timedelta(days=self.age_in_days)

        # Now we can query the database for all files that were uploaded in the past age_in_days days.
        query_start = time.perf_counter()
        files = (
            session.query(Instance)
            .filter(Instance.store == store and Instance.created_time > start_time)
            .all()
        )
        query_end = time.perf_counter()
        logger.info(
            "Queried database for instances created since {} in {} seconds",
            start_time,
            query_end - query_start,
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
                logger.error(
                    "Instance {} on store {} is missing. (Instance: {})",
                    file.path,
                    store.name,
                    file.id,
                )
                continue

            # Compare checksum to database
            expected_checksum = file.file.checksum

            if compare_checksums(expected_checksum, path_info.checksum):
                logger.info(
                    "Instance {} on store {} has been validated (Instance: {})",
                    file.path,
                    store.name,
                    file.id,
                )
                continue
            else:
                # File is not fine. Log it.
                all_files_fine = False
                logger.error(
                    "Instance {} on store {} has an incorrect checksum. Expected {}, got {}. (Instance: {})",
                    file.path,
                    store.name,
                    expected_checksum,
                    path_info.checksum,
                    file.id,
                )
        if all_files_fine:
            logger.info(
                "All files uploaded since {} on store {} have been validated.",
                start_time,
                store.name,
            )
        else:
            logger.error(
                "Store {} has files with incorrect checksums.",
                store.name,
            )

        return all_files_fine
