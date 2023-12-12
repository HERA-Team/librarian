"""
Task for checking the integrity of the store.
"""

from .task import Task

import logging
import datetime

from schedule import CancelJob

from librarian_server.database import session, query
from librarian_server.orm import StoreMetadata, Instance


logger = logging.getLogger('schedule')

class CheckIntegrity(Task):
    """
    A background task that checks the integrity of a given store.
    """

    store_name: str
    "Name of the store to check."
    age_in_days: int
    "Age in days of the files to check. I.e. only check files younger than this (we assume older files are fine as they've been checked before)"

    def get_store(self) -> StoreMetadata:
        possible_metadata = query(StoreMetadata).filter(StoreMetadata.name == self.store_name).first()

        if not possible_metadata:
            raise ValueError(f"Store {self.store_name} does not exist.")
        
        return possible_metadata

    def on_call(self):
        try:
            store = self.get_store()
        except ValueError:
            # Store doesn't exist. Cancel this job.
            logger.error(f"Store {self.store_name} does not exist. Cancelling job. Please update the configuration.")
            return CancelJob
        
        # Now figure out what files were uploaded in the past age_in_days days.
        start_time = datetime.datetime.now() - datetime.timedelta(days=self.age_in_days)

        # Now we can query the database for all files that were uploaded in the past age_in_days days.
        files = query(Instance).filter(Instance.store == store).filter(Instance.created_time > start_time).all()

        all_files_fine = True

        for file in files:
            # Now we can check the integrity of each file.
            try:
                path_info = store.store_manager.path_info(file.path)
            except FileNotFoundError:
                all_files_fine = False
                logger.error(f"File {file.path} on store {store.name} is missing!")
                continue

            # Compare checksum to database
            if path_info.md5 == file.md5:
                # File is fine.
                logger.info(f"File {file.path} on store {store.name} has been validated.")
                continue
            else:
                # File is not fine. Log it.
                all_files_fine = False
                logger.error(f"File {file.path} on store {store.name} has an incorrect checksum. Expected {file.md5}, got {path_info.md5}.")

        if all_files_fine:
            logger.info(f"All files uploaded since {start_time} on store {store.name} have been validated.")
        else:
            logger.error(f"Some files uploaded since {start_time} on store {store.name} have not been validated. Please check the logs.")

        return

    
