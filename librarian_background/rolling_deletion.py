"""
A (very) dangerous task that you may have to use. This task will delete files that
are older than a certain age, subject to some (optional) constraints:

a) The file must have $N$ remote instances available throughout the network
b) The checksums of those files must match the original checksum
"""

import time
from datetime import datetime, timedelta, timezone

from loguru import logger
from schedule import CancelJob
from sqlalchemy import select
from sqlalchemy.orm import Session

from librarian_server.api.validate import calculate_checksum_of_remote_copies
from librarian_server.database import get_session
from librarian_server.orm import Instance, Librarian, StoreMetadata

from .task import Task


class RollingDeletion(Task):
    """
    A background task that deletes _instances_ (not files!) that are older than
    a certain age.
    """

    store_name: str
    "Name of the store to delete instances from"
    age_in_days: float
    "Age of the instances to delete, in days; older instances will be deleted if they pass the checks"

    number_of_remote_copies: int = 3
    "Number of remote copies that must be available to delete the file"
    verify_downstream_checksums: bool = True
    "Whether to verify the checksums of the remote copies"
    mark_unavailable: bool = True
    "Whether to mark the instances as unavailable after deletion, or to delete them (False)"
    force_deletion: bool = True
    "Whether to over-ride the deletion policy of instances"

    def get_store(self, name: str, session: Session) -> StoreMetadata:
        possible_metadata = session.query(StoreMetadata).filter_by(name=name).first()

        if not possible_metadata:
            raise ValueError(f"Store {name} does not exist.")

        return possible_metadata

    def on_call(self):
        with get_session() as session:
            return self.core(session=session)

    def core(self, session: Session):
        core_begin = datetime.now(timezone.utc)
        age_cutoff = core_begin - timedelta(days=self.age_in_days)

        try:
            store = self.get_store(self.store_name, session)
        except ValueError as e:
            logger.error("Error getting store: {}, cannot continue; cancelling job", e)
            return CancelJob

        logger.info(
            "Beginning rolling deletion for store {} (ID: {})", store.name, store.id
        )

        # Get the instances that are older than the age

        logger.info(
            "Querying for created_times later than {} UTC ({} local)",
            age_cutoff,
            age_cutoff.astimezone(),
        )

        query_begin = time.perf_counter()
        stmt = select(Instance).filter(
            Instance.store_id == store.id,
            Instance.created_time < age_cutoff.astimezone(timezone.utc),
            Instance.available == True,
        )

        instances = session.execute(stmt).scalars().all()
        query_end = time.perf_counter()

        logger.info("Queried for old instances in {} seconds", query_end - query_begin)

        logger.info(
            "Found {} instances that are older than {} days",
            len(instances),
            self.age_in_days,
        )

        deleted = 0
        for instance in instances:
            # First, see if we've timed out.
            if self.soft_timeout is not None:
                if (datetime.now(timezone.utc) - core_begin) > self.soft_timeout:
                    logger.warning(
                        "Ran out of time in deletion task! Only successfully deleted "
                        "{n}/{m} instances; we will return later",
                        n=deleted,
                        m=len(instances),
                    )
                    return False

            # Check that we got what we wanted.
            valid_time = instance.created_time.replace(tzinfo=timezone.utc) < age_cutoff
            valid_store = instance.store_id == store.id
            all_ok = valid_time and valid_store and instance.available

            if not all_ok:
                logger.error(
                    "Instance {} does not meet the criteria, skipping", instance.id
                )
                continue

            # Check if the file associated with the instance has enough copies.
            remote_librarian_ids = {
                remote_instance.librarian_id
                for remote_instance in instance.file.remote_instances
            }

            logger.info(
                "Calling up {} remote librarians to check for copies",
                len(remote_librarian_ids),
            )

            downstream = []

            for librarian_id in remote_librarian_ids:
                stmt = select(Librarian).filter(Librarian.id == librarian_id)
                librarian = session.execute(stmt).scalar()

                if not librarian:
                    continue

                downstream += calculate_checksum_of_remote_copies(
                    librarian=librarian, file_name=instance.file_name
                )

            # Now check if we have enough!
            if len(downstream) < self.number_of_remote_copies:
                logger.warning(
                    "Instance {} does not have enough remote copies {}/{}, skipping",
                    instance.id,
                    len(downstream),
                    self.number_of_remote_copies,
                )
                continue

            # Now check if the checksums match
            if self.verify_downstream_checksums:
                for info in downstream:
                    if not info.computed_same_checksum:
                        logger.warning(
                            "Instance {} has a mismatched checksum on {}, skipping",
                            instance.id,
                            info.librarian,
                        )
                        continue

            # If we're here, we can delete the instance.
            logger.info(
                "Verified that we have the correct number of copies of "
                "{instance.id} ({instance.file_name}): {n}/{m}, proceeding to deletion",
                instance=instance,
                n=len(downstream),
                m=self.number_of_remote_copies,
            )

            try:
                instance.delete(
                    session=session,
                    commit=True,
                    force=self.force_deletion,
                    mark_unavailable=self.mark_unavailable,
                )
                logger.info("Deleted data for instance {} successfully", instance.id)
                deleted += 1
            except FileNotFoundError:
                logger.error(
                    "Instance {} does not exist on disk, skipping", instance.id
                )
                continue

        core_end = datetime.now(timezone.utc)

        logger.info(
            "Finished rolling deletion for store {} (ID: {}) in {} seconds, deleted {}/{} instances",
            store.name,
            store.id,
            (core_end - core_begin).total_seconds(),
            deleted,
            len(instances),
        )

        return deleted == len(instances)
