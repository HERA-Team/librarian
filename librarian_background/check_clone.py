"""
Checks async in-progress clones to see if they have progressed from
INITIATED to STAGED. If so, it makes a call to the downstream librarian
to let it know that it can begin to ingest the file.
"""

import datetime
from typing import TYPE_CHECKING

from librarian_server.database import get_session
from librarian_server.logger import ErrorCategory, ErrorSeverity, log, log_to_database
from librarian_server.orm.transfer import OutgoingTransfer, TransferStatus

from .core import Task

# from sqlalchemy.orm import query

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class CheckClone(Task):
    """
    Checks all in-flight clones and queries their status.
    """

    transfers_per_run: int = 1024
    "The maximum number of transfers to check per run of CheckClone."

    def on_call(self):
        with get_session() as session:
            return self.core(session=session)

    def core(self, session: Session):
        """
        Loop through all valid OutgoingTransfers and check their status.
        """

        core_begin = datetime.datetime.now(datetime.UTC)

        ongoing_transfers = list[OutgoingTransfer] = (
            session.query(OutgoingTransfer)
            .filter_by(status=TransferStatus.ONGOING)
            .all()
        )

        all_transfers_succeeded = True

        if len(ongoing_transfers) == 0:
            log.info("No currently ongoing asynchronous outbound transfers found.")

        transfers_processed = 0

        for transfer in ongoing_transfers:
            current_time = datetime.datetime.now(datetime.UTC)
            elapsed_time = current_time - core_begin
            over_time = elapsed_time > self.soft_timeout if self.soft_timeout else False

            if over_time:
                log.info(
                    "CheckClone task has gone over time. Will reschedule for later."
                )
                break

            if transfers_processed >= self.transfers_per_run:
                log.info(
                    f"Processed {transfers_processed} transfers, which is the maximum "
                    "for each CheckClone run. Rescheduling for later."
                )
                break

            if not transfer.status == TransferStatus.ONGOING:
                log_to_database(
                    severity=ErrorSeverity.ERROR,
                    category=ErrorCategory.PROGRAMMING,
                    message=f"Transfer {transfer.id} was selected with ONGOING "
                    f"status but actually has status {transfer.status}. This is a "
                    "programming error, potentailly due to concurrency.",
                )

            if transfer.transfer_data is None:
                # Nothing we can do for this one! Probably a synchronous transfer,
                # or a sneaker net. Best to leave it alone.
                continue

            # Now actually increment our counter; all cases before this are
            # 'easy' and should complete in microseconds.
            transfers_processed += 1

            response: TransferStatus = transfer.transfer_data.transfer_status

            if response == TransferStatus.STAGED:
                transfer.staged_transfer(session=session, commit=False)
                continue
            elif response == TransferStatus.FAILED:
                transfer.fail_transfer(session=session, commit=False)
            elif response == TransferStatus.ONGOING:
                # Keep waiting.
                continue
            else:
                log_to_database(
                    severity=ErrorSeverity.ERROR,
                    category=ErrorCategory.PROGRAMMING,
                    message=(
                        f"CheckClone recieved an unexpected transfer status response "
                        f"from query to OutgoingTransfer ID={transfer.id} ({response})."
                    ),
                )
                continue

        session.commit()

        log.info(
            f"CheckClone task completed. Processed {transfers_processed} transfers."
        )
