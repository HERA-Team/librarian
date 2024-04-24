"""
Consumers for the background task queues.
"""

import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import sysrsync
from sqlalchemy import select

from hera_librarian.queues import Queue
from hera_librarian.transfer import TransferStatus
from librarian_server.logger import ErrorCategory, ErrorSeverity, log, log_to_database
from librarian_server.orm.sendqueue import LocalSendQueue, RsyncSendQueue, SendQueue

from .settings import background_settings

if TYPE_CHECKING:
    from typing import Callable

    from sqlalchemy.orm import Session


# New plan:
# Two problems - we can't track when things have been successfully popped off the queue
# easily. We also can't, on the recv side, actually see when transfers have completed.
# When send_clone runs, it places things in the transfer queue to be popped off.
# This does nothing other than initiate the transfer.
# During consume_*_queue, transfers are marked as 'ongoing'. We place some stuff
# in the OutgoingTransfer's transfer_data portion that can be used to confirm
# if the transfer actually made it. We could build an AsyncTransferChecker object
# that comes pre-filled? This can then provide three states: COMPLETED, FAILED, WAIT.
# There is a third task, called check_clone, that runs through all INITIATED
# transfers and checks on their status. Now the important thing here is that at this point
# we can call up the downstream server and tell them that their transfer has been STAGED.
# We will also need purning taks for incoming and outoging transfers...
# Need to think also about how this interacts with sneakernet recvs.


def check_on_consumed(
    session_maker: Callable[[], Session],
    complete_status: TransferStatus = TransferStatus.STAGED,
):
    """
    Check on the 'consumed' SendQueue items. Loop through everything with
    consumed = True, and ask to see if their transfers have gone through.

    There are three possible results:

    1. The transfer is still marked as INTIATED, which means that it is
       still ongoing. It is left as-is.
    2. The transfer is marked as COMPLETED. All downstream OutgoingTransfer
       objects will be updated to complete_status.
    3. The transfer is marked as FAILED. All downstream OutgoingTransfer
       objects will be updated to also have been failed.

    Parameters
    ----------

    session_maker: Callable[[], Session]
        A callable that returns a new session object.
    complete_status: TransferStatus
        The status to mark the transfer as if it is complete. By default, this
        is STAGED. All OutgoingTransfer objects will have their status' updated
        in this case.
    """

    with session_maker() as session:
        stmt = select(SendQueue).with_for_update(skip_locked=True)
        stmt = stmt.filter_by(consumed=True).filter_by(completed=False)
        queue_items = session.execute(stmt).scalars().all()

        for queue_item in queue_items:
            current_status = queue_item.async_transfer_manager.transfer_status

            if current_status == TransferStatus.INITIATED:
                continue
            elif current_status == TransferStatus.COMPLETED:
                if complete_status == TransferStatus.STAGED:
                    # TODO: Error handling.
                    queue_item.update_transfer_status(
                        new_status=complete_status,
                        session=session,
                    )
                else:
                    raise ValueError(
                        "No other status than STAGED is supported for checking on consumed"
                    )
            elif current_status == TransferStatus.FAILED:
                for transfer in queue_item.transfers:
                    transfer.fail_transfer(session=session, commit=False)
            else:
                log_to_database(
                    severity=ErrorSeverity.WARNING,
                    category=ErrorCategory.TRANSFER,
                    message=(
                        f"Incompatible return value for transfer status from "
                        f"SendQueue item {queue_item.id} ({current_status})."
                    ),
                    session=session,
                )
                continue

            # If we got down here, we can mark the transfer as consumed.
            queue_item.completed = True
            queue_item.completed_time = datetime.datetime.now(datetime.UTC)

            session.commit()

    return


def consume_queue_item(session_maker: Callable[[], Session]):
    """
    Consume the current, oldest, and highest priority item.
    """

    with session_maker() as session:
        stmt = select(SendQueue).with_for_update(skip_locked=True)
        stmt = stmt.filter_by(completed=False).filter_by(consumed=False)
        stmt = stmt.order_by(SendQueue.priority.desc(), SendQueue.created_time)
        queue_item = session.execute(stmt).scalar()

        if queue_item is None:
            # Nothing to do!
            return

        # Otherwise, we are free to consume this item.
        transfer_list = [
            (Path(x.source_path), Path(x.dest_path)) for x in queue_item.transfers
        ]
        transfer_manager = queue_item.async_transfer_manager
        success = transfer_manager.batch_transfer(transfer_list)

        if success:
            queue_item.consumed = True
            queue_item.consumed_time = datetime.datetime.now(datetime.UTC)

            # Be careful, the internal state of the async transfer manager
            # may have changed. Send it back.
            queue_item.async_transfer_manager = transfer_manager
        else:
            queue_item.retries += 1

        session.commit()

    return
