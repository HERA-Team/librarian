"""
Check in and modify the states of source and destination
transfers.
"""

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from hera_librarian.models.checkin import (
    CheckinStatusRequest,
    CheckinStatusResponse,
    CheckinUpdateRequest,
    CheckinUpdateResponse,
)
from hera_librarian.transfer import TransferStatus
from librarian_server.orm.transfer import IncomingTransfer, OutgoingTransfer

from ..database import yield_session
from ..logger import log
from .auth import CallbackUserDependency, ReadappendUserDependency, User

router = APIRouter(prefix="/api/v2/checkin")

# Only some transfer states are allowed to be
# changed to. For instance, one cannot change
# a FAILED transfer to INITIATED; that doesn't
# make sense.
ALLOWED_UPDATES = {
    TransferStatus.INITIATED: {
        TransferStatus.ONGOING,
        TransferStatus.STAGED,
        TransferStatus.FAILED,
        TransferStatus.CANCELLED,
    },
    TransferStatus.ONGOING: {
        TransferStatus.STAGED,
        TransferStatus.FAILED,
        TransferStatus.CANCELLED,
    },
    TransferStatus.STAGED: {
        # Complete is a special status that must be handled
        # by other endpoints. You can never complete something
        # because it would require syncronous file moves, etc.
        # This is handled by the RecieveClone background task
        # or truly sychronously during uploads.
        # TransferStatus.COMPLETED,
        TransferStatus.FAILED,
        TransferStatus.CANCELLED,
    },
    TransferStatus.COMPLETED: {},
    TransferStatus.FAILED: {},
    TransferStatus.CANCELLED: {},
}


def modify_transfers_by_id(
    transfer_ids: list[int],
    transfer_type: IncomingTransfer | OutgoingTransfer,
    new_status: TransferStatus,
    session: Session,
    user: User,
):
    """
    Process just one type of transfers for status modification.
    """

    processed = []
    unprocessed = []
    reasons = set()

    for transfer_id in transfer_ids:
        transfer = session.get(transfer_type, transfer_id, with_for_update=True)

        if transfer is None:
            unprocessed.append(transfer_id)
            reasons.add("No destination transfer with the appropriate ID was found.")
            continue

        authorized = user.is_admin

        # IncomingTransfers
        try:
            authorized = (
                authorized
                or (transfer.source == user.username)
                or (transfer.uploader == user.username)
            )
        except AttributeError:
            pass

        # OutgoingTransfers
        try:
            authorized = authorized or (transfer.destination == user.username)
        except AttributeError:
            pass

        if not authorized:
            unprocessed.append(transfer_id)
            reasons.add("You are not authorized to modify the transfer.")
            continue

        if new_status not in ALLOWED_UPDATES.get(transfer.status, {}):
            unprocessed.append(transfer_id)
            reasons.add(
                f"Transition from {transfer.status} to {new_status} "
                "is not permitted."
            )
            continue

        transfer.status = new_status
        processed.append(transfer_id)

    session.commit()

    return processed, unprocessed, reasons


def get_status_by_id(
    transfer_ids: list[int],
    transfer_type: IncomingTransfer | OutgoingTransfer,
    session: Session,
    user: User,
):
    """
    Get the list of status' for a specific type of transfer.
    """

    status = {}

    for transfer_id in transfer_ids:
        transfer = session.get(transfer_type, transfer_id, with_for_update=True)

        authorized = user.is_admin

        # IncomingTransfers
        try:
            authorized = (
                authorized
                or (transfer.source == user.username)
                or (transfer.uploader == user.username)
            )
        except AttributeError:
            pass

        # OutgoingTransfers
        try:
            authorized = authorized or (transfer.destination == user.username)
        except AttributeError:
            pass

        status[transfer_id] = (
            transfer.status if transfer is not None and authorized else None
        )

    return status


@router.post("/update", response_model=CheckinUpdateResponse)
def update(
    request: CheckinUpdateRequest,
    response: Response,
    user: CallbackUserDependency,
    session: Session = Depends(yield_session),
):
    """
    Checkin and update transfer status.

    Note that only administrators are able to change transfers
    they themselves did not create.
    """

    log.debug(f"Recieved checkin update request from {user.username}: {request}")

    processed_incoming, unprocessed_incoming, reasons_incoming = modify_transfers_by_id(
        transfer_ids=request.destination_transfer_ids,
        transfer_type=IncomingTransfer,
        new_status=request.new_status,
        session=session,
        user=user,
    )

    processed_outgoing, unprocessed_outgoing, reasons_outgoing = modify_transfers_by_id(
        transfer_ids=request.source_transfer_ids,
        transfer_type=OutgoingTransfer,
        new_status=request.new_status,
        session=session,
        user=user,
    )

    response = CheckinUpdateResponse(
        modified_destination_transfer_ids=processed_incoming,
        unmodified_destination_transfer_ids=unprocessed_incoming,
        modified_source_transfer_ids=processed_outgoing,
        unmodified_source_transfer_ids=unprocessed_outgoing,
        reasons=reasons_incoming | reasons_outgoing,
    )

    log.debug(f"Responding to checkin request with: {response}.")

    return response


@router.post("/status", response_model=CheckinStatusResponse)
def status(
    request: CheckinStatusRequest,
    response: Response,
    user: CallbackUserDependency,
    session: Session = Depends(yield_session),
):
    """
    Checkin and request the transfer status.
    """

    log.debug(f"Recieved checkin status request from {user.username}: {request}")

    destination_status = get_status_by_id(
        transfer_ids=request.destination_transfer_ids,
        transfer_type=IncomingTransfer,
        session=session,
        user=user,
    )

    source_status = get_status_by_id(
        transfer_ids=request.source_transfer_ids,
        transfer_type=OutgoingTransfer,
        session=session,
        user=user,
    )

    response = CheckinStatusResponse(
        source_transfer_status=source_status,
        destination_transfer_status=destination_status,
    )

    log.debug(f"Responding to checkin request with: {response}.")

    return response
