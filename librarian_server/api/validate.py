"""
Server endpoints for validating existing files within the librarian.
This can also have a 'chaining' effect, where the server will validate
remote instances too.
"""

import asyncio
from pathlib import Path
from time import perf_counter

from asyncer import asyncify
from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from hera_librarian.errors import ErrorCategory, ErrorSeverity
from hera_librarian.exceptions import LibrarianError, LibrarianHTTPError
from hera_librarian.models.validate import (
    FileValidationFailedResponse,
    FileValidationRequest,
    FileValidationResponse,
    FileValidationResponseItem,
)
from hera_librarian.utils import compare_checksums, get_hash_function_from_hash

from ..database import yield_session
from ..logger import log
from ..orm.file import CorruptFile, File
from ..orm.instance import Instance
from ..orm.librarian import Librarian
from ..settings import server_settings
from .auth import ReadonlyUserDependency

router = APIRouter(prefix="/api/v2/validate")


def calculate_checksum_of_local_copy(
    original_checksum: str,
    original_size: int,
    path_info_function: callable,
    path: Path,
    store_id: int,
    instance_id: int,
):
    start = perf_counter()
    hash_function = get_hash_function_from_hash(original_checksum)
    try:
        path_info = path_info_function(path, hash_function=hash_function)
        response = FileValidationResponseItem(
            librarian=server_settings.name,
            store=store_id,
            instance_id=instance_id,
            original_checksum=original_checksum,
            original_size=original_size,
            current_checksum=path_info.checksum,
            current_size=path_info.size,
            computed_same_checksum=compare_checksums(
                original_checksum, path_info.checksum
            ),
        )
        end = perf_counter()

        log.debug(
            f"Calculated path info for {instance_id} ({path_info.size} B) "
            f"in {end - start:.2f} seconds."
        )

        return [response]
    except FileNotFoundError:
        # A mistakenly 'available' file that is not actually available.
        log.error(
            f"File {path} in store {store_id} marked as available but does not exist."
        )

        return []


def calculate_checksum_of_remote_copies(
    librarian,
    file_name,
):
    start = perf_counter()
    try:
        client = librarian.client()
        responses = client.validate_file(file_name)
        end = perf_counter()

        log.debug(
            f"Validated file {file_name} with librarian {librarian.name} in {end - start:.2f} seconds."
            f"Found {len(responses)} instances."
        )

        return responses
    except (LibrarianHTTPError, LibrarianError):
        log.error(
            f"Failed to validate file {file_name} with librarian {librarian.name}"
        )
        return []


@router.post(
    "/file", response_model=FileValidationResponse | FileValidationFailedResponse
)
async def validate_file(
    request: FileValidationRequest,
    response: Response,
    user: ReadonlyUserDependency,
    session: Session = Depends(yield_session),
):
    """
    Validate a file within the librarian.

    Possible response codes:

    200 - OK.

    Note that the response code DOES NOT indicate whether the file is valid or not.
    The response body will contain the current checksum and the current size of the file.
    It will contain the listed checksum in this librarian's metadata, and the listed size.

    It is up to you to determine whether the file is valid or not using this information.

    Note that this will be a very slow operation! We should be able to speed this up
    by awaiting the responses from other librarians before we go away and try to calculate
    our own.
    """

    log.debug(
        f"Recieved file validation request for {request.file_name} from {user.username}: {request}"
    )

    query = select(File)

    query = query.where(File.name == request.file_name)

    file = session.execute(query).scalar()

    if not file:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return FileValidationFailedResponse(
            reason="This file does not exist in the librarian.",
            suggested_remedy="Check the file name and try again.",
        )

    coroutines = []

    # Call up our neighbours and ask them FIRST.
    # But what we actually have is a list of remote instances. There might
    # be more than one per librarian! First, use the list of remote instances
    # to generate a list of librarians we need to query.
    remote_librarian_ids = set()

    for remote_instance in file.remote_instances:
        remote_librarian_ids.add(remote_instance.librarian_id)

    # Now we can query the database for the librarians we need to query.
    for librarian_id in remote_librarian_ids:
        query = select(Librarian)

        query = query.where(Librarian.id == librarian_id)

        librarian = session.execute(query).scalar()

        if not librarian:
            continue

        # Now we can query the librarian for the file.
        responses = asyncify(calculate_checksum_of_remote_copies)(
            librarian=librarian, file_name=request.file_name
        )

        coroutines.append(responses)

    # For each instance we need to calculate the path info.
    for instance in file.instances:
        if not instance.available:
            continue

        this_checksum_info = asyncify(calculate_checksum_of_local_copy)(
            original_checksum=file.checksum,
            original_size=file.size,
            path_info_function=instance.store.store_manager.path_info,
            path=instance.path,
            store_id=instance.store.id,
            instance_id=instance.id,
        )

        coroutines.append(this_checksum_info)

    checksum_info = await asyncio.gather(*coroutines)

    # Flatten checksum_info
    checksum_info = [item for sublist in checksum_info for item in sublist]

    for info in checksum_info:
        if not info.computed_same_checksum and info.librarian == server_settings.name:
            # Add the corrupt file to the database, though check if we already have
            # it first.
            query = select(CorruptFile).filter(CorruptFile.file_name == file.name)

            corrupt_file = session.execute(query).one_or_none()

            if corrupt_file is not None:
                corrupt_file.count += 1
                session.commit()
                continue
            else:
                corrupt_file = CorruptFile.new_corrupt_file(
                    instance=session.get(Instance, info.instance_id),
                    size=info.current_size,
                    checksum=info.current_checksum,
                )
                session.add(corrupt_file)
                session.commit()

            log.error(
                "File validation failed, the checksums do not match for file "
                "{} in store {}. CorruptFile: {}",
                request.file_name,
                info.store,
                corrupt_file.id,
            )

    return FileValidationResponse(checksum_info)
