"""
Contains endpoints for searching the files uploaded to the librarian.
"""

from typing import Optional

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from hera_librarian.models.errors import (
    ErrorSearchFailedResponse,
    ErrorSearchRequest,
    ErrorSearchResponse,
    ErrorSearchResponses,
)
from hera_librarian.models.search import (
    FileSearchFailedResponse,
    FileSearchRequest,
    FileSearchResponse,
    FileSearchResponses,
    InstanceSearchResponse,
    RemoteInstanceSearchResponse,
)

from ..database import yield_session
from ..logger import log
from ..orm.errors import Error, ErrorCategory, ErrorSeverity
from ..orm.file import File
from ..settings import server_settings
from .auth import AdminUserDependency, ReadonlyUserDependency

router = APIRouter(prefix="/api/v2/search")


@router.post("/file", response_model=FileSearchResponses | FileSearchFailedResponse)
def file(
    request: FileSearchRequest,
    response: Response,
    user: ReadonlyUserDependency,
    session: Session = Depends(yield_session),
):
    """
    Searches for files in the librarian.

    Possible response codes:

    200 - OK. Search completed successfully.
    404 - No file found to match search criteria.
    """

    log.debug(f"Received file search request from user {user.username}: {request}")

    # Start to build our query.
    query = select(File)

    if request.name is not None:
        query = query.where(File.name == request.name)

    if request.create_time_window is not None:
        query = query.where(File.create_time >= request.create_time_window[0])
        query = query.where(File.create_time <= request.create_time_window[1])

    if request.uploader is not None:
        query = query.where(File.uploader == request.uploader)

    if request.source is not None:
        query = query.where(File.source == request.source)

    query = query.order_by(desc(File.create_time))
    max_results = max(min(request.max_results, server_settings.max_search_results), 0)
    query = query.limit(max_results)

    # Execute the query.
    results = session.execute(query).scalars().all()

    if len(results) == 0:
        log.debug(f"No files found. Returning error.")
        response.status_code = status.HTTP_404_NOT_FOUND
        return FileSearchFailedResponse(
            reason="No files found.",
            suggested_remedy="Check that you are searching for the correct file.",
        )

    # Build the response.
    respond_files = []

    for result in results:
        respond_files.append(
            FileSearchResponse(
                name=result.name,
                create_time=result.create_time,
                size=result.size,
                checksum=result.checksum,
                uploader=result.uploader,
                source=result.source,
                instances=[
                    InstanceSearchResponse(
                        path=instance.path,
                        deletion_policy=instance.deletion_policy,
                        created_time=instance.created_time,
                        available=instance.available,
                    )
                    for instance in result.instances
                ],
                remote_instances=[
                    RemoteInstanceSearchResponse(
                        librarian_name=remote_instance.librarian_name,
                        copy_time=remote_instance.copy_time,
                    )
                    for remote_instance in result.remote_instances
                ],
            )
        )

    return FileSearchResponses(respond_files)


@router.post("/error", response_model=ErrorSearchResponses | ErrorSearchFailedResponse)
def error(
    request: ErrorSearchRequest,
    response: Response,
    user: AdminUserDependency,
    session: Session = Depends(yield_session),
):
    """
    Searches for errors based upon the ErrorSearchRequest.

    Possible response codes:

    200 - OK. Search completed successfully.
    404 - No file found to match search criteria.
    """

    log.debug(f"Received error search request from {user.username}: {request}")

    # Start to build our query.
    query = select(Error)

    if request.id is not None:
        query = query.where(Error.id == request.id)

    if request.category is not None:
        query = query.where(Error.category == request.category)

    if request.severity is not None:
        query = query.where(Error.severity == request.severity)

    if request.create_time_window is not None:
        query = query.where(Error.raised_time >= request.create_time_window[0])
        query = query.where(Error.raised_time <= request.create_time_window[1])

    if request.include_resolved is False:
        query = query.where(Error.cleared == False)

    query = query.order_by(desc(Error.raised_time))
    max_results = max(min(request.max_results, server_settings.max_search_results), 0)
    query = query.limit(max_results)

    results = session.execute(query).scalars().all()

    if len(results) == 0:
        log.debug(f"No errors found. Returning 'error'.")
        response.status_code = status.HTTP_404_NOT_FOUND
        return ErrorSearchFailedResponse(
            reason="No errors found.",
            suggested_remedy="Check that you are searching for the correct "
            "errors, or maybe you have a happy librarian!",
        )

    # Build the response.
    respond_errors = []

    for result in results:
        respond_errors.append(
            ErrorSearchResponse(
                id=result.id,
                severity=result.severity,
                category=result.category,
                message=result.message,
                raised_time=result.raised_time,
                cleared_time=result.cleared_time,
                cleared=result.cleared,
                caller=result.caller,
            )
        )

    return ErrorSearchResponses(respond_errors)
