"""
Contains endpoints for searching the files uploaded to the librarian.
"""

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from hera_librarian.models.errors import (
    ErrorSearchFailedResponse,
    ErrorSearchRequest,
    ErrorSearchResponse,
    ErrorSearchResponses,
)

from hera_librarian.models.instances import (
    InstanceSearchRequest,
    InstanceSearchResponse,
    InstanceSearchResponses,
    InstanceSearchFailedResponse,
    RemoteInstanceSearchResponse,
    RemoteInstanceSearchResponses,
    RemoteInstanceSearchRequest,
    RemoteInstanceSearchFailedResponse,
)

from hera_librarian.models.search import (
    FileSearchFailedResponse,
    FileSearchRequest,
    FileSearchResponse,
    FileSearchResponses,
)

from ..database import yield_session
from ..logger import log
from ..orm.errors import Error
from ..orm.instance import Instance, RemoteInstance
from ..orm.file import File
from ..orm.librarian import Librarian
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

    # Get the mapping between librarian IDs and names.
    librarian_id_to_name = {}

    for librarian in session.query(Librarian).all():
        librarian_id_to_name[librarian.id] = librarian.name

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
                        librarian_name=librarian_id_to_name[
                            remote_instance.librarian_id
                        ],
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


@router.post(
    "/instance_local",
    response_model=InstanceSearchResponses | InstanceSearchFailedResponse,
)
def instance_local(
    request: InstanceSearchRequest,
    response: Response,
    user: AdminUserDependency,
    session: Session = Depends(yield_session),
):
    """
    Searches for instances based upon the InstanceSearchRequest.

    Possible response codes:

    200 - OK. Search completed successfully.
    404 - No file found to match search criteria.
    """

    log.debug(f"Received instance search request from {user.username}: {request}")

    # Start to build our query.
    query = select(Instance)

    if request.id is not None:
        query = query.where(Instance.id == request.id)

    if request.path is not None:
        query = query.where(Instance.path == request.path)

    if request.deletion_policy is not None:
        query = query.where(Instance.deletion_policy == request.deletion_policy)

    if request.created_time is not None:
        query = query.where(Instance.created_time == request.created_time)

    if request.file_name is not None:
        query = query.where(Instance.file_name == request.file_name)

    if request.store_id is not None:
        query = query.where(Instance.store_id == request.store_id)

    if request.available is not None:
        query = query.where(Instance.available == request.available)

    query = query.order_by(desc(Instance.created_time))
    max_results = max(min(request.max_results, server_settings.max_search_results), 0)
    query = query.limit(max_results)

    results = session.execute(query).scalars().all()

    if len(results) == 0:
        log.debug("No isntances found. Returning 'error'.")
        response.status_code = status.HTTP_404_NOT_FOUND
        return InstanceSearchFailedResponse(
            reason="No instances found.",
            suggested_remedy="Check that you are searching for existing instances",
        )

    # Build the response.
    respond_instances = []

    for result in results:
        respond_instances.append(
            InstanceSearchResponse(
                id=result.id,
                path=result.path,
                deletion_policy=result.deletion_policy,
                created_time=result.created_time,
                file_name=result.file_name,
                store_id=result.store_id,
                available=result.available,
            )
        )

    return InstanceSearchResponses(respond_instances)


@router.post(
    "/instance_remote",
    response_model=RemoteInstanceSearchResponses | RemoteInstanceSearchFailedResponse,
)
def instance_remote(
    request: RemoteInstanceSearchRequest,
    response: Response,
    user: AdminUserDependency,
    session: Session = Depends(yield_session),
):
    """
    Searches for instances based upon the RemoteInstanceSearchRequest.

    Possible response codes:

    200 - OK. Search completed successfully.
    404 - No file found to match search criteria.
    """

    log.debug(f"Received instance search request from {user.username}: {request}")

    # Start to build our query.
    query = select(RemoteInstance)

    if request.id is not None:
        query = query.where(RemoteInstance.id == request.id)

    if request.file_name is not None:
        query = query.where(RemoteInstance.file_name == request.file_name)

    if request.store_id is not None:
        query = query.where(RemoteInstance.store_id == request.store_id)

    if request.librarian_id is not None:
        query = query.where(RemoteInstance.librarian_id == request.librarian_id)

    if request.copy_time is not None:
        query = query.where(RemoteInstance.deletion_policy == request.copy_time)

    if request.sender is not None:
        query = query.where(RemoteInstance.sender == request.sender)

    query = query.order_by(desc(RemoteInstance.copy_time))
    max_results = max(min(request.max_results, server_settings.max_search_results), 0)
    query = query.limit(max_results)

    results = session.execute(query).scalars().all()

    if len(results) == 0:
        log.debug("No isntances found. Returning 'error'.")
        response.status_code = status.HTTP_404_NOT_FOUND
        return RemoteInstanceSearchFailedResponse(
            reason="No instances found.",
            suggested_remedy="Check that you are searching for existing remote instances",
        )

    librarian_id_to_name = {}

    for librarian in session.query(Librarian).all():
        librarian_id_to_name[librarian.id] = librarian.name

    # Build the response.
    respond_instances = []

    for result in results:
        respond_instances.append(
            RemoteInstanceSearchResponse(
                librarian_name=librarian_id_to_name[result.librarian_id],
                copy_time=result.copy_time,
                id=result.id,
                file_name=result.file_name,
                store_id=result.store_id,
                sender=result.sender,
            )
        )

    return RemoteInstanceSearchResponses(respond_instances)
