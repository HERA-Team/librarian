"""
Contains endpoints for searching the files uploaded to the librarian.
"""

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from hera_librarian.models.search import (FileSearchFailedResponse,
                                          FileSearchRequest,
                                          FileSearchResponse,
                                          FileSearchResponses,
                                          InstanceSearchResponse,
                                          RemoteInstanceSearchResponse)

from ..database import yield_session
from ..logger import log
from ..orm.file import File
from ..orm.instance import Instance, RemoteInstance
from ..settings import server_settings

router = APIRouter(prefix="/api/v2/search")


@router.post("/file", response_model=FileSearchResponses | FileSearchFailedResponse)
def file(
    request: FileSearchRequest,
    response: Response,
    session: Session = Depends(yield_session),
):
    """
    Searches for files in the librarian.

    Possible response codes:

    200 - OK. Search completed successfully.
    404 - No file found to match search criteria.
    """

    log.debug(f"Received file search request: {request}")

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

    query.order_by(File.create_time)
    query.limit(max(min(request.max_results, server_settings.max_search_results), 0))

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
