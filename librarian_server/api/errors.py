"""
Endpoints for the API that allows the control of errors.

For searching errors, see /search.py.
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.orm import Session

from hera_librarian.models.errors import (ErrorClearRequest,
                                          ErrorClearResponse,
                                          ErrorSearchFailedResponse)
from librarian_server.database import yield_session
from librarian_server.orm import Error, ErrorCategory, ErrorSeverity

router = APIRouter("/api/v2/error")


@router.post("/clear", response_model=ErrorClearResponse | ErrorSearchFailedResponse)
def clear_error(
    request: ErrorClearRequest,
    response: Response,
    session: Session = Depends(yield_session),
):
    """
    Clears an error.

    Possible response codes:

    200 - OK. Error cleared successfully.
    404 - No error found to match search criteria.
    """

    error = session.get(Error, request.id)

    if error is None:
        response.status_code = status.HTTP_404_NOT_FOUND

        return ErrorSearchFailedResponse(
            error_message="No error found to with ID {request.id} to clear.",
            suggested_remedy="Check you are searching for a valid error ID.",
        )

    error.clear(session)

    error = session.get(Error, request.id)

    return ErrorClearResponse(
        id=request.id, cleared_time=error.cleared_time, cleared=error.cleared
    )
