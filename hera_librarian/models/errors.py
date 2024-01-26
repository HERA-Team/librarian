"""
Models for error endpoints.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, RootModel

from ..errors import ErrorCategory, ErrorSeverity


class ErrorRequest(BaseModel):
    """
    A request for a set of errors from the librarian.
    """

    id: Optional[int] = None
    "The ID of the error to search for. If left empty, all errors will be returned."
    category: Optional[ErrorCategory] = None
    "The category of errors to return. If left empty, all errors will be returned."
    severity: Optional[ErrorSeverity] = None
    "The severity of errors to return. If left empty, all errors will be returned."
    create_time_window: Optional[tuple[datetime, ...]] = Field(
        default=None, min_length=2, max_length=2
    )
    "The time window to search for files in. This is a tuple of two datetimes, the first being the start and the second being the end. Note that the datetimes should be in UTC."
    include_resolved: bool = False
    "Whether or not to include resolved errors in the response. By default, we do not."
    max_results: int = 64
    "The number of errors to return."


class ErrorResponse(BaseModel):
    """
    The response model for an individual error. We actually return
    ErrorResponses, defined below, which is a list of these.
    """

    id: int
    "The ID of this error."
    severity: ErrorSeverity
    "The severity of this error."
    category: ErrorCategory
    "The category of this error."
    message: str
    "The message describing this error."
    raised_time: datetime
    "The time at which this error was raised."
    cleared_time: Optional[datetime]
    "The time at which this error was cleared."
    cleared: bool
    "Whether or not this error has been cleared."
    caller: Optional[str]
    "The caller that raised this error."


ErrorResponses = RootModel[list[ErrorResponse]]


class ErrorClearRequest(BaseModel):
    """
    A request to clear an error.
    """

    id: int
    "The ID of the error to clear."


class ErrorClearResponse(BaseModel):
    """
    The response to an error clear request.
    """

    id: int
    "The ID of the error that was cleared."
    cleared_time: datetime
    "The time at which the error was cleared."
    cleared: bool
    "Whether or not the error was cleared."


class ErrorSearchFailedResponse(BaseModel):
    """
    The response to an error search request that failed.
    """

    reason: str
    "The reason the search failed."
    suggested_remedy: str
    "A suggested remedy for the failure."
