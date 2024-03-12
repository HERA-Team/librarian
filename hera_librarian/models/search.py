"""
Pydantic models for the search endpoint.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, RootModel

from hera_librarian.deletion import DeletionPolicy
from hera_librarian.models.instances import (
    InstanceSearchResponse,
    RemoteInstanceSearchResponse,
)


class FileSearchRequest(BaseModel):
    """
    Represents a file search request.
    """

    name: Optional[str] = None
    "The name of the file to search for."
    create_time_window: Optional[tuple[datetime, ...]] = Field(
        default=None, min_length=2, max_length=2
    )
    "The time window to search for files in. This is a tuple of two datetimes, the first being the start and the second being the end. Note that the datetimes should be in UTC."
    uploader: Optional[str] = None
    "The uploader to search for."
    source: Optional[str] = None
    "The source to search for."
    max_results: int = 64
    "The maximum number of results to return."


class FileSearchResponse(BaseModel):
    """
    Represents a file search response.
    """

    name: str
    "The name of the file."
    create_time: datetime
    "The time the file was created."
    size: int
    "The size of the file in bytes."
    checksum: str
    "The checksum of the file."
    uploader: str
    "The uploader of the file."
    source: str
    "The source of the file."
    instances: list[InstanceSearchResponse]
    "The instances of the file."
    remote_instances: list[RemoteInstanceSearchResponse]
    "The remote instances of the file."


FileSearchResponses = RootModel[list[FileSearchResponse]]


class FileSearchFailedResponse(BaseModel):
    """
    Represents a file search failure response.
    """

    reason: str
    "The reason why the search failed."
    suggested_remedy: str
    "A suggested remedy for the failure."
