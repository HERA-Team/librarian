"""
Pydantic models for the search endpoint.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from hera_librarian.deletion import DeletionPolicy


class FileSearchRequest(BaseModel):
    """
    Represents a file search request.
    """

    name: Optional[str] = None
    "The name of the file to search for."
    create_time_window: Optional[tuple[datetime]] = Field(default=None, min_length=2, max_length=2)
    "The time window to search for files in. This is a tuple of two datetimes, the first being the start and the second being the end."
    uploader: Optional[str] = None
    "The uploader to search for."
    source: Optional[str] = None
    "The source to search for."
    max_results: Optional[int] = None
    "The maximum number of results to return."


class InstanceSearchResponse(BaseModel):
    """
    Represents an instance in the file search response.
    """
    path: Path
    "The path of the instance."
    deletion_policy: DeletionPolicy
    "The deletion policy of the instance."
    created_time: datetime
    "The time the instance was created."
    available: bool
    "Whether or not the instance is available."


class RemoteInstanceSearchResponse(BaseModel):
    """
    Represents a remote instance in the file search response.
    """
    librarian_name: str
    "The name of the librarian that this instance lives on."
    copy_time: datetime
    "The time at which this instance was copied to the remote librarian."


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
    instances = list[InstanceSearchResponse]
    "The instances of the file."
    remote_instances = list[RemoteInstanceSearchResponse]
    "The remote instances of the file."


class FileSearchFailedResponse(BaseModel):
    """
    Represents a file search failure response.
    """

    reason: str
    "The reason why the search failed."
    suggested_remedy: str
    "A suggested remedy for the failure."