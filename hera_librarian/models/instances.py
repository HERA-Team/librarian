"""
Models for instance administration.
"""

from datetime import datetime
from typing import Optional
from pathlib import Path
from pydantic import BaseModel, RootModel

from hera_librarian.deletion import DeletionPolicy


class InstanceAdministrationDeleteRequest(BaseModel):
    """
    A request to delete a instance.
    """

    "The instance id of the instance to delete."
    id: int


class InstanceAdministrationChangeResponse(BaseModel):
    """
    A response to a user change request.
    """

    "Whether the change was successful."
    success: bool

    "The instance name of the instance that was changed."
    id: int


class InstanceSearchRequest(BaseModel):
    """
    A request for a set of instances from the librarian.
    """

    id: Optional[int] = None
    path: Optional[str] = None
    deletion_policy: Optional[str] = None
    created_time: Optional[datetime] = None
    file_name: Optional[str] = None
    store_id: Optional[int] = None
    available: Optional[bool] = None
    max_results: int = 64


class InstanceSearchResponse(BaseModel):
    """
    The response model for an individual instance. We actually return
    InstanceSearchResponses, defined below, which is a list of these.
    """

    path: Path
    deletion_policy: DeletionPolicy
    created_time: datetime
    available: bool
    id: Optional[int] = None
    file_name: Optional[str] = None
    store_id: Optional[int] = None


InstanceSearchResponses = RootModel[list[InstanceSearchResponse]]


class InstanceSearchFailedResponse(BaseModel):
    """
    The response to an error search request that failed.
    """

    reason: str
    "The reason the search failed."
    suggested_remedy: str
    "A suggested remedy for the failure."


class RemoteInstanceSearchRequest(BaseModel):
    """
    A request for a set of instances from the librarian.
    """

    id: Optional[int] = None
    file_name: Optional[str] = None
    store_id: Optional[int] = None
    librarian_id: Optional[int] = None
    copy_time: Optional[datetime] = None
    sender: Optional[str] = None
    max_results: int = 64


class RemoteInstanceSearchResponse(BaseModel):
    """
    Represents a remote instance in the file search response.
    """

    librarian_name: str
    "The name of the librarian that this instance lives on."
    copy_time: datetime
    "The time at which this instance was copied to the remote librarian."
    id: Optional[int] = None
    file_name: Optional[str] = None
    store_id: Optional[int] = None
    sender: Optional[str] = None


RemoteInstanceSearchResponses = RootModel[list[RemoteInstanceSearchResponse]]


class RemoteInstanceSearchFailedResponse(BaseModel):
    """
    The response to an error search request that failed.
    """

    reason: str
    "The reason the search failed."
    suggested_remedy: str
    "A suggested remedy for the failure."
