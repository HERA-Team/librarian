"""
Models for instance administration.
"""

from typing import Literal

from pydantic import BaseModel


class InstanceAdministrationDeleteRequest(BaseModel):
    """
    A request to delete a user.
    """

    "The instance id of the instance to delete."
    instance_id: str


class InstanceAdministrationChangeResponse(BaseModel):
    """
    A response to a user change request.
    """

    "Whether the change was successful."
    success: bool

    "The instance name of the instance that was changed."
    instance_id: str

    "The instance type to delete"
    instance_type: Literal["local", "remote"]
