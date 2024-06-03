"""
Models for user administration.
"""

from typing import Optional

from pydantic import BaseModel

from ..authlevel import AuthLevel


class UserAdministrationCreationRequest(BaseModel):
    """
    A request to create a new user.
    """

    username: str
    "The username of the new user."
    password: str
    "The password of the new user."
    permission: AuthLevel
    "The permission level of the new user."


class UserAdministrationUpdateRequest(BaseModel):
    """
    A request to update a user.
    """

    username: str
    "The username of the user to update."
    password: Optional[str] = None
    "The new password of the user. If not provided, the password will not be changed."
    permission: Optional[AuthLevel] = None


class UserAdministrationDeleteRequest(BaseModel):
    """
    A request to delete a user.
    """

    username: str
    "The username of the user to delete."


class UserAdministrationChangeResponse(BaseModel):
    """
    A response to a user change request.
    """

    success: bool
    "Whether the change was successful."

    username: str
    "The username of the user that was changed."


class UserAdministrationGetRequest(BaseModel):
    """
    A request to get information about a user.
    """

    username: str
    "The username of the user to get information about."


class UserAdministrationGetResponse(BaseModel):
    """
    A response to a user get request.
    """

    username: str
    "The username of the user."

    permission: AuthLevel
    "The permission level of the user."


class UserAdministrationPasswordChange(BaseModel):
    """
    A request to change a user's _own_ password.
    """

    password: str
    "The old password of the user."

    new_password: str
    "The new password of the user."
