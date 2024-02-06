"""
Authenticaiton helper functions for the web API.

You should really care about the following dependencies:

- NoneUserDependency 
- ReadonlyUserDependency 
- ReadappendUserDependency 
- ReadwriteUserDependency 
- AdminUserDependency 

These are used to ensure that the user is authenticated with the correct level
of permissions.  If they are not, we raise a HTTPException (see
UnauthorizedError).
"""

from typing import Annotated

from fastapi import Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..authlevel import AuthLevel
from ..database import yield_session
from ..orm import User

security = HTTPBasic()

SecurityDepedency = Annotated[HTTPBasicCredentials, Depends(security)]
SessionDependency = Annotated[Session, Depends(yield_session)]

UnauthorizedError = HTTPException(
    status_code=401,
    detail="Incorrect username or password",
    headers={"WWW-Authenticate": "Basic"},
)


class UserPermissions(BaseModel):
    """
    A simple model to represent a user and their permission.
    """

    username: str
    "The username of the user."
    permission: AuthLevel
    "The permission level of the user."


def get_user(
    credentials: SecurityDepedency, session: SessionDependency
) -> UserPermissions:
    """
    Get the user and their permissions from the database.
    """

    return UserPermissions(
        username=credentials.username,
        permission=User.check_user(
            username=credentials.username,
            password=credentials.password,
            session=session,
        ),
    )


def get_user_with_level(
    level: AuthLevel, credentials: SecurityDepedency, session: SessionDependency
) -> UserPermissions:
    """
    Get the user and their permissions from the database.
    If the user does not have the required level, raise an UnauthorizedError.
    """

    user = get_user(credentials, session)

    if user.permission.value < level.value:
        raise UnauthorizedError

    return user


def get_user_with_none(
    credentials: SecurityDepedency, session: SessionDependency
) -> UserPermissions:
    """
    Ensure user is authenticated with a level of at least NONE.
    """

    return get_user_with_level(AuthLevel.NONE, credentials, session)


def get_user_with_readonly(
    credentials: SecurityDepedency, session: SessionDependency
) -> UserPermissions:
    """
    Ensure user is authenticated with a level of at least READONLY.
    """

    return get_user_with_level(AuthLevel.READONLY, credentials, session)


def get_user_with_callback(
    credentials: SecurityDepedency, session: SessionDependency
) -> UserPermissions:
    """
    Ensure user is authenticated with a level of at least CALLBACK.
    """

    return get_user_with_level(AuthLevel.CALLBACK, credentials, session)


def get_user_with_readappend(
    credentials: SecurityDepedency, session: SessionDependency
) -> UserPermissions:
    """
    Ensure user is authenticated with a level of at least READAPPEND.
    """

    return get_user_with_level(AuthLevel.READAPPEND, credentials, session)


def get_user_with_readwrite(
    credentials: SecurityDepedency, session: SessionDependency
) -> UserPermissions:
    """
    Ensure user is authenticated with a level of at least READWRITE.
    """

    return get_user_with_level(AuthLevel.READWRITE, credentials, session)


def get_user_with_admin(
    credentials: SecurityDepedency, session: SessionDependency
) -> UserPermissions:
    """
    Ensure user is authenticated with a level of at least ADMIN.
    """

    return get_user_with_level(AuthLevel.ADMIN, credentials, session)


NoneUserDependency = Annotated[UserPermissions, Depends(get_user_with_none)]
ReadonlyUserDependency = Annotated[UserPermissions, Depends(get_user_with_readonly)]
CallbackUserDependency = Annotated[UserPermissions, Depends(get_user_with_callback)]
ReadappendUserDependency = Annotated[UserPermissions, Depends(get_user_with_readappend)]
ReadwriteUserDependency = Annotated[UserPermissions, Depends(get_user_with_readwrite)]
AdminUserDependency = Annotated[UserPermissions, Depends(get_user_with_admin)]
