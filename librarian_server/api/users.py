"""
API endpoints for user management.

Obviously, you need admin permissions to use these endpoints, except
the self-password change.
"""

from typing import Optional

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import desc, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from hera_librarian.models.users import (
    UserAdministrationChangeResponse,
    UserAdministrationCreationRequest,
    UserAdministrationDeleteRequest,
    UserAdministrationGetRequest,
    UserAdministrationGetResponse,
    UserAdministrationPasswordChange,
    UserAdministrationUpdateRequest,
)

from ..database import yield_session
from ..logger import log
from ..orm.user import User
from ..settings import server_settings
from .auth import AdminUserDependency, ReadonlyUserDependency, UnauthorizedError

router = APIRouter(prefix="/api/v2/users")


@router.post("/create", response_model=UserAdministrationChangeResponse)
def create(
    request: UserAdministrationCreationRequest,
    user: AdminUserDependency,
    response: Response,
    session: Session = Depends(yield_session),
) -> UserAdministrationChangeResponse:
    """
    Create a new user.

    Must be an admin to use this endpoint.

    Possible response codes:

    - 201: The user was created successfully.
    - 400: The user already exists.
    """

    log.info(
        f"Request to create new user from {user.username}: "
        f"{request.username} with permission {request.permission}"
    )

    try:
        new_user = User.new_user(
            username=request.username,
            password=request.password,
            permission=request.permission,
        )
        session.add(new_user)
        session.commit()
    except IntegrityError:
        log.error(f"User {request.username} already exists.")
        session.rollback()
        response.status_code = status.HTTP_400_BAD_REQUEST
        return UserAdministrationChangeResponse(
            success=False, username=request.username
        )

    response.status_code = status.HTTP_201_CREATED
    return UserAdministrationChangeResponse(success=True, username=request.username)


@router.post("/update", response_model=UserAdministrationChangeResponse)
def update(
    request: UserAdministrationUpdateRequest,
    user: AdminUserDependency,
    response: Response,
    session: Session = Depends(yield_session),
) -> UserAdministrationChangeResponse:
    """
    Update a user.

    Must be an admin to use this endpoint.

    Possible response codes:

    - 200: The user was updated successfully.
    - 400: The user does not exist.
    """

    log.info(
        f"Request to update user from {user.username}: "
        f"{request.username} with permission {request.permission}"
    )

    user = session.get(User, request.username)

    if user is None:
        log.error(f"User {request.username} does not exist.")
        response.response_code = status.HTTP_400_BAD_REQUEST
        return UserAdministrationChangeResponse(
            success=False, username=request.username
        )

    if request.password is not None:
        user.password = user.hash_password(request.password)

    if request.permission is not None:
        user.permission = request.permission

    session.commit()

    return UserAdministrationChangeResponse(success=True, username=request.username)


@router.post("/delete", response_model=UserAdministrationChangeResponse)
def delete(
    request: UserAdministrationDeleteRequest,
    user: AdminUserDependency,
    response: Response,
    session: Session = Depends(yield_session),
) -> UserAdministrationChangeResponse:
    """
    Delete a user.

    Must be an admin to use this endpoint.

    Possible response codes:

    - 200: The user was deleted successfully.
    - 400: The user does not exist.
    """

    log.info(f"Request to delete user from {user.username}: {request}")

    user = session.get(User, request.username)

    if user is None:
        log.error(f"User {request.username} does not exist.")
        response.status_code = status.HTTP_400_BAD_REQUEST
        return UserAdministrationChangeResponse(
            success=False, username=request.username
        )

    session.delete(user)
    session.commit()

    return UserAdministrationChangeResponse(success=True, username=request.username)


@router.post("/get", response_model=UserAdministrationGetResponse)
def get(
    request: UserAdministrationGetRequest,
    user: AdminUserDependency,
    response: Response,
    session: Session = Depends(yield_session),
) -> UserAdministrationGetResponse:
    """
    Get information about a user.

    Must be an admin to use this endpoint.

    Possible response codes:

    - 200: The user was found.
    - 400: The user does not exist.
    """

    log.info(f"Request to get user from {user.username}: {request}")

    user = session.get(User, request.username)

    if user is None:
        log.error(f"User {request.username} does not exist.")
        response.status_code = status.HTTP_400_BAD_REQUEST
        return UserAdministrationGetResponse(username=request.username)

    return UserAdministrationGetResponse(
        username=user.username,
        permission=user.permission,
    )


@router.post("/password_update", response_model=UserAdministrationChangeResponse)
def password_update(
    request: UserAdministrationPasswordChange,
    user: ReadonlyUserDependency,
    response: Response,
    session: Session = Depends(yield_session),
) -> None:
    """
    Update the password of the current user.

    Must be logged in to use this endpoint.

    Possible response codes:

    - 200: The password was updated successfully.
    - 400: The user does not exist.
    - 401: The user is not logged in.
    """

    log.info(f"Request to update password from {user.username}.")

    user = session.get(User, user.username)

    if user is None:
        log.error(f"User {user.username} does not exist.")
        response.status_code = status.HTTP_400_BAD_REQUEST
        return UserAdministrationChangeResponse(success=False, username=user.username)

    user.password = user.hash_password(request.new_password)
    session.commit()

    response.status_code = status.HTTP_200_OK
    return UserAdministrationChangeResponse(success=True, username=user.username)
