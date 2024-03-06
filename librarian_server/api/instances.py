"""
API endpoints for instance management.

Obviously, you need admin permissions to use these endpoints, except
the self-password change.
"""

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import desc, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from hera_librarian.authlevel import AuthLevel
from hera_librarian.models.instances import (
    InstanceAdministrationDeleteRequest,
    InstanceAdministrationChangeResponse,
)

from ..database import yield_session
from ..logger import log
from ..orm.instance import RemoteInstance, Instance
from ..settings import server_settings
from .auth import AdminUserDependency, ReadonlyUserDependency, UnauthorizedError

router = APIRouter(prefix="/api/v2/instances")


@router.post(
    path="/delete_remote_instance", response_model=InstanceAdministrationChangeResponse
)
def delete_remote_instance(
    request: InstanceAdministrationDeleteRequest,
    user: AdminUserDependency,
    response: Response,
    session: Session = Depends(yield_session),
) -> InstanceAdministrationChangeResponse:
    """
    Delete a remote instance.

    Must be an admin to use this endpoint

    Possible responses codes:
    - 201: The instance has been deleted
    - 400: The instance does not exist
    """

    log.info(
        f"Request from {user.username} to delete remote "
        f"instance {request.instance_id}"
    )

    instance = session.get(RemoteInstance, request.instance_id)

    if instance is None:
        log.error(f"Instance does not exist: {request.instance_id}")
        response.response_conde = status.HTTP_400_BAD_REQUEST
        return InstanceAdministrationChangeResponse(
            succes=False, instance_id=request.instance_id
        )
    session.delete(instance)
    session.commit()

    return InstanceAdministrationChangeResponse(
        success=True, instance_id=request.instance_id
    )


@router.post(
    path="/delete_local_instance", response_model=InstanceAdministrationChangeResponse
)
def delete_local_instance(
    request: InstanceAdministrationDeleteRequest,
    user: AdminUserDependency,
    response: Response,
    session: Session = Depends(yield_session),
) -> InstanceAdministrationChangeResponse:
    """
    Delete a local instance.

    Must be an admin to use this endpoint

    Possible responses codes:
    - 201: The instance has been deleted
    - 400: The instance does not exist
    """

    log.info(
        f"Request from {user.username} to delete remote "
        f"instance {request.instance_id}"
    )

    instance = session.get(Instance, request.instance_id)

    if instance is None:
        log.error(f"Instance does not exist: {request.instance_id}")
        response.response_conde = status.HTTP_400_BAD_REQUEST
        return InstanceAdministrationChangeResponse(
            succes=False, instance_id=request.instance_id
        )
    session.delete(instance)
    session.commit()

    return InstanceAdministrationChangeResponse(
        success=True, instance_id=request.instance_id
    )
