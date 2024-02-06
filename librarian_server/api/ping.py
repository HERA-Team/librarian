"""
Contains endpoints for pinging and requesting a ping back.
"""

from fastapi import APIRouter

from hera_librarian.models.ping import PingRequest, PingResponse

from ..logger import log
from ..settings import server_settings
from .auth import AdminUserDependency, NoneUserDependency, ReadonlyUserDependency

router = APIRouter(prefix="/api/v2/ping")


@router.post("/", response_model=PingResponse)
def ping(request: PingRequest, user: NoneUserDependency):
    """
    Pings the librarian server. Returns some information about
    the server.
    """

    log.debug(f"Received ping request: {request} from user {user}")

    return PingResponse(
        name=server_settings.displayed_site_name,
        description=server_settings.displayed_site_description,
    )


@router.post("/logged", response_model=PingResponse)
def ping_logged_in(request: PingRequest, user: ReadonlyUserDependency):
    """
    Pings the librarian server. Returns some information about
    the server.
    """

    log.debug(f"Received ping (logged in) request: {request} from user {user}")

    return PingResponse(
        name=server_settings.displayed_site_name,
        description=server_settings.displayed_site_description,
    )


@router.post("/admin", response_model=PingResponse)
def ping_admin(request: PingRequest, user: AdminUserDependency):
    """
    Pings the librarian server. Returns some information about
    the server.
    """

    log.debug(f"Received ping (admin) request: {request} from user {user}")

    return PingResponse(
        name=server_settings.displayed_site_name,
        description=server_settings.displayed_site_description,
    )
