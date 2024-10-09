"""
Flask endpoints for the v2 version of the API.

Also includes a helper decorator that makes it easier to write
these endpoints with pydantic models.
"""

from .admin import router as admin_router
from .checkin import router as checkin_router
from .clone import router as clone_router
from .errors import router as error_router
from .ping import router as ping_router
from .search import router as search_router
from .upload import router as upload_router
from .users import router as users_router
from .validate import router as validate_router
