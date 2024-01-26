"""
Flask endpoints for the v2 version of the API.

Also includes a helper decorator that makes it easier to write
these endpoints with pydantic models.
"""

from .upload import router as upload_router
from .ping import router as ping_router
from .clone import router as clone_router
from .search import router as search_router
from .errors import router as error_router