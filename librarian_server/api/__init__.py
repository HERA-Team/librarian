"""
Flask endpoints for the v2 version of the API.

Also includes a helper decorator that makes it easier to write
these endpoints with pydantic models.
"""

from .upload import router as upload_router