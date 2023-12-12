"""
The Librarian v2.0 server.

We have moved from Flask to FastAPI to ensure that web requests can be performed
asynchronously, and that background tasks can work on any available ASGI server.
"""

import os

from .settings import server_settings
from .logger import log
from .database import engine, session

from fastapi import FastAPI

log.info("Starting Librarian v2.0 server.")
log.debug("Creating FastAPI app instance.")

app = FastAPI()

log.debug("Adding API router.")

from .api import upload_router

app.include_router(upload_router)