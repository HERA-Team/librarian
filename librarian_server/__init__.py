"""
The Librarian v2.0 server.

We have moved from Flask to FastAPI to ensure that web requests can be performed
asynchronously, and that background tasks can work on any available ASGI server.
"""

from .settings import server_settings
from fastapi import FastAPI

def main() -> FastAPI:
    from .logger import log

    log.info("Starting Librarian v2.0 server.")
    log.debug("Creating FastAPI app instance.")

    app = FastAPI()

    log.debug("Adding API router.")

    from .api import upload_router, ping_router, clone_router

    app.include_router(upload_router)
    app.include_router(ping_router)
    app.include_router(clone_router)

    return app