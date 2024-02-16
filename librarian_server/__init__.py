"""
The Librarian v2.0 server.

We have moved from Flask to FastAPI to ensure that web requests can be performed
asynchronously, and that background tasks can work on any available ASGI server.
"""

from fastapi import FastAPI

from .settings import server_settings


def main() -> FastAPI:
    from .logger import log

    log.info("Starting Librarian v2.0 server.")
    log.debug("Creating FastAPI app instance.")

    app = FastAPI()

    log.debug("Adding API router.")

    from .api import (
        admin_router,
        clone_router,
        error_router,
        ping_router,
        search_router,
        upload_router,
        users_router,
    )

    app.include_router(upload_router)
    app.include_router(ping_router)
    app.include_router(clone_router)
    app.include_router(search_router)
    app.include_router(error_router)
    app.include_router(users_router)
    app.include_router(admin_router)

    return app
