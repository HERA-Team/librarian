#!python3

"""
Runs the librarian server. This is really just an _example_ for how to run
the server; you can run the librarian_server module with any ASGI
server framework (e.g. guivcorn if you needed more threads), and
you can even run the librarian_background module as a separate instance.
"""

import subprocess
from pathlib import Path

from librarian_background import background
from librarian_server.database import get_session
from librarian_server.logger import log
from librarian_server.orm import StoreMetadata
from librarian_server.settings import server_settings

# Do this in if __name__ == "__main__" so we can spawn threads on MacOS...


def main():
    # Now we can start the background process thread.
    log.info("Starting background process.")

    from multiprocessing import Process

    background_process = Process(target=background)
    background_process.start()

    # Now we can actually start the server.
    log.info("Creating uvicorn instance.")

    import uvicorn

    uvicorn.run(
        "librarian_server:main",
        port=server_settings.port,
        log_level=server_settings.log_level.lower(),
        factory=True,
    )

    log.info("Server shut down.")

    log.info("Waiting for background process to finish.")
    background_process.terminate()
    log.info("Background process finished.")
