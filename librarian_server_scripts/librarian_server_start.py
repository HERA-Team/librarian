#!python3

"""
Runs the librarian server. This is really just an _example_ for how to run
the server; you can run the librarian_server module with any ASGI
server framework (e.g. guivcorn if you needed more threads), and
you can even run the librarian_background module as a separate instance.
"""

import argparse as ap
import os
import subprocess
import sys
from pathlib import Path

from librarian_background import background
from librarian_server.logger import log
from librarian_server.settings import server_settings

# Do this in if __name__ == "__main__" so we can spawn threads on MacOS...


parser = ap.ArgumentParser(
    description=(
        "Librarian server start. Used to start both the server and "
        "background task process simultaneously. If you pass --setup, ",
        "it will also run the setup script with the default arguments. "
        "This is not recommended for production, but is helpful for testing.",
    )
)

parser.add_argument(
    "--setup",
    action="store_true",
    help="Run the setup script before starting the server.",
)

args = parser.parse_args()


def main(setup=args.setup):
    if setup:
        log.info("Running setup script.")
        subprocess.call(
            [sys.executable, Path(__file__).parent / "librarian_server_setup.py"],
            env=os.environ,
        )

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
        host=server_settings.host,
        port=server_settings.port,
        log_level=server_settings.log_level.lower(),
        factory=True,
    )

    log.info("Server shut down.")

    log.info("Waiting for background process to finish.")
    background_process.terminate()
    log.info("Background process finished.")
