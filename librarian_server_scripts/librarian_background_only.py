#!python3

"""
Runs the background tasks from librarian server. Does not actually
run a web server instance.
"""

import argparse as ap

from librarian_background import background
from librarian_server.logger import log

# Do this in if __name__ == "__main__" so we can spawn threads on MacOS...


def main():
    parser = ap.ArgumentParser(
        description="Runs the background tasks from librarian server. Does not actually run a web server instance."
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help="Run the background tasks once and then exit.",
    )

    args = parser.parse_args()

    # Now we can start the background process thread.
    log.info("Starting background process.")

    background(run_once=args.once)

    log.info("Background process finished.")
