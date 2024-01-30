"""
Background tasks for the librarian. This handles things like purging old
items from the database, communicating with other librarians, etc. It is
in a separate module becuase this 'server' should only be ran once per
instance (but the frontend web server could have many threads that accept
requests!).

This module should be invoked as a separate process.
"""

import time

from .check_integrity import CheckIntegrity
from .core import SafeScheduler
from .create_clone import CreateLocalClone


def background(run_once: bool = False):
    scheduler = SafeScheduler()
    # Set scheduling...
    scheduler.every(12).hours.do(
        CheckIntegrity(name="check_integrity", store_name="local_store", age_in_days=7)
    )
    scheduler.every(12).hours.do(
        CreateLocalClone(
            name="create_clone",
            clone_from="local_store",
            clone_to="local_clone",
            age_in_days=7,
        )
    )

    # ...and run it all on startup.
    scheduler.run_all()

    # ...begin scheduling operations.
    while not run_once:
        try:
            scheduler.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            break
