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


def background():
    scheduler = SafeScheduler()
    # Set scheduling...
    scheduler.every(12).hours.do(CheckIntegrity(name="check_integrity", store_name="local_store", age_in_days=7))

    # ...and run it all on startup.
    scheduler.run_all()

    # ...begin scheduling operations.
    while True:
        try:
            scheduler.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            break
