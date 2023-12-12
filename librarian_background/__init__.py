"""
Background tasks for the librarian. This handles things like purging old
items from the database, communicating with other librarians, etc. It is
in a separate module becuase this 'server' should only be ran once per
instance (but the frontend web server could have many threads that accept
requests!).

This module should be invoked as a separate process.
"""

import time

from .bad import bad
from .poll import poll
from .core import SafeScheduler


def background():
    scheduler = SafeScheduler()
    # Set scheduling...
    scheduler.every(5).seconds.do(poll)
    scheduler.every(10).seconds.do(bad)

    # ...and run it.
    while True:
        try:
            scheduler.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            break
