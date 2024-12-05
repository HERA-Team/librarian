"""
Background tasks for the librarian. This handles things like purging old
items from the database, communicating with other librarians, etc. It is
in a separate module becuase this 'server' should only be ran once per
instance (but the frontend web server could have many threads that accept
requests!).

This module should be invoked as a separate process.
"""

import time

from .core import SafeScheduler
from .settings import background_settings


def background(run_once: bool = False):
    scheduler = SafeScheduler()
    # Set scheduling...

    all_tasks = (
        background_settings.check_integrity
        + background_settings.create_local_clone
        + background_settings.send_clone
        + background_settings.recieve_clone
        + background_settings.consume_queue
        + background_settings.check_consumed_queue
        + background_settings.outgoing_transfer_hypervisor
        + background_settings.incoming_transfer_hypervisor
        + background_settings.duplicate_remote_instance_hypervisor
        + background_settings.rolling_deletion
    )

    for task in all_tasks:
        scheduler.every(task.every.seconds).seconds.do(task.task)

    # ...and run it all on startup.
    scheduler.run_all()

    # ...begin scheduling operations.
    while not run_once:
        try:
            scheduler.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            break
