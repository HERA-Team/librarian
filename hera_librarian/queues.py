"""
Enum for asynchronous queues.
"""

from enum import Enum


class Queue(Enum):
    """
    The queues that we can send items to.
    """

    LOCAL = "local"
    "The local (i.e. no network involvement) queue."

    RSYNC = "rsync"
    "The rsync queue"

    GLOBUS = "globus"
    "The Globus queue"
