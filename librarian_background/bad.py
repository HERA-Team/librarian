"""
A simple example job that dies.
"""

from .task import Task


class Bad(Task):
    """
    A simple background task that polls for new files.
    """

    name: str = "bad"

    def on_call(self):
        raise Exception
        return
