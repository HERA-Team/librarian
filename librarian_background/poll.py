"""
A simple 'example' background task that does nothing but print
to the screen.
"""

from .task import Task

class Poll(Task):
    """
    A simple background task that polls for new files.
    """

    name: str = "poll"

    def on_call(self):
        print("Polling for new files...")
        return
