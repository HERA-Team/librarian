"""
A pydantic model that implements callable so that it can be executed as a task.
"""

import abc
from pydantic import BaseModel

class Task(BaseModel, abc.ABC):
    """
    A task is a pydantic model that allows you to implement very complex
    behaviour to be executed, but can be easily added to the scheduler.

    Your model will always call on_call() when it is executed. It is your
    job to make sure that when that happens either the model can grab
    the state it needs or it already contains it.
    """

    name: str
    "Name of the task to be displayed in the scheduler."
    reschedule_on_failure: bool = True
    "Whether or not to reschedule the task if it fails."

    @abc.abstractmethod
    def on_call(self):
        """
        This function is called when the task is executed.
        """

        raise NotImplementedError("on_call() not implemented.")

    def __call__(self):
        """
        Calls the function with the given keyword arguments.
        """

        return self.on_call()

