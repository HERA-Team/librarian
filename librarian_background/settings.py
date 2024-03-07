"""
Background task settings.
"""

import abc
import datetime
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from hera_librarian.deletion import DeletionPolicy

from .check_integrity import CheckIntegrity
from .create_clone import CreateLocalClone
from .recieve_clone import RecieveClone
from .send_clone import SendClone

if TYPE_CHECKING:
    from .task import Task

    background_settings: "BackgroundSettings"


class BackgroundTaskSettings(BaseModel, abc.ABC):
    """
    Settings for an individual background task. Generic, should be inherited from for
    specific tasks.
    """

    task_name: str
    "The name of the task. Used for logging purposes."

    every: datetime.timedelta
    "How often to run the task. You can pass in any ``datetime.timedelta`` string, e.g. HH:MM:SS (note leading zeroes are required)."

    soft_timeout: datetime.timedelta | None = None
    "A soft request to timeout the task after this amount of time."

    @abc.abstractproperty
    def task(self) -> "Task":  # pragma: no cover
        raise NotImplementedError


class CheckIntegritySettings(BackgroundTaskSettings):
    """
    Settings for the integrity check task.
    """

    age_in_days: int
    "The age of the items to check, in days."

    store_name: str
    "The name of the store to check."

    @property
    def task(self) -> CheckIntegrity:
        return CheckIntegrity(
            name=self.task_name,
            store_name=self.store_name,
            age_in_days=self.age_in_days,
        )


class CreateLocalCloneSettings(BackgroundTaskSettings):
    """
    Settings for the local clone creation task.
    """

    age_in_days: int
    "The age of the items to check, in days."

    clone_from: str
    "The name of the store to clone from."

    clone_to: str | list[str]
    "The name of the store to clone to."

    files_per_run: int = 1024
    "The number of files to clone per run."

    disable_store_on_full: bool = False
    "If true, will disable the store if it cannot fit a new file. If false, will just skip the file and keep trying."

    @property
    def task(self) -> CreateLocalClone:
        return CreateLocalClone(
            name=self.task_name,
            clone_from=self.clone_from,
            clone_to=self.clone_to,
            age_in_days=self.age_in_days,
            files_per_run=self.files_per_run,
            soft_timeout=self.soft_timeout,
            disable_store_on_full=self.disable_store_on_full,
        )


class SendCloneSettings(BackgroundTaskSettings):
    """
    Settings for the clone sending task.
    """

    destination_librarian: str
    "The destination librarian for this clone."

    age_in_days: int
    "The age of the items to check, in days."

    store_preference: Optional[str]
    "The store to send. If None, send all stores."

    @property
    def task(self) -> SendClone:
        return SendClone(
            name=self.task_name,
            destination_librarian=self.destination_librarian,
            age_in_days=self.age_in_days,
            store_preference=self.store_preference,
        )


class RecieveCloneSettings(BackgroundTaskSettings):
    """
    Settings for the clone receiving task.
    """

    deletion_policy: DeletionPolicy
    "The deletion policy for the incoming files."

    @property
    def task(self) -> RecieveClone:
        return RecieveClone(
            name=self.task_name,
            deletion_policy=self.deletion_policy,
        )


class BackgroundSettings(BaseSettings):
    """
    Background task settings, configurable.
    """

    check_integrity: list[CheckIntegritySettings] = []
    "Settings for the integrity check task."

    create_local_clone: list[CreateLocalCloneSettings] = []
    "Settings for the local clone creation task."

    send_clone: list[SendCloneSettings] = []
    "Settings for the clone sending task."

    recieve_clone: list[RecieveCloneSettings] = []
    "Settings for the clone receiving task."

    model_config = SettingsConfigDict(env_prefix="librarian_background_")

    @classmethod
    def from_file(cls, config_path: Path | str) -> "BackgroundSettings":
        """
        Loads the settings from the given path.
        """

        with open(config_path, "r") as handle:
            return cls.model_validate_json(handle.read())


# Automatically create a settings object on use.

_settings = None


def load_settings() -> BackgroundSettings:
    """
    Load the settings from the config file.
    """

    global _settings

    try_paths = [
        os.environ.get("LIBRARIAN_BACKGROUND_CONFIG", None),
    ]

    for path in try_paths:
        if path is not None:
            path = Path(path)
        else:
            continue

        if path.exists():
            _settings = BackgroundSettings.from_file(path)
            return _settings

    _settings = BackgroundSettings()

    return _settings


def __getattr__(name):
    """
    Try to load the settings if they haven't been loaded yet.
    """

    if name == "background_settings":
        global _settings

        if _settings is not None:
            return _settings

        return load_settings()

    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
