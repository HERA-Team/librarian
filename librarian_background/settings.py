"""
Background task settings.
"""

import abc
import datetime
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from pydantic import BaseModel, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from hera_librarian.deletion import DeletionPolicy
from librarian_background.hypervisor import (
    DuplicateRemoteInstanceHypervisor,
    IncomingTransferHypervisor,
    OutgoingTransferHypervisor,
)
from librarian_background.rolling_deletion import RollingDeletion

from .check_integrity import CheckIntegrity
from .create_clone import CreateLocalClone
from .queues import CheckConsumedQueue, ConsumeQueue, TransferStatus
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

    send_batch_size: int = 128
    "The number of files to send per batch."

    @property
    def task(self) -> SendClone:
        return SendClone(
            name=self.task_name,
            destination_librarian=self.destination_librarian,
            age_in_days=self.age_in_days,
            store_preference=self.store_preference,
            send_batch_size=self.send_batch_size,
        )


class RecieveCloneSettings(BackgroundTaskSettings):
    """
    Settings for the clone receiving task.
    """

    deletion_policy: DeletionPolicy
    "The deletion policy for the incoming files."

    files_per_run: int = 1024
    "The number of files to process per run."

    @property
    def task(self) -> RecieveClone:
        return RecieveClone(
            name=self.task_name,
            deletion_policy=self.deletion_policy,
        )


class ConsumeQueueSettings(BackgroundTaskSettings):
    """
    Settings for the consume queue task.
    """

    @property
    def task(self) -> ConsumeQueue:
        return ConsumeQueue(
            name=self.task_name,
            soft_timeout=self.soft_timeout,
        )


class CheckConsumedQueueSettings(BackgroundTaskSettings):
    """
    Settings for the check consumed queue task.
    """

    complete_status: TransferStatus = TransferStatus.STAGED
    "The status to set the completed items to."

    @property
    def task(self) -> CheckConsumedQueue:
        return CheckConsumedQueue(
            name=self.task_name,
            complete_status=self.complete_status,
            soft_timeout=self.soft_timeout,
        )


class OutgoingTransferHypervisorSettings(BackgroundTaskSettings):
    """
    Settings for the hypervisor task.
    """

    age_in_days: int
    "The age of the items to check, in days."

    @property
    def task(self) -> OutgoingTransferHypervisor:
        return OutgoingTransferHypervisor(
            name=self.task_name,
            age_in_days=self.age_in_days,
            soft_timeout=self.soft_timeout,
        )


class IncomingTransferHypervisorSettings(BackgroundTaskSettings):
    """
    Settings for the hypervisor task.
    """

    age_in_days: int
    "The age of the items to check, in days."

    @property
    def task(self) -> IncomingTransferHypervisor:
        return IncomingTransferHypervisor(
            name=self.task_name,
            age_in_days=self.age_in_days,
            soft_timeout=self.soft_timeout,
        )


class DuplicateRemoteInstanceHypervisorSettings(BackgroundTaskSettings):
    """
    Settings for the duplicate instance hypervisor task.
    """

    @property
    def task(self) -> DuplicateRemoteInstanceHypervisor:
        return DuplicateRemoteInstanceHypervisor(
            name=self.task_name,
            soft_timeout=self.soft_timeout,
        )


class RollingDeletionSettings(BackgroundTaskSettings):
    """
    Settings for the rolling deletion task
    """

    store_name: str
    age_in_days: float

    number_of_remote_copies: int = 3
    verify_downstream_checksums: bool = True
    mark_unavailable: bool = True
    force_deletion: bool = True

    @property
    def task(self) -> RollingDeletion:
        return RollingDeletion(
            name=self.task_name,
            soft_timeout=self.soft_timeout,
            store_name=self.store_name,
            age_in_days=self.age_in_days,
            number_of_remote_copies=self.number_of_remote_copies,
            verify_downstream_checksums=self.verify_downstream_checksums,
            mark_unavailable=self.mark_unavailable,
            force_deletion=self.force_deletion,
        )


class BackgroundSettings(BaseSettings):
    """
    Background task settings, configurable.
    """

    # Individual background task settings:

    check_integrity: list[CheckIntegritySettings] = []
    "Settings for the integrity check task."

    create_local_clone: list[CreateLocalCloneSettings] = []
    "Settings for the local clone creation task."

    send_clone: list[SendCloneSettings] = []
    "Settings for the clone sending task."

    recieve_clone: list[RecieveCloneSettings] = []
    "Settings for the clone receiving task."

    consume_queue: list[ConsumeQueueSettings] = []
    "Settings for the consume queue task."

    check_consumed_queue: list[CheckConsumedQueueSettings] = []
    "Settings for the check consumed queue task."

    outgoing_transfer_hypervisor: list[OutgoingTransferHypervisorSettings] = []
    incoming_transfer_hypervisor: list[IncomingTransferHypervisorSettings] = []
    duplicate_remote_instance_hypervisor: list[
        DuplicateRemoteInstanceHypervisorSettings
    ] = []

    rolling_deletion: list[RollingDeletionSettings] = []

    # Global settings:

    max_rsync_retries: int = 8

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
            try:
                _settings = BackgroundSettings.from_file(path)
            except ValidationError as e:
                print(f"Error loading settings from {path}: {e}")
                raise e
            return _settings

    try:
        _settings = BackgroundSettings()
    except ValidationError as e:
        print(f"Not all settings have defaults: {e}")
        raise e

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
