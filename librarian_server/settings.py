"""
Settings for the librarian server. This is a pydantic model
deserialized from the available librarian config path.
"""

import os
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import loguru
from notifiers.logging import NotificationHandler
from pydantic import BaseModel, Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL

from .stores import StoreNames

if TYPE_CHECKING:
    server_settings: "ServerSettings"


class StoreSettings(BaseModel):
    """
    Settings for a store. This is passed to the StoreMetadata ORM
    model to generate a new store.
    """

    store_name: str
    "Name of the store."
    ingestable: bool
    "Whether this store is ingestable. If not, it is only used for cloning."
    store_type: int
    "Type of the store."
    store_data: dict
    "Data for the store that is specific to the store type."
    transfer_manager_data: dict[str, dict]
    "Transfer managers for this store."
    async_transfer_manager_data: dict[str, dict] | None = None
    "Asyncronous transfer managers for this store."

    @field_validator("store_type", mode="before")
    def store_type_is_valid(cls, v: str) -> int:
        """
        Validates that the store type is valid.
        """

        if v not in StoreNames:
            raise ValidationError(f"Invalid store type {v}")

        return StoreNames[v]


class LogSettings(BaseModel):
    """
    Settings for the loguru logger.
    """

    files: dict[Path, str] = {}
    "Egress files for the logger. Rotation (e.g. 500 MB, 1 week) is the string."

    # Slack integration; by default disable this. You will need a slack
    # webhook url, and by default we raise all error alerts to slack too.
    slack_webhook_enable: bool = False
    slack_webhook_url: Optional[str] = None
    slack_webhook_url_file: Optional[Path] = None
    slack_webhook_level: str = "ERROR"

    def model_post_init(__context, *args, **kwargs):
        """
        Post initialization for the model.
        """
        if __context.slack_webhook_url_file is not None:
            with open(__context.slack_webhook_url_file, "r") as handle:
                __context.slack_webhook_url = handle.read().strip()

    def setup_logs(self, username: str):
        for file_name, rotation in self.files.items():
            loguru.logger.add(file_name, rotation=rotation, enqueue=True)

        if self.slack_webhook_enable:
            params = {
                "username": username,
                "icon_emoji": ":ledger:",
                "webhook_url": self.slack_webhook_url,
            }

            handler = NotificationHandler("slack", defaults=params)

            format = (
                "Librarian *{level: <8}*\n"
                "> _Module_ `{name}`\n"
                "> _Function_ `{function}:{line}`\n"
                "> *{message}*\n"
                "> _Raised at_ {time: YYYY-MM-DD HH:mm:ss.SSS}\n"
            )

            loguru.logger.add(handler, level=self.slack_webhook_level, format=format)

        return


class ServerSettings(BaseSettings):
    """
    Settings for the librarian server. Note that because this is a BaseSettings
    object, you can overwrite the values in the config file with environment
    variables.
    """

    # Top level name of the server. Should be unique.
    name: str = "librarian_server"

    # Whether to enable debugging features, like the API docs and OpenAPI schema.
    debug: bool = True

    # Encryption key for the server, for connecting to other librarians.
    # Don't write this in the config file, it should be set as an environment
    # variable.
    encryption_key: Optional[str] = None
    encryption_key_file: Optional[Path] = None

    # Database settings.
    database_driver: str = "sqlite"
    database_user: Optional[str] = None
    database_password: Optional[str] = None
    database_host: Optional[str] = None
    database_port: Optional[int] = None
    database: Optional[str] = None

    database_password_file: Optional[Path] = None

    log_level: str = "DEBUG"

    # Display name and description of the site, used in UI only.
    displayed_site_name: str = "Untitled Librarian"
    displayed_site_description: str = "No description set."

    # Host and port to bind to.
    host: str = "0.0.0.0"
    port: int = 8080

    # Stores that the librarian should add or migrate
    add_stores: list[StoreSettings] = []

    # Database migration settings
    alembic_config_path: str = "."
    alembic_path: str = "alembic"

    # Client restraints
    max_search_results: int = 64
    maximal_upload_size_bytes: int = 50_000_000_000  # 50 GB

    # Transfer constraints
    max_async_send_retries: int = 3
    # The maximum number of in-flight asynchronous transfers to
    # a specific destination.
    max_async_inflight_transfers: int = 64

    # Log settings
    log_settings: LogSettings = Field(default_factory=LogSettings)

    # Globus integration; by default disable this. This contains a client ID and
    # login secret (for authenticating with Globus as a service), whether this
    # client is a "native app" or not, as well as the UUID for the local Globus
    # endpoint. That endpoint ID should be duplicated in any async transfer manager
    # definitions we want to use a source. We also save the "local root" for the Globus
    # endpoint, which is the root directory presented to the Globus endpoint and
    # all file transfers using Globus are relative to. For example, if the
    # endpoint is configured so that `/mnt/globus` is the local root, then the
    # path to `/mnt/globus/file.txt` should be given as `/~/file.txt`.  This is
    # important for forming paths that Globus can understand.
    globus_enable: bool = False
    globus_client_id: str = ""
    globus_client_native_app: bool = False
    globus_local_endpoint_id: str = ""
    globus_local_root: str = ""

    globus_client_secret: Optional[str] = None
    globus_client_secret_file: Optional[Path] = None

    model_config = SettingsConfigDict(env_prefix="librarian_server_")

    def model_post_init(__context, *args, **kwargs):
        """
        Read sensitive data from their appropriate files.
        """

        if __context.encryption_key_file is not None:
            with open(__context.encryption_key_file, "r") as handle:
                __context.encryption_key = handle.read().strip()

        if __context.globus_client_secret_file is not None:
            with open(__context.globus_client_secret_file, "r") as handle:
                __context.globus_client_secret = handle.read().strip()

        if __context.database_password_file is not None:
            with open(__context.database_password_file, "r") as handle:
                __context.database_password = handle.read().strip()

    @property
    def sqlalchemy_database_uri(self) -> str:
        """
        The SQLAlchemy database URI.
        """

        return URL.create(
            self.database_driver,
            username=self.database_user,
            password=self.database_password,
            host=self.database_host,
            port=self.database_port,
            database=self.database,
        )

    @classmethod
    def from_file(cls, config_path: Path | str) -> "ServerSettings":
        """
        Loads the settings from the given path.
        """

        with open(config_path, "r") as handle:
            return cls.model_validate_json(handle.read())


# Automatically create a variable, server_settings, from the environment variable
# on _use_!

_settings = None


def load_settings() -> ServerSettings:
    """
    Load the settings from the config file.
    """

    global _settings

    try_paths = [
        os.environ.get("LIBRARIAN_CONFIG_PATH", None),
    ]

    for path in try_paths:
        if path is not None:
            path = Path(path)
        else:
            continue

        if path.exists():
            try:
                _settings = ServerSettings.from_file(path)
            except ValidationError as e:
                print(f"Error loading settings from {path}: {e}")
                raise e

            return _settings
    try:
        _settings = ServerSettings()
    except ValidationError as e:
        print(f"Not all settings have defaults: {e}")
        raise e

    return _settings


def __getattr__(name):
    """
    Try to load the settings if they haven't been loaded yet.
    """

    if name == "HELLO_WORLD":
        return "Hello World!"

    if name == "server_settings":
        global _settings

        if _settings is not None:
            return _settings

        return load_settings()

    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
