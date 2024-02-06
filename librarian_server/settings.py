"""
Settings for the librarian server. This is a pydantic model
deserialized from the available librarian config path.
"""

import os
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from pydantic import BaseModel, ValidationError, field_validator
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

    @field_validator("store_type", mode="before")
    def store_type_is_valid(cls, v: str) -> int:
        """
        Validates that the store type is valid.
        """

        if v not in StoreNames:
            raise ValidationError(f"Invalid store type {v}")

        return StoreNames[v]


class ServerSettings(BaseSettings):
    """
    Settings for the librarian server. Note that because this is a BaseSettings
    object, you can overwrite the values in the config file with environment
    variables.
    """

    database_driver: str = "sqlite"
    database_user: Optional[str] = None
    database_password: Optional[str] = None
    database_host: Optional[str] = None
    database_port: Optional[int] = None
    database: Optional[str] = None

    log_level: str = "DEBUG"
    displayed_site_name: str = "Untitled Librarian"
    displayed_site_description: str = "No description set."

    host: str = "0.0.0.0"
    port: int

    add_stores: list[StoreSettings]

    alembic_config_path: str = "."
    alembic_path: str = "alembic"

    max_search_results: int = 64

    model_config = SettingsConfigDict(env_prefix="librarian_server_")

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
            _settings = ServerSettings.from_file(path)
            return _settings

    _settings = ServerSettings()

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
