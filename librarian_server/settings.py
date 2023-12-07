"""
Settings for the librarian server. This is a pydantic model
deserialized from the available librarian config path.
"""

from pydantic import BaseModel, field_validator, ValidationError
from pydantic_settings import BaseSettings

from hera_librarian.stores import StoreNames

from pathlib import Path
import os

class StoreSettings(BaseModel):
    """
    Settings for a store. This is passed to the StoreMetadata ORM
    model to generate a new store.
    """

    store_name: str
    "Name of the store."
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

    secret_key: str
    sqlalchemy_database_uri: str
    sqlalchemy_track_modifications: bool 

    log_level: str = "DEBUG"
    displayed_site_name: str = "Untitled Librarian"
    displayed_site_description: str = "No description set."

    port: int

    add_stores: list[StoreSettings]

    @classmethod
    def from_file(cls, config_path: Path | str) -> "ServerSettings":
        """
        Loads the settings from the given path.
        """

        with open(config_path, "r") as handle:
            return cls.model_validate_json(handle.read())

        
# Automatically create a variable, server_settings, from the environment variable
# on import!

server_settings = ServerSettings.from_file(os.environ["LIBRARIAN_CONFIG_PATH"])