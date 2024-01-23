"""
Client settings.
"""

import os
from pathlib import Path

from pydantic import BaseModel
from pydantic_settings import BaseSettings


class ClientInfo(BaseModel):
    """
    Information for an individual client.
    """

    user: str
    "Your username on this librarian"
    port: int
    "The port of this librarian server"
    host: str
    "The hostname of this librarian server"


class ClientSettings(BaseSettings):
    connections: dict[str, ClientInfo] = {}


# Automatically create a settings object on use.

_settings = None


def load_settings() -> ClientSettings:
    """
    Load the settings from the config file.
    """

    global _settings

    try_paths = [
        Path(os.environ["HL_CLIENT_CONFIG"]),
        Path.home() / ".hl_client.cfg",
        Path.home() / ".hl_client.json",
    ]

    for path in try_paths:
        if path.exists():
            _settings = ClientSettings.from_file(path)
            return _settings

    _settings = ClientSettings()

    return _settings


def __getattr__(name):
    """
    Try to load the settings if they haven't been loaded yet.
    """

    if name == "client_settings":
        global _settings

        if _settings is not None:
            return _settings

        return load_settings()

    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
