"""
Enumeration for authentication levels.
"""

from enum import Enum


class AuthLevel(Enum):
    """
    The level of authorization that a given user has.
    """

    NONE = 0
    "Not used, but in the case where someone is not allowed to do anything."

    READONLY = 1
    "Can read from the databases and store, but not write."

    CALLBACK = 2
    "Can read from the databases and store, and can mark callbacks for outbound messages."

    READAPPEND = 3
    "Can read and append to the databases and store."

    READWRITE = 4
    "Can read and write to the databases and store."

    ADMIN = 100
    "Can do anything, including modifying the configuration."

    def __str__(self) -> str:
        return self.name
