from .logger import log
from enum import Enum

class DeletionPolicy(Enum):
    """
    Enumeration for whether or not a file can be deleted from a store.

    Always defaults to 'DISALLOWED' when parsing.
    """ 
    
    DISALLOWED = 0
    ALLOWED = 1

    @classmethod
    def from_str(cls, text: str) -> "DeletionPolicy":
        text = text.lower()
        if text == "disallowed":
            return cls.DISALLOWED
        elif text == "allowed":
            return cls.ALLOWED
        else:
            log.warn(f"Unrecognized deletion policy {text}; using DISALLOWED")
            return cls.DISALLOWED