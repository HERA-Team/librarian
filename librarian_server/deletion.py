from . import logger
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
        if text == "disallowed":
            return cls.DISALLOWED
        elif text == "allowed":
            return cls.ALLOWED
        else:
            logger.warn('Unrecognized deletion policy %r; using DISALLOWED', text)
            return cls.DISALLOWED