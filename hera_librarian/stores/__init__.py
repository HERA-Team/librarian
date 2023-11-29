"""
All valid stores.
"""

from .pathinfo import PathInfo
from .core import CoreStore
from .local import LocalStore


from enum import Enum

Stores: dict[int, CoreStore] = {
    0: CoreStore,
    1: LocalStore,
}

StoreNames: dict[str, int] = {
    "core": 0,
    "local": 1,
}