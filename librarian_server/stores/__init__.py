"""
All valid stores.
"""

from enum import Enum

from .core import CoreStore
from .local import LocalStore
from .pathinfo import PathInfo

Stores: dict[int, CoreStore] = {
    0: CoreStore,
    1: LocalStore,
}

StoreNames: dict[str, int] = {
    "core": 0,
    "local": 1,
}

InvertedStoreNames = {v: k for k, v in StoreNames.items()}
