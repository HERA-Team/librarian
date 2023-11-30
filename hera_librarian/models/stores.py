"""
Pydantic model for store information.
"""

from pydantic import BaseModel
from ..stores import CoreStore

class StoreRequest(BaseModel):
    """
    Pydantic model for store information.
    """

    stores: list[CoreStore]