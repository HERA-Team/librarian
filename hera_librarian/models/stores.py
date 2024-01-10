"""
Pydantic model for store information.
"""

from pydantic import BaseModel

class StoreRequest(BaseModel):
    """
    Pydantic model for store information.
    """

    stores: list["CoreStore"]