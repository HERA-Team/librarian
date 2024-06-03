"""
Pydantic models for the ping endpoint.
"""

from pydantic import BaseModel


class PingRequest(BaseModel):
    """
    Represents a ping request.
    """

    pass


class PingResponse(BaseModel):
    """
    Represents a ping response.
    """

    name: str
    "The name of this librarian."
    description: str
    "The description of this librarian."

    pass
