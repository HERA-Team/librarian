"""
Pydantic modems for the admin endpoints
"""

from datetime import datetime

from pydantic import BaseModel


class AdminCreateFileRequest(BaseModel):
    # File properties
    name: str
    "The unique filename of this file."
    create_time: datetime
    "The time at which this file was placed on the stcaore."
    size: int
    "Size in bytes of the file"
    checksum: str
    "Checksum (MD5 hash) of the file."

    uploader: str
    "Uploader of the file."
    source: str
    "Source of the file."

    # Instance properties
    path: str
    "Path to the instance (full) on the store."
    store_name: str
    "The name of the store that this file is on."


class AdminCreateFileResponse(BaseModel):
    already_exists: bool = False
    "In the case that the file already exists, this will be true."

    file_exists: bool = False
    "If the file exists or not."

    success: bool = False
    "Whether we were totally successful."


class AdminRequestFailedResponse(BaseModel):
    reason: str
    "The reason why the search failed."
    suggested_remedy: str
    "A suggested remedy for the failure."
