"""
Models for validation requests and responses.
"""

from pydantic import BaseModel, RootModel


class FileValidationRequest(BaseModel):
    """
    A request to validate a file.
    """

    file_name: str
    "The name of the file to validate."


class FileValidationResponseItem(BaseModel):
    """
    Information about a file validation response.
    """

    librarian: str
    "The name of the librarian."
    store: int
    "The store ID."
    instance_id: int
    "The instance ID."
    original_checksum: str
    "The original checksum of the file."
    original_size: int
    "The original size of the file."
    current_checksum: str
    "The current checksum of the file."
    current_size: int
    "The current size of the file."
    computed_same_checksum: bool
    "Whether the checksums are the same (though you should validate this yourself)"


FileValidationResponse = FileSearchResponses = RootModel[
    list[FileValidationResponseItem]
]


class FileValidationFailedResponse(BaseModel):
    """
    A response to a failed file validation request.
    """

    reason: str
    "The reason the file validation failed."
    suggested_remedy: str
    "A suggested remedy for the failure."
