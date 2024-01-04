"""
Tests that you can successfully upload files to the server.
"""

from pathlib import Path

def test_upload_simple(librarian_client, garbage_file, xprocess):
    librarian_client.upload_file(
        garbage_file,
        Path("test_file"),
        null_obsid=True
    )

