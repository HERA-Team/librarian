"""
Tests for file searches.
"""

import subprocess


def test_simple_name_search(librarian_client_command_line, garbage_file):
    subprocess.call(
        [
            "librarian",
            "upload",
            librarian_client_command_line,
            garbage_file,
            "test_file_for_searching",
        ]
    )

    captured = subprocess.check_output(
        [
            "librarian",
            "search-files",
            librarian_client_command_line,
            "--name=test_file_for_searching",
        ]
    )

    assert "test_file_for_searching" in str(captured)
