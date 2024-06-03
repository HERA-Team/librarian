"""
Tests that the CLI does what we expect it to.
"""

import subprocess


def test_should_fail_removed():
    """
    Tests that our CLI fails when we have removed functionality.
    """

    calls = [
        ["add-file-event", "ABC", "/path/to/nowhere", "A", "AB=CD"],
        ["add-obs", "ABC", "STORE", "/path/to/nowhere"],
        ["assign-sessions", "ABC", "--min-start-jd=2400", "--max-start-jd=2450"],
        ["copy-metadata", "ABC", "DEF", "abcd"],
        ["delete-files", "DBC", "QUERY", "--store=ABC"],
        ["initiate-offload", "ABC", "SOURCE", "DEST"],
        ["launch-copy", "SOURCE", "DEST", "FILE"],
        [
            "offload-helper",
            "LOCAL",
            "--name=HELLO",
            "--pp=NONE",
            "--host=HOST",
            "--destrel=REL",
        ],
        ["set-file-deletion-policy", "CONN", "FILENAME", "DISALLOWED", "--store=hello"],
        ["stage-files", "ABC", "DEF", "GHI"],
    ]

    for call in calls:
        with subprocess.Popen(
            ["librarian", *call],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ) as proc:
            _, stderr = proc.communicate()

            assert proc.returncode != 0
            assert b"LibrarianClientRemovedFunctionality" in stderr
