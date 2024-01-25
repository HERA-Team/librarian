"""
Tests the search-files parser.
"""

import datetime

import dateutil.parser

from hera_librarian import cli


def test_parser_simple_name():
    parser = cli.generate_parser()

    args = parser.parse_args(
        [
            "search-files",
            "fake_connection",
            "--name=test_file",
        ]
    )

    assert args.name == "test_file"


def test_parser_lots():
    parser = cli.generate_parser()

    args = parser.parse_args(
        [
            "search-files",
            "fake_connection",
            "--name=test_file",
            "--create-time-start=2020-01-01",
            "--create-time-end=2020-01-02",
            "--uploader=uploader",
            "--source=source",
            "--max-results=10",
        ]
    )

    assert args.name == "test_file"
    assert dateutil.parser.parse(args.create_time_start) == datetime.datetime(
        year=2020, month=1, day=1
    )
    assert dateutil.parser.parse(args.create_time_end) == datetime.datetime(
        year=2020, month=1, day=2
    )
    assert args.uploader == "uploader"
    assert args.source == "source"
    assert args.max_results == 10
