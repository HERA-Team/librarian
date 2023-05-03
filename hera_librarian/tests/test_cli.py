# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD License

"""Test code in hera_librarian/cli.py

"""


import pytest

import hera_librarian
from hera_librarian import cli


def test_die(capsys):
    # test without specifying replacement args
    def assertions(capsys, e):
        result = capsys.readouterr()
        assert e.type == SystemExit
        assert e.value.code == 1
        assert result.err == "error: my error\n"

    with pytest.raises(SystemExit) as e:
        cli.die("my error")
    assertions(capsys, e)

    # test with replacement args
    with pytest.raises(SystemExit) as e:
        cli.die("my %s", "error")
    assertions(capsys, e)


def test_print_table(capsys):
    # define dicts
    dict1 = {"name": "foo", "size": 10}
    dict2 = {"name": "bar", "size": 12}
    dict_list = [dict1, dict2]
    col_list = ["name", "size"]
    col_names = ["Name of file", "Size of file"]

    # test without specifying order
    cli.print_table(dict_list)
    captured = capsys.readouterr()
    stdout = captured.out
    correct_table = """name | size
---- | ----
foo  | 10  
bar  | 12  
"""
    assert stdout == correct_table

    # test without column names
    cli.print_table(dict_list, col_list)
    captured = capsys.readouterr()
    stdout = captured.out
    assert stdout == correct_table

    # test with column names
    cli.print_table(dict_list, col_list, col_names)
    captured = capsys.readouterr()
    stdout = captured.out
    correct_table = """Name of file | Size of file
------------ | ------------
foo          | 10          
bar          | 12          
"""
    assert stdout == correct_table

    # test using the wrong number of column headers
    with pytest.raises(ValueError, match="Number of column headers specified must"):
        cli.print_table(dict_list, col_list, col_names[:1])

    return


def test_sizeof_fmt():
    # test a few known values
    bts = 512
    assert cli.sizeof_fmt(bts) == "512.0 B"

    bts = 1024
    assert cli.sizeof_fmt(bts) == "1.0 kB"

    bts = 1024**2
    assert cli.sizeof_fmt(bts) == "1.0 MB"

    bts = 1024**3
    assert cli.sizeof_fmt(bts) == "1.0 GB"

    bts = 1024**4
    assert cli.sizeof_fmt(bts) == "1.0 TB"

    bts = 1024**5
    assert cli.sizeof_fmt(bts) == "1.0 PB"

    bts = 1024**6
    assert cli.sizeof_fmt(bts) == "1.0 EB"

    bts = 1024**7
    assert cli.sizeof_fmt(bts) == "1.0 ZB"

    bts = 1024**8
    assert cli.sizeof_fmt(bts) == "1.0 YB"

    return


def test_generate_parser():
    ap = cli.generate_parser()

    # make sure we have all the subparsers we're expecting
    available_subparsers = tuple(ap._subparsers._group_actions[0].choices.keys())
    assert "add-file-event" in available_subparsers
    assert "add-obs" in available_subparsers
    assert "launch-copy" in available_subparsers
    assert "assign-sessions" in available_subparsers
    assert "delete-files" in available_subparsers
    assert "locate-file" in available_subparsers
    assert "initiate-offload" in available_subparsers
    assert "offload-helper" in available_subparsers
    assert "search-files" in available_subparsers
    assert "set-file-deletion-policy" in available_subparsers
    assert "stage-files" in available_subparsers
    assert "upload" in available_subparsers

    return


def test_main(script_runner):
    version = hera_librarian.__version__
    ret = script_runner.run("librarian", "-V")
    assert ret.stdout == f"librarian {version}\n"
