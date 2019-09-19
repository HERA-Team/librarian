# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD License

"""Test code in hera_librarian/store.py

"""

from __future__ import print_function, division, absolute_import
import pytest
import os
import json
from hera_librarian import base_store, RPCError

from . import ALL_FILES, filetypes, obsids, md5sums, pathsizes


@pytest.fixture
def local_store():
    return base_store.BaseStore("local_store", "/tmp", "localhost")


def test_path(local_store):
    # test that store path is prepended
    assert local_store._path("my_dir") == "/tmp/my_dir"

    # test passing in an absolute path
    with pytest.raises(ValueError):
        local_store._path("/tmp/my_dir")

    return


def test_ssh_slurp(local_store):
    # test simple command
    assert local_store._ssh_slurp("echo hello world") == "hello world\n"

    # try a bogus command
    with pytest.raises(RPCError):
        local_store._ssh_slurp("foo")

    return


def test_copy_to_store(tmpdir, local_store):
    # make a fake file in our tmpdir
    tmppath = os.path.join(str(tmpdir), "my_file.txt")
    with open(tmppath, "w") as f:
        print("hello world", file=f)

    # copy it over
    local_store.copy_to_store(str(tmpdir), "test_directory")

    # check that it exists
    assert local_store._ssh_slurp("ls /tmp/test_directory") == "my_file.txt\n"

    return


def test_chmod(local_store):
    # make a small test file on the store, then change permissions
    local_store._ssh_slurp("touch /tmp/my_empty_file")
    local_store._chmod("my_empty_file", "664")

    # make sure permissions are correct
    output = local_store._ssh_slurp("ls -l /tmp/my_empty_file")
    perms = output.split(" ")[0]
    assert perms == "-rw-rw-r--"

    return


def test_move(local_store):
    # test moving a file
    # first make sure that target file does not exist
    local_store._ssh_slurp("rm -f /tmp/my_moved_file")
    local_store._ssh_slurp("touch /tmp/my_empty_file")
    local_store._move("my_empty_file", "my_moved_file")
    assert local_store._ssh_slurp("ls /tmp/my_moved_file") == "/tmp/my_moved_file\n"

    # test trying to overwrite a file that already exists
    with pytest.raises(RPCError):
        local_store._ssh_slurp("touch /tmp/my_empty_file")
        local_store._move("my_empty_file", "my_moved_file")

    # remove existing files; test using chmod
    local_store._ssh_slurp("rm -f /tmp/my_empty_file /tmp/my_moved_file")
    local_store._ssh_slurp("touch /tmp/my_empty_file")
    local_store._move("my_empty_file", "my_moved_file", chmod_spec=664)
    output = local_store._ssh_slurp("ls -l /tmp/my_moved_file")
    perms = output.split(" ")[0]
    assert perms == "-rw-rw-r--"

    return


def test_delete(local_store):
    # test removing a file
    local_store._ssh_slurp("touch /tmp/my_empty_file")
    local_store._delete("my_empty_file")
    assert (
        local_store._ssh_slurp(
            "if [ -f /tmp/my_empty_file ]; then echo file_still_exists; fi"
        )
        == ""
    )

    # test deleting a write-protected file
    local_store._ssh_slurp("mkdir /tmp/my_empty_dir; chmod 755 /tmp/my_empty_dir")
    local_store._delete("my_empty_dir", chmod_before=True)
    assert (
        local_store._ssh_slurp(
            "if [ -d /tmp/my_empty_dir ]; then echo dir_still_exists; fi"
        )
        == ""
    )

    return


def test_create_tempdir(local_store):
    # make sure no temp dirs currently exist on host
    local_store._ssh_slurp("rm -rf /tmp/libtmp.*")
    tmppath = local_store._create_tempdir()
    # we don't know exactly what the directory name will be, because a random
    # 6-digit string is appended to the end
    assert tmppath.startswith("libtmp.")
    assert len(tmppath) == len("libtmp.") + 6
    # make sure it exists on the host
    assert (
        local_store._ssh_slurp("ls -d1 /tmp/{}".format(tmppath))
        == "/tmp/{}\n".format(tmppath)
    )

    return


@ALL_FILES
def test_get_info_for_path(local_store, datafiles):
    # copy a datafile to store directory, so we can get its info
    filepaths = sorted(list(map(str, datafiles.listdir())))
    filename = os.path.basename(filepaths[0])
    local_store.copy_to_store(filepaths[0], filename)

    # get the file info and check that it's right
    info = local_store.get_info_for_path(filename)
    # make a dict of the correct answers
    # the uvh5 properties are first in these lists
    correct_dict = {
        "md5": md5sums[0],
        "obsid": obsids[0],
        "type": filetypes[0],
        "size": pathsizes[0],
    }
    assert info == correct_dict

    return


def test_get_space_info(local_store):
    # get the disk information of the store
    info = local_store.get_space_info()
    assert "used" in info.keys()
    assert "available" in info.keys()
    assert "total" in info.keys()
    assert info["used"] + info["available"] == info["total"]

    # test using the cache -- make sure the info is the same
    info_cached = local_store.get_space_info()
    assert info == info_cached

    # we also test the capacity, space_left, and usage_percentage properties
    capacity = local_store.capacity
    space_left = local_store.space_left
    usage_percentage = local_store.usage_percentage
    assert capacity == info["total"]
    assert space_left == info["available"]
    assert usage_percentage == pytest.approx(100.0 * info["used"] / info["total"])

    return
