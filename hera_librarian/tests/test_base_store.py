# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD License

"""Test code in hera_librarian/store.py

"""


import pytest

import os
import shutil
import tempfile

from hera_librarian import RPCError, base_store

from . import ALL_FILES, filetypes, md5sums, obsids, pathsizes


@pytest.fixture()
def local_store():
    tempdir = tempfile.mkdtemp(dir="/tmp")
    return base_store.BaseStore("local_store", tempdir, "localhost"), tempdir


def test_path(local_store):
    # test that store path is prepended
    dirpath = os.path.join(local_store[1], "my_dir")
    assert local_store[0]._path("my_dir") == dirpath

    # test passing in an absolute path
    with pytest.raises(ValueError, match="store paths must not be absolute"):
        local_store[0]._path("/tmp/my_dir")

    # clean up
    shutil.rmtree(os.path.join(local_store[1]))

    return


def test_ssh_slurp(local_store):
    # test simple command
    assert local_store[0]._ssh_slurp("echo hello world").decode("utf-8") == "hello world\n"

    # try a bogus command
    with pytest.raises(RPCError):
        local_store[0]._ssh_slurp("foo")

    # clean up
    shutil.rmtree(os.path.join(local_store[1]))

    return


def test_copy_to_store(tmp_path, local_store):
    # make a fake file in our tmp_path
    tmppath = tmp_path / "my_file.txt"
    with open(tmppath, "w") as f:
        print("hello world", file=f)

    # copy it over
    local_store[0].copy_to_store(str(tmp_path), "test_directory")

    # check that it exists
    dirpath = os.path.join(local_store[1], "test_directory")
    assert local_store[0]._ssh_slurp(f"ls {dirpath}").decode("utf-8") == "my_file.txt\n"

    # clean up
    shutil.rmtree(os.path.join(local_store[1]))

    return


def test_chmod(local_store):
    # make a small test file on the store, then change permissions
    tempdir = local_store[1]
    temppath = os.path.join(tempdir, "my_empty_file")
    local_store[0]._ssh_slurp(f"touch {temppath}")
    local_store[0]._chmod("my_empty_file", "664")

    # make sure permissions are correct
    output = local_store[0]._ssh_slurp(f"ls -l {temppath}").decode("utf-8")
    perms = output.split(" ")[0]
    assert perms == "-rw-rw-r--"

    # clean up
    shutil.rmtree(os.path.join("/tmp", local_store[1]))

    return


def test_move(local_store):
    # test moving a file
    temppath = os.path.join(local_store[1], "my_empty_file")
    local_store[0]._ssh_slurp(f"touch {temppath}")
    local_store[0]._move("my_empty_file", "my_moved_file")
    temppath2 = os.path.join("/tmp", local_store[1], "my_moved_file")
    assert local_store[0]._ssh_slurp(f"ls {temppath2}").decode("utf-8") == f"{temppath2}\n"

    # test trying to overwrite a file that already exists
    local_store[0]._ssh_slurp(f"touch {temppath}")
    with pytest.raises(RPCError):
        local_store[0]._move("my_empty_file", "my_moved_file")

    # remove existing files; test using chmod
    local_store[0]._ssh_slurp(f"rm -f {temppath} {temppath2}")
    local_store[0]._ssh_slurp(f"touch {temppath}")
    local_store[0]._move("my_empty_file", "my_moved_file", chmod_spec=664)
    output = local_store[0]._ssh_slurp(f"ls -l {temppath2}").decode("utf-8")
    perms = output.split(" ")[0]
    assert perms == "-rw-rw-r--"

    # clean up
    shutil.rmtree(os.path.join(local_store[1]))

    return


def test_delete(local_store):
    # test removing a file
    temppath = os.path.join(local_store[1], "my_empty_file")
    local_store[0]._ssh_slurp(f"touch {temppath}")
    local_store[0]._delete("my_empty_file")
    assert (
        local_store[0]
        ._ssh_slurp(f"if [ -f {temppath} ]; then echo file_still_exists; fi")
        .decode("utf-8")
        == ""
    )

    # test deleting a write-protected file
    tempdir = os.path.join(local_store[1], "my_empty_dir")
    local_store[0]._ssh_slurp("mkdir {0}; chmod 755 {0}".format(tempdir))
    local_store[0]._delete("my_empty_dir", chmod_before=True)
    assert (
        local_store[0]
        ._ssh_slurp(f"if [ -d {tempdir} ]; then echo dir_still_exists; fi")
        .decode("utf-8")
        == ""
    )

    # clean up
    shutil.rmtree(os.path.join(local_store[1]))

    return


def test_create_tempdir(local_store):
    # make sure no temp dirs currently exist on host
    tempdir = os.path.join(local_store[1])
    local_store[0]._ssh_slurp(f"rm -rf {tempdir}/libtmp.*")
    tmppath = local_store[0]._create_tempdir()
    # we don't know exactly what the directory name will be, because a random
    # 6-digit string is appended to the end
    assert tmppath.startswith("libtmp.")
    assert len(tmppath) == len("libtmp.") + 6
    # make sure it exists on the host
    assert (
        local_store[0]._ssh_slurp(f"ls -d1 {tempdir}/{tmppath}").decode("utf-8")
        == f"{tempdir}/{tmppath}\n"
    )

    # clean up
    shutil.rmtree(os.path.join(local_store[1]))

    return


@ALL_FILES
def test_get_info_for_path(local_store, datafiles):
    # copy a datafile to store directory, so we can get its info
    filepaths = sorted(map(str, datafiles.iterdir()))
    filename = os.path.basename(filepaths[0])
    local_store[0].copy_to_store(filepaths[0], filename)

    # get the file info and check that it's right
    info = local_store[0].get_info_for_path(filename)
    # make a dict of the correct answers
    # the uvh5 properties are first in these lists
    correct_dict = {
        "md5": md5sums[0],
        "obsid": obsids[0],
        "type": filetypes[0],
        "size": pathsizes[0],
    }
    assert info == correct_dict

    # clean up
    shutil.rmtree(os.path.join(local_store[1]))

    return


def test_get_space_info(local_store):
    # get the disk information of the store
    info = local_store[0].get_space_info()
    assert "used" in info.keys()
    assert "available" in info.keys()
    assert "total" in info.keys()
    assert info["used"] + info["available"] == info["total"]

    # test using the cache -- make sure the info is the same
    info_cached = local_store[0].get_space_info()
    assert info == info_cached

    # we also test the capacity, space_left, and usage_percentage properties
    capacity = local_store[0].capacity
    space_left = local_store[0].space_left
    usage_percentage = local_store[0].usage_percentage
    assert capacity == info["total"]
    assert space_left == info["available"]
    assert usage_percentage == pytest.approx(100.0 * info["used"] / info["total"])

    # clean up
    shutil.rmtree(os.path.join(local_store[1]))

    return
