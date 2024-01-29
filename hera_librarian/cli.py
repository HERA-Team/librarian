# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 The HERA Collaboration
# Licensed under the 2-clause BSD License.

"""Module for the librarian command line script.

"""

import argparse
import datetime
import json
import os
import sys
import time
from pathlib import Path
from typing import Optional

import dateutil.parser

from . import LibrarianClient
from .exceptions import (LibrarianClientRemovedFunctionality, LibrarianError,
                         LibrarianHTTPError)
from .settings import client_settings

__version__ = "TEST"


# define some common help strings
_conn_name_help = "Which Librarian to talk to; as in ~/.hl_client.cfg."


def die(fmt, *args):
    """Exit the script with the specifying error string.

    This function will exit the interpreter with code 1 and print the specified
    error message.

    Parameters
    ----------
    fmt : str
        String to be appended to the error message.
    args : str
        If `fmt` contains string substitution, args are unpacked for this purpose.

    Returns
    -------
    None

    """
    if not len(args):
        text = str(fmt)
    else:
        text = fmt % args
    print("error:", text, file=sys.stderr)
    sys.exit(1)


def get_client(conn_name):
    if conn_name not in client_settings.connections:
        die("Connection name {} not found in client settings.".format(conn_name))

    return LibrarianClient.from_info(client_settings.connections[conn_name])


def parse_create_time_window(
    args,
    start_time_name: str = "create_time_start",
    end_time_name: str = "create_time_end",
) -> Optional[tuple[datetime.datetime, datetime.datetime]]:
    """
    Parses a window to search for files between two times.
    """

    create_time_window = None

    if args.create_time_start is not None or args.create_time_end is not None:
        create_time_window = []

        if args.create_time_start is not None:
            create_time_window.append(dateutil.parser.parse(args.create_time_start))
        else:
            create_time_window.append(datetime.datetime.min)

        if args.create_time_end is not None:
            create_time_window.append(dateutil.parser.parse(args.create_time_end))
        else:
            create_time_window.append(datetime.datetime.max)

        create_time_window = tuple(create_time_window)

    return create_time_window


# from https://stackoverflow.com/questions/17330139/python-printing-a-dictionary-as-a-horizontal-table-with-headers
def print_table(dict_list, col_list=None, col_names=None):
    """Pretty print a list of dictionaries as a dynamically sized table.

    Given a list of dictionaries that all contain the same keys, print a
    "pretty" table where each dictionary is a different row in said table. The
    column sizes for each key are dynamically sized based on the length of
    columns and their entries. The user may optionally specify the order of the
    columns, and the column headers that should be used.

    Parameters
    ----------
    dict_list : list of dicts
    col_list : list of str
        These are the names of the dictionary keys to be treated as columns, and
        will be printed in the order given in the list. If not specified, keys are
        printed in alphabetical order.
    col_names : list of str
        Names of column headers. Must be the same size as col_list. If not
        specified, dictionary key names are used.

    Returns
    -------
    None

    Notes
    -----
    Author: Thierry Husson - Use it as you want but don't blame me.

    """
    if col_list is None:
        col_list = sorted(list(dict_list[0].keys()) if dict_list else [])
    if col_names is None:
        myList = [col_list]  # 1st row = header
    else:
        if len(col_names) != len(col_list):
            raise ValueError(
                "Number of column headers specified must match number of columns"
            )
        myList = [col_names]
    for item in dict_list:
        myList.append([str(item[col] or "") for col in col_list])
    # figure out the maximum size for each column
    colSize = [max(list(map(len, col))) for col in zip(*myList)]
    formatStr = " | ".join(["{{:<{}}}".format(i) for i in colSize])
    myList.insert(1, ["-" * i for i in colSize])  # Seperating line
    for item in myList:
        print(formatStr.format(*item))

    return


# from https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
def sizeof_fmt(num, suffix="B"):
    """Format the size of a file in human-readable values.

    Parameters
    ----------
    num : int
        File size in bytes.
    suffix : str
        Suffix to use.

    Returns
    -------
    output : str
        Human readable filesize.

    Notes
    -----
    Follows the Django convention of the web search where base-10 prefixes are used,
    but base-2 counting is done.
    """
    for unit in ["", "k", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1024.0:
            return "{0:3.1f} {1:}{2:}".format(num, unit, suffix)
        num /= 1024.0
    return "{0:.1f} {1:}{2:}".format(num, "Y", suffix)


def add_file_event(args):
    """
    Add a file event to a file in the librarian.
    """

    raise LibrarianClientRemovedFunctionality(
        "add_file_event", "File events are no longer part of the librarian."
    )


def add_obs(args):
    """
    Register a list of files with the librarian.
    """

    raise LibrarianClientRemovedFunctionality(
        "add_obs", "Consider using the 'upload' command instead."
    )


def launch_copy(args):
    """
    Launch a copy from one Librarian to another.
    """

    raise LibrarianClientRemovedFunctionality(
        "launch_copy",
        "This is no longer required as it is handled by the background tasks.",
    )


def assign_sessions(args):
    """
    Tell the Librarian to assign any recent Observations to grouped "observing sessions".
    """

    raise LibrarianClientRemovedFunctionality(
        "assign_sessions", "Observing sessions are no longer tracked."
    )


def check_connections(args):
    """
    Check this host's ability to connect to the other Librarians that have been configured,
    as well as their stores.

    """

    any_failed = False

    for conn_name, conn_info in client_settings.connections.items():
        client = LibrarianClient.from_info(conn_info)

        try:
            client.ping()
            print("Connection to {} ({}) succeeded.".format(conn_name, client.hostname))
        except Exception as e:
            print(
                "Connection to {} ({}) failed: {}".format(conn_name, client.hostname, e)
            )
            any_failed = True

    if any_failed:
        sys.exit(1)


def copy_metadata(args):
    """
    Copy metadata for files from one librarian to another.
    """

    raise LibrarianClientRemovedFunctionality(
        "copy_metadata", "Metadata copying is now handled using background tasks."
    )


def delete_files(args):
    """
    Request to delete instances of files matching a given query.
    """

    raise LibrarianClientRemovedFunctionality(
        "delete_files", "Deletion is currently not available using the client."
    )


def initiate_offload(args):
    """
    Initiate an "offload": move a bunch of file instances from one store to another.
    """

    raise LibrarianClientRemovedFunctionality(
        "initiate_offload", "Offloading is now handled using background tasks."
    )


def locate_file(args):
    """
    Ask the Librarian where to find a file.
    """

    raise NotImplementedError(
        "This needs to be implemented, but requires a change to the Librarian API."
    )


def offload_helper(args):
    """
    Launch this script to implement the "offload" functionality.
    """

    raise LibrarianClientRemovedFunctionality(
        "offload_helper", "Offloading is now handled using background tasks."
    )


def search_files(args):
    """
    Search for files in the librarian.
    """

    if args.search is not None:
        raise LibrarianClientRemovedFunctionality(
            "search_files", "JSON search functionality is removed. See help."
        )

    # Create the search request

    create_time_window = parse_create_time_window(args)

    # Perform the search

    client = LibrarianClient.from_info(client_settings.connections[args.conn_name])

    search_response = client.search_files(
        name=args.name,
        create_time_window=create_time_window,
        uploader=args.uploader,
        source=args.source,
        max_results=args.max_results,
    )

    if len(search_response) == 0:
        print("No results found.")
        return 1

    # Print the results
    for file in search_response:
        print(
            "\033[1m"
            + f"{file.name} ({sizeof_fmt(file.size)}) - {file.create_time} - {file.uploader} - {file.source}"
            + "\033[0m"
        )

        if len(file.instances) == 0:
            print("No instances of this file found.")
        else:
            print("Instances:")

        for instance in file.instances:
            print(
                f"    {instance.path} - {'AVAILABLE' if instance.available else 'NOT AVAILABLE'}"
            )

        if len(file.remote_instances) == 0:
            print("No remote instances of this file found.")
        else:
            print("Remote instances:")

        for remote_instance in file.remote_instances:
            print(f"    {remote_instance.librarian_name}")

    return 0


def set_file_deletion_policy(args):
    """
    Set the "deletion policy" of one instance of this file.
    """

    raise LibrarianClientRemovedFunctionality(
        "set_file_deletion_policy",
        "Deletion is currently not available using the client.",
    )


def stage_files(args):
    """
    Tell the Librarian to stage files onto the local scratch disk.
    """

    raise LibrarianClientRemovedFunctionality(
        "stage_files", "Staging is now handled automatically during upload."
    )


def upload(args):
    """
    Upload a file to a Librarian.
    """
    # Argument validation is pretty simple
    if os.path.isabs(args.dest_store_path):
        die(
            "destination path must be relative to store top; got {}".format(
                args.dest_store_path
            )
        )

    if args.null_obsid and args.meta != "infer":
        die('illegal to specify --null-obsid when --meta is not "infer"')

    if args.meta == "json-stdin":
        raise LibrarianClientRemovedFunctionality(
            "upload::json-stdin", "JSON metadata is no longer supported."
        )
    elif args.meta == "infer":
        pass
    else:
        die("unexpected metadata-gathering method {}".format(args.meta))

    # Let's do it
    client = LibrarianClient.from_info(client_settings.connections[args.conn_name])

    try:
        client.upload(
            local_path=Path(args.local_path),
            dest_path=Path(args.dest_store_path),
            deletion_policy=args.deletion,
        )
    except ValueError as e:
        die("Upload failed, check paths: {}".format(e))
    except LibrarianError as e:
        die("Upload failed, librarian not contactable: {}".format(e))
    except Exception as e:
        die("Upload failed (unknown error): {}".format(e))

    return 0


def search_errors(args):
    """
    Search for errors on the librarian.
    """

    client = get_client(args.conn_name)

    try:
        errors = client.search_errors(
            id=args.id,
            category=args.category,
            severity=args.severity,
            create_time_window=args.create_time_window,
            include_resolved=args.include_resolved,
            max_results=args.max_results,
        )
    except LibrarianHTTPError as e:
        die(f"Unexpected error communicating with the librarian server: {e.reason}")

    if len(errors) == 0:
        print("No errors found.")
        return

    print_table(
        [e.dict() for e in errors],
        col_list=[
            "id",
            "severity",
            "category",
            "message",
            "raised_time",
            "cleared_time",
            "cleared",
            "caller",
        ],
        col_names=[
            "ID",
            "Severity",
            "Category",
            "Message",
            "Raised",
            "Cleared",
            "Cleared Time",
            "Caller",
        ],
    )

    return 0


def clear_error(args):
    """
    Clear an error on the librarian.
    """

    client = get_client(args.conn_name)

    try:
        client.clear_error(args.id)
    except ValueError as e:
        die(f"Unable to find or clear error on the librarian: {e.args[0]}")
    except LibrarianHTTPError as e:
        die(f"Unexpected error communicating with the librarian server: {e.reason}")

    return 0


# make the base parser
def generate_parser():
    """Make a librarian ArgumentParser.

    The `ap` object returned contains subparsers for all librarian sub-commands.

    Parameters
    ----------
    None

    Returns
    -------
    ap : ArgumentParser
    """
    ap = argparse.ArgumentParser(
        description="librarian is a command for interacting with the hera_librarian"
    )
    ap.add_argument(
        "-V",
        "--version",
        action="version",
        version="librarian {}".format(__version__),
        help="Show the librarian version and exit.",
    )

    # add subparsers
    sub_parsers = ap.add_subparsers(metavar="command", dest="cmd")
    config_add_file_event_subparser(sub_parsers)
    config_add_obs_subparser(sub_parsers)
    config_assign_session_subparser(sub_parsers)
    config_check_connections_subparser(sub_parsers)
    config_copy_metadata_subparser(sub_parsers)
    config_delete_files_subparser(sub_parsers)
    config_initiate_offload_subparser(sub_parsers)
    config_offload_helper_subparser(sub_parsers)
    config_launch_copy_subparser(sub_parsers)
    config_locate_file_subparser(sub_parsers)
    config_search_files_subparser(sub_parsers)
    config_set_file_deletion_policy_subparser(sub_parsers)
    config_stage_files_subparser(sub_parsers)
    config_upload_subparser(sub_parsers)
    config_search_errors_subparser(sub_parsers)

    return ap


def config_add_file_event_subparser(sub_parsers):
    # function documentation
    doc = """Add an "event" record for a File known to the Librarian. These records are
    essentially freeform. The event data are specified as "key=value" items on the
    command line after the event type. The data are parsed as JSON before being
    sent to the Librarian. This makes it possible to use data structures like
    JSON-format lists, dictionaries, and so on. However, when the data item is
    intended to simply be a string, make sure that it comes through the shell as a
    JSON string by writing it like 'foo=\\"bar\\"' (without the single quotes).

    """
    hlp = "Add an event record to a file"

    # add sub parser
    sp = sub_parsers.add_parser("add-file-event", description=doc, help=hlp)
    sp.add_argument(
        "conn_name", metavar="CONNECTION-NAME", type=str, help=_conn_name_help
    )
    sp.add_argument(
        "file_path",
        metavar="PATH/TO/FILE",
        type=str,
        help="The path to file in librarian.",
    )
    sp.add_argument(
        "event_type", metavar="EVENT-TYPE", type=str, help="The type of event."
    )
    sp.add_argument(
        "key_vals",
        metavar="key1=val1...",
        type=str,
        nargs="+",
        help="key-value pairs of events.",
    )
    sp.set_defaults(func=add_file_event)

    return


def config_add_obs_subparser(sub_parsers):
    # function documentation
    doc = """Register a list of files with the librarian.

    """
    hlp = "Register a list of files with the librarian"

    # add sub parser
    sp = sub_parsers.add_parser("add-obs", description=doc, help=hlp)
    sp.add_argument(
        "conn_name", metavar="CONNECTION-NAME", type=str, help=_conn_name_help
    )
    sp.add_argument(
        "store_name",
        metavar="NAME",
        help="The 'store' name under which the Librarian knows this computer.",
    )
    sp.add_argument(
        "paths",
        metavar="PATHS",
        nargs="+",
        help="The paths to the files on this computer.",
    )
    sp.set_defaults(func=add_obs)

    return


def config_assign_session_subparser(sub_parsers):
    # function documentation
    doc = """Tell the Librarian to assign any recent Observations to grouped "observing
    sessions". You should only do this if no data are currently being taken,
    because otherwise the currently-active session will be incorrectly described.
    The RTP only ingests data from observations that have been assigned to
    sessions, so this command must be run before the RTP will start working on a
    night's data.

    """
    hlp = "Group observations into sessions"

    # add sub parser
    sp = sub_parsers.add_parser("assign-sessions", description=doc, help=hlp)
    sp.add_argument(
        "--min-start-jd",
        dest="minimum_start_jd",
        metavar="JD",
        type=float,
        help="Only consider observations starting after JD.",
    )
    sp.add_argument(
        "--max-start-jd",
        dest="maximum_start_jd",
        metavar="JD",
        type=float,
        help="Only consider observations starting before JD.",
    )
    sp.add_argument("conn_name", metavar="CONNECTION-NAME", help=_conn_name_help)
    sp.set_defaults(func=assign_sessions)

    return


def config_check_connections_subparser(sub_parsers):
    doc = """Check whether this machine can connect to all the stores of its Librarian
    peers.

    """
    hlp = "Check connectivity to remote stores"

    sp = sub_parsers.add_parser("check-connections", description=doc, help=hlp)
    sp.set_defaults(func=check_connections)

    return


def config_copy_metadata_subparser(sub_parsers):
    doc = """Copy File metadata to another librarian server.

    """
    hlp = "Copy a file's metadata to another libarian"

    # add sub parser
    sp = sub_parsers.add_parser("copy-metadata", description=doc, help=hlp)
    sp.add_argument(
        "source_conn_name",
        metavar="SOURCE-CONNECTION-NAME",
        help="Which Librarian originates the metadata; as in ~/.hl_client.cfg.",
    )
    sp.add_argument(
        "dest_conn_name",
        metavar="DEST-CONNECTION-NAME",
        help="Which Librarian receives the metadata; as in ~/.hl_client.cfg.",
    )
    sp.add_argument(
        "file_name",
        metavar="FILE-NAME",
        help="The name of the file's metadata to copy; need not be a local path.",
    )
    sp.set_defaults(func=copy_metadata)

    return


def config_delete_files_subparser(sub_parsers):
    doc = """Request to delete instances of files matching a given query.

    """
    hlp = "Delete instances of files matching a query"

    # add sub parser
    sp = sub_parsers.add_parser("delete-files", description=doc, help=hlp)
    sp.add_argument(
        "-n",
        "--noop",
        dest="noop",
        action="store_true",
        help="Enable no-op mode: nothing is actually deleted.",
    )
    sp.add_argument(
        "--store",
        metavar="STORE-NAME",
        help="Only delete instances found on the named store.",
    )
    sp.add_argument("conn_name", metavar="CONNECTION-NAME", help=_conn_name_help)
    sp.add_argument(
        "query",
        metavar="QUERY",
        help="The JSON-formatted search identifying files to delete.",
    )
    sp.set_defaults(func=delete_files)

    return


def config_initiate_offload_subparser(sub_parsers):
    # function documentation
    doc = """Initiate an "offload": move a bunch of file instances from one store to
    another. This tool is intended for very specialized circumstances: when you
    are trying to clear out a store so that it can be shut down.

    """
    hlp = "Initiate an 'offload' operation"

    # add sub parser
    sp = sub_parsers.add_parser("initiate-offload", description=doc, help=hlp)
    sp.add_argument("conn_name", metavar="CONNECTION-NAME", help=_conn_name_help)
    sp.add_argument(
        "source_name", metavar="SOURCE-NAME", help="The name of the source store."
    )
    sp.add_argument(
        "dest_name", metavar="DEST-NAME", help="The name of the destination store."
    )
    sp.set_defaults(func=initiate_offload)

    return


def config_launch_copy_subparser(sub_parsers):
    # function documentation
    doc = """Launch a copy from one Librarian to another. Note that the filename
    argument is treated just as the name of a file known to the source Librarian:
    it does NOT have to be a file that exists on this particular machine. The
    source Librarian will look up an existing instance of the file (on any
    available store) and copy it over.

    """
    hlp = "Launch a copy from one Librarian to another"

    # add sub parser
    sp = sub_parsers.add_parser("launch-copy", description=doc, help=hlp)
    sp.add_argument(
        "--dest",
        type=str,
        help="The path in which the file should be stored at the destination. "
        "Default is the same as used locally.",
    )
    sp.add_argument(
        "--pre-staged",
        dest="pre_staged",
        metavar="STORENAME:SUBDIR",
        help="Specify that the data have already been staged at the destination.",
    )
    sp.add_argument(
        "source_conn_name",
        metavar="SOURCE-CONNECTION-NAME",
        help="Which Librarian originates the copy; as in ~/.hl_client.cfg.",
    )
    sp.add_argument(
        "dest_conn_name",
        metavar="DEST-CONNECTION-NAME",
        help="Which Librarian receives the copy; as in ~/.hl_client.cfg.",
    )
    sp.add_argument(
        "file_name",
        metavar="FILE-NAME",
        help="The name of the file to copy; need not be a local path.",
    )
    sp.set_defaults(func=launch_copy)

    return


def config_locate_file_subparser(sub_parsers):
    # function documentation
    doc = """Ask the Librarian where to find a file. The file location is returned
    as an SCP-ready string of the form "<host>:<full-path-on-host>".

    """
    hlp = "Find the location of a given file"

    # add sub parser
    sp = sub_parsers.add_parser("locate-file", description=doc, help=hlp)
    sp.add_argument("conn_name", metavar="CONNECTION-NAME", help=_conn_name_help)
    sp.add_argument("file_name", metavar="PATH", help="The name of the file to locate.")
    sp.set_defaults(func=locate_file)

    return


def config_offload_helper_subparser(sub_parsers):
    # function documentation
    doc = """The Librarian launches this script on stores to implement the "offload"
    functionality. Regular users should never need to run it.

    """
    # add sub parser
    # purposely don't add help for this function, to prevent users from using it accidentally
    sp = sub_parsers.add_parser("offload-helper", description=doc)
    sp.add_argument(
        "--name", required=True, help="Displayed name of the destination store."
    )
    sp.add_argument(
        "--pp", required=True, help='"Path prefix" of the destination store.'
    )
    sp.add_argument(
        "--host", required=True, help="Target SSH host of the destination store."
    )
    sp.add_argument(
        "--destrel",
        required=True,
        help="Destination path, relative to the path prefix.",
    )
    sp.add_argument(
        "local_path",
        metavar="LOCAL-PATH",
        help="The name of the file to upload on this machine.",
    )
    sp.set_defaults(func=offload_helper)

    return


def config_search_files_subparser(sub_parsers):
    # function documentation
    doc = """Search for files in the librarian.

    """
    example = """For documentation of the JSON search format, see
    https://github.com/HERA-Team/librarian/blob/master/librarian_packages/hera_librarian/docs/Searching.md .
    Wrap your JSON in single quotes to prevent your shell from trying to interpret the
    special characters."""
    hlp = "Search for files matching a query"

    # add sub parser
    sp = sub_parsers.add_parser(
        "search-files", description=doc, epilog=example, help=hlp
    )
    sp.add_argument("conn_name", metavar="CONNECTION-NAME", help=_conn_name_help)
    sp.add_argument(
        "--search",
        metavar="JSON-SEARCH",
        help="A JSON search specification; files that match will be displayed.",
        required=False,
    )
    sp.add_argument(
        "-n",
        "--name",
        default=None,
        help="Only search for files with this name.",
    )
    sp.add_argument(
        "--create-time-start",
        help="Search for files who were created after this date and time. Use a parseable date string, if no timezone is specified, UTC is assumed.",
    )
    sp.add_argument(
        "--create-time-end",
        help="Search for files who were created before this date and time. Use a parseable date string, if no timezone is specified, UTC is assumed.",
    )
    sp.add_argument(
        "-u", "--uploader", help="Search for files uploaded by this uploader."
    )
    sp.add_argument(
        "-s", "--source", help="Search for files uploaded from this source."
    )
    sp.add_argument(
        "--max-results",
        type=int,
        default=64,
        help="Maximum number of results to return.",
    )

    sp.set_defaults(func=search_files)

    return


def config_set_file_deletion_policy_subparser(sub_parsers):
    # function documentation
    doc = """Set the "deletion policy" of one instance of this file.

    """
    hlp = "Set the 'deletion policy' of one instance of the specified file"

    # add sub parser
    sp = sub_parsers.add_parser("set-file-deletion-policy", description=doc, help=hlp)
    sp.add_argument(
        "--store",
        metavar="STORE-NAME",
        help="Only alter instances found on the named store.",
    )
    sp.add_argument("conn_name", metavar="CONNECTION-NAME", help=_conn_name_help)
    sp.add_argument(
        "file_name", metavar="FILE-NAME", help="The name of the file to modify."
    )
    sp.add_argument(
        "deletion",
        metavar="POLICY",
        help='The new deletion policy: "allowed" or "disallowed"',
    )
    sp.set_defaults(func=set_file_deletion_policy)

    return


def config_stage_files_subparser(sub_parsers):
    # function documentation
    doc = """Tell the Librarian to stage files onto the local scratch disk. At NRAO,
    this is the Lustre filesystem.

    """
    example = """For documentation of the JSON search format, see
    https://github.com/HERA-Team/librarian/blob/master/librarian_packages/hera_librarian/docs/Searching.md ..
    Wrap your JSON in single quotes to prevent your shell from trying to interpret the
    special characters."""
    hlp = "Stage the files matching a query"

    # add sub parser
    sp = sub_parsers.add_parser(
        "stage-files", description=doc, epilog=example, help=hlp
    )
    sp.add_argument(
        "-w",
        "--wait",
        dest="wait",
        action="store_true",
        help="If specified, do not exit until the staging is done.",
    )
    sp.add_argument("conn_name", metavar="CONNECTION-NAME", help=_conn_name_help)
    sp.add_argument(
        "dest_dir",
        metavar="DEST-PATH",
        help="What directory to put the staged files in.",
    )
    sp.add_argument(
        "search",
        metavar="JSON-SEARCH",
        help="A JSON search specification; files that match will be staged.",
    )
    sp.set_defaults(func=stage_files)

    return


def config_upload_subparser(sub_parsers):
    # function documentation
    doc = """Upload a file to a Librarian. Do NOT use this script if the file that you
   wish to upload is already known to the local Librarian. In that case, use the
   "librarian launch-copy" script -- it will make sure to preserve the
   associated metadata correctly.

   """

    example = """The LOCAL-PATH specifies where to find the source data on this machine, and
   can take any form. The DEST-PATH specifies where the data should be store
   in the Librarian and should look something like "2345678/data.txt". The
   'basename' of DEST-PATH gives the unique filename under which the data
   will be stored. The other pieces (the 'store path'; "2345678" in the
   example) give a subdirectory where the file will be stored on one of the
   Librarian's stores; this location is not meaningful but is helpful for
   grouping related files. Unlike the "cp" command, it is incorrect to give
   the DEST-PATH as just "2345678": that will cause the file to be ingested
   under the name "2345678" with an empty 'store path'.

   """
    hlp = "Upload files to the librarian"

    # add sub parser
    sp = sub_parsers.add_parser("upload", description=doc, epilog=example, help=hlp)
    sp.add_argument(
        "--meta",
        dest="meta",
        default="infer",
        help='How to gather metadata: "json-stdin" or "infer"',
    )
    sp.add_argument(
        "--null-obsid",
        dest="null_obsid",
        action="store_true",
        help="Require the new file to have *no* obsid associate (for maintenance files)",
    )
    sp.add_argument(
        "--deletion",
        dest="deletion",
        default="disallowed",
        help=(
            'Whether the created file instance will be deletable: "allowed" or "disallowed"'
        ),
    )
    sp.add_argument(
        "--pre-staged",
        dest="pre_staged",
        metavar="STORENAME:SUBDIR",
        help="Specify that the data have already been staged at the destination.",
    )
    sp.add_argument(
        "conn_name",
        metavar="CONNECTION-NAME",
        help=_conn_name_help,
    )
    sp.add_argument(
        "local_path",
        metavar="LOCAL-PATH",
        help="The path to the data on this machine.",
    )
    sp.add_argument(
        "dest_store_path",
        metavar="DEST-PATH",
        help='The destination location: combination of "store path" and file name.',
    )
    sp.add_argument(
        "--use_globus",
        dest="use_globus",
        action="store_true",
        help="Specify that we should try to use globus to transfer data.",
    )
    sp.add_argument(
        "--client_id",
        dest="client_id",
        metavar="CLIENT-ID",
        help="The globus client ID.",
    )
    sp.add_argument(
        "--transfer_token",
        dest="transfer_token",
        metavar="TRANSFER-TOKEN",
        help="The globus transfer token.",
    )
    sp.add_argument(
        "--source_endpoint_id",
        dest="source_endpoint_id",
        metavar="SOURCE-ENDPOINT-ID",
        help="The source endpoint ID for the globus transfer.",
    )
    sp.set_defaults(func=upload)

    return


def config_search_errors_subparser(sub_parsers):
    # function documentation
    doc = """Search for errors in the librarian.

    """
    example = """Search for errors matching the query, for instance to find all errors
    with a level of 'CRITICAL', you would use:

    librarian search-errors LIBRARIAN_NAME --severity=critical

    """

    hlp = "Search for errors matching a query"

    from .errors import ErrorCategory, ErrorSeverity

    # add sub parser
    sp = sub_parsers.add_parser(
        "search-errors", description=doc, epilog=example, help=hlp
    )

    sp.add_argument("conn_name", metavar="CONNECTION-NAME", help=_conn_name_help)

    sp.add_argument(
        "--id",
        help="Search for an error with this ID.",
        type=int,
    )

    sp.add_argument(
        "-c",
        "--category",
        type=ErrorCategory,
        choices=list(ErrorCategory),
    )

    sp.add_argument(
        "-s",
        "--severity",
        help="Search for errors with this severity.",
        type=ErrorSeverity,
        choices=list(ErrorSeverity),
    )

    sp.add_argument(
        "--create-time-start",
        help="Search for errors who were created after this date and time. Use a parseable date string, if no timezone is specified, UTC is assumed.",
    )

    sp.add_argument(
        "--create-time-end",
        help="Search for errors who were created before this date and time. Use a parseable date string, if no timezone is specified, UTC is assumed.",
    )

    sp.add_argument(
        "--include-resolved",
        action="store_true",
        help="If this flag is present, include errors that have been cleared in the search. Otherwise, only active errors are returned.",
    )

    sp.add_argument(
        "--max-results",
        type=int,
        default=64,
        help="Maximum number of results to return.",
    )

    sp.set_defaults(func=search_errors)


def config_clear_error_subparser(sub_parsers):
    # function documentation
    doc = """Clear an error on the librarian.

    """
    example = """Clear an error with the given ID:

    librarian clear-error LIBRARIAN_NAME 1234

    """

    hlp = "Clear an error on the librarian"

    # add sub parser
    sp = sub_parsers.add_parser(
        "clear-error", description=doc, epilog=example, help=hlp
    )

    sp.add_argument("conn_name", metavar="CONNECTION-NAME", help=_conn_name_help)

    sp.add_argument(
        "id",
        help="The ID of the error to clear.",
        type=int,
    )

    sp.set_defaults(func=clear_error)


def main():
    # make a parser and run the specified command
    parser = generate_parser()
    parsed_args = parser.parse_args()
    parsed_args.func(parsed_args)

    return


if __name__ == "__main__":
    sys.exit(main())
