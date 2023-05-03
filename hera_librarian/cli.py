# Copyright 2019 The HERA Collaboration
# Licensed under the 2-clause BSD License.

"""Module for the librarian command line script.

"""


import argparse
import json
import os
import sys
import time

from . import LibrarianClient, RPCError, __version__, base_store, utils

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
    text = fmt % args if len(args) else str(fmt)
    print("error:", text, file=sys.stderr)
    sys.exit(1)


# from https://stackoverflow.com/questions/17330139/
#            python-printing-a-dictionary-as-a-horizontal-table-with-headers
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
        The list of dictionaries to be printed.
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
    elif len(col_names) == len(col_list):
        myList = [col_names]
    else:
        raise ValueError("Number of column headers specified must match number of columns")
    for item in dict_list:
        myList.append([str(item[col] or "") for col in col_list])
    # figure out the maximum size for each column
    colSize = [max(list(map(len, col))) for col in zip(*myList)]
    formatStr = " | ".join([f"{{:<{i}}}" for i in colSize])
    myList.insert(1, ["-" * i for i in colSize])  # Seperating line
    for item in myList:
        print(formatStr.format(*item))

    return


# from https://stackoverflow.com/questions/1094841/
#                     reusable-library-to-get-human-readable-version-of-file-size
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
            return f"{num:3.1f} {unit}{suffix}"
        num /= 1024.0
    return "{:.1f} {}{}".format(num, "Y", suffix)


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
        version=f"librarian {__version__}",
        help="Show the librarian version and exit.",
    )

    # add subparsers
    sub_parsers = ap.add_subparsers(metavar="command", dest="cmd")
    config_add_file_event_subparser(sub_parsers)
    config_add_obs_subparser(sub_parsers)
    config_assign_session_subparser(sub_parsers)
    config_check_connections_subparser(sub_parsers)
    config_delete_files_subparser(sub_parsers)
    config_initiate_offload_subparser(sub_parsers)
    config_offload_helper_subparser(sub_parsers)
    config_launch_copy_subparser(sub_parsers)
    config_locate_file_subparser(sub_parsers)
    config_search_files_subparser(sub_parsers)
    config_set_file_deletion_policy_subparser(sub_parsers)
    config_stage_files_subparser(sub_parsers)
    config_upload_subparser(sub_parsers)

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
    sp.add_argument("connection_name", metavar="CONNECTION-NAME", type=str, help=_conn_name_help)
    sp.add_argument(
        "file_path", metavar="PATH/TO/FILE", type=str, help="The path to file in librarian."
    )
    sp.add_argument("event_type", metavar="EVENT-TYPE", type=str, help="The type of event.")
    sp.add_argument(
        "key_vals", metavar="key1=val1...", type=str, nargs="+", help="key-value pairs of events."
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
    sp.add_argument("conn_name", metavar="CONNECTION-NAME", type=str, help=_conn_name_help)
    sp.add_argument(
        "store_name",
        metavar="NAME",
        help="The 'store' name under which the Librarian knows this computer.",
    )
    sp.add_argument(
        "paths", metavar="PATHS", nargs="+", help="The paths to the files on this computer."
    )
    sp.add_argument(
        "--null-obsid",
        dest="null_obsid",
        action="store_true",
        help="Require the new file to have *no* obsid associated (for maint. files)",
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
        "--store", metavar="STORE-NAME", help="Only delete instances found on the named store."
    )
    sp.add_argument("conn_name", metavar="CONNECTION-NAME", help=_conn_name_help)
    sp.add_argument(
        "query", metavar="QUERY", help="The JSON-formatted search identifying files to delete."
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
    sp.add_argument("source_name", metavar="SOURCE-NAME", help="The name of the source store.")
    sp.add_argument("dest_name", metavar="DEST-NAME", help="The name of the destination store.")
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
    # purposely don't add help for this function, to prevent users
    # from using it accidentally
    sp = sub_parsers.add_parser("offload-helper", description=doc)
    sp.add_argument("--name", required=True, help="Displayed name of the destination store.")
    sp.add_argument("--pp", required=True, help='"Path prefix" of the destination store.')
    sp.add_argument("--host", required=True, help="Target SSH host of the destination store.")
    sp.add_argument(
        "--destrel", required=True, help="Destination path, relative to the path prefix."
    )
    sp.add_argument(
        "local_path", metavar="LOCAL-PATH", help="The name of the file to upload on this machine."
    )
    sp.set_defaults(func=offload_helper)

    return


def config_search_files_subparser(sub_parsers):
    # function documentation
    doc = """Search for files in the librarian.

    """
    _url = (
        "https://github.com/HERA-Team/librarian/blob/master/librarian_packages/"
        + "hera_librarian/docs/Searching.md"
    )
    example = f"""For documentation of the JSON search format, see
    {_url} .
    Wrap your JSON in single quotes to prevent your shell from trying to interpret the
    special characters."""
    hlp = "Search for files matching a query"

    # add sub parser
    sp = sub_parsers.add_parser("search-files", description=doc, epilog=example, help=hlp)
    sp.add_argument("conn_name", metavar="CONNECTION-NAME", help=_conn_name_help)
    sp.add_argument(
        "search",
        metavar="JSON-SEARCH",
        help="A JSON search specification; files that match will be displayed.",
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
        "--store", metavar="STORE-NAME", help="Only alter instances found on the named store."
    )
    sp.add_argument("conn_name", metavar="CONNECTION-NAME", help=_conn_name_help)
    sp.add_argument("file_name", metavar="FILE-NAME", help="The name of the file to modify.")
    sp.add_argument(
        "deletion", metavar="POLICY", help='The new deletion policy: "allowed" or "disallowed"'
    )
    sp.set_defaults(func=set_file_deletion_policy)

    return


def config_stage_files_subparser(sub_parsers):
    # function documentation
    doc = """Tell the Librarian to stage files onto the local scratch disk. At NRAO,
    this is the Lustre filesystem.

    """
    _url = (
        "https://github.com/HERA-Team/librarian/blob/master/librarian_packages/"
        "hera_librarian/docs/Searching.md"
    )
    example = f"""For documentation of the JSON search format, see
    {_url} .
    Wrap your JSON in single quotes to prevent your shell from trying to interpret the
    special characters."""
    hlp = "Stage the files matching a query"

    # add sub parser
    sp = sub_parsers.add_parser("stage-files", description=doc, epilog=example, help=hlp)
    sp.add_argument(
        "-w",
        "--wait",
        dest="wait",
        action="store_true",
        help="If specified, do not exit until the staging is done.",
    )
    sp.add_argument("conn_name", metavar="CONNECTION-NAME", help=_conn_name_help)
    sp.add_argument(
        "dest_dir", metavar="DEST-PATH", help="What directory to put the staged files in."
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

    example = """The LOCAL-PATH specifies where to find the source data on this machine,
   and can take any form. The DEST-PATH specifies where the data should be store
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
        help="Require the new file to have *no* obsid associated (for maint. files)",
    )
    sp.add_argument(
        "--deletion",
        dest="deletion",
        default="disallowed",
        help=("Whether the created file instance will be deletable: " '"allowed" or "disallowed"'),
    )
    sp.add_argument(
        "--pre-staged",
        dest="pre_staged",
        metavar="STORENAME:SUBDIR",
        help="Specify that the data have already been staged at the destination.",
    )
    sp.add_argument("conn_name", metavar="CONNECTION-NAME", help=_conn_name_help)
    sp.add_argument(
        "local_path", metavar="LOCAL-PATH", help="The path to the data on this machine."
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
        "--client_id", dest="client_id", metavar="CLIENT-ID", help="The globus client ID."
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


def add_file_event(args):
    """
    Add a file event to a file in the librarian.
    """
    payload = {}
    for arg in args.key_vals:
        bits = arg.split("=", 1)
        if len(bits) != 2:
            die(f'argument {arg} must take the form "key=value"')

        # We parse each "value" as JSON ... and then re-encode it as JSON when
        # talking to the server. So this is mostly about sanity checking.
        key, text_val = bits
        try:
            value = json.loads(text_val)
        except ValueError:
            die(f"value {text_val} for keyword {key} does not parse as JSON")

        payload[key] = value

    path = os.path.basename(args.file_path)  # in case user provided a real filesystem path

    # Let's do it
    client = LibrarianClient(args.conn_name)

    try:
        client.create_file_event(path, args.event_type, **payload)
    except RPCError as e:
        die(f"event creation failed: {e}")

    return


def add_obs(args):
    """
    Register a list of files with the librarian.
    """
    # Load the info ...
    print("Gathering information ...")
    file_info = {}

    for path in args.paths:
        path = os.path.abspath(path)
        print("  ", path)
        file_info[path] = utils.gather_info_for_path(path)

    # ... and upload what we learned
    print("Registering with Librarian.")
    client = LibrarianClient(args.conn_name)
    try:
        client.register_instances(args.store_name, file_info, null_obsid=args.null_obsid)
    except RPCError as e:
        die(f"RPC failed: {e}")

    return


def launch_copy(args):
    """
    Launch a copy from one Librarian to another.
    """
    # Argument validation is pretty simple
    known_staging_store = None
    known_staging_subdir = None

    if args.pre_staged is not None:
        known_staging_store, known_staging_subdir = args.pre_staged.split(":", 1)

    # Let's do it
    file_name = os.path.basename(args.file_name)  # in case the user has spelled out a path
    client = LibrarianClient(args.source_conn_name)

    try:
        client.launch_file_copy(
            file_name,
            args.dest_conn_name,
            remote_store_path=args.dest,
            known_staging_store=known_staging_store,
            known_staging_subdir=known_staging_subdir,
        )
    except RPCError as e:
        die(f"launch failed: {e}")

    return


def assign_sessions(args):
    """
    Tell the Librarian to assign recent Observations to grouped "observing sessions".
    """
    # Let's do it
    client = LibrarianClient(args.conn_name)
    try:
        result = client.assign_observing_sessions(
            minimum_start_jd=args.minimum_start_jd, maximum_start_jd=args.maximum_start_jd
        )
    except RPCError as e:
        die(f"assignment failed: {e}")

    try:
        n = 0

        for info in result["new_sessions"]:
            if n == 0:
                print("New sessions created:")
            print(
                (
                    "  {id:d}: start JD {start_time_jd:f}, stop JD {stop_time_jd:f}, "
                    "n_obs {n_obs:d}"
                ).format(**info)
            )
            n += 1

        if n == 0:
            print("No new sessions created.")
    except Exception as e:
        die(f"sessions created, but failed to print info: {e}")

    return


def check_connections(args):
    """
    Check that this host can connect to other Librarians that have been configured.

    Also checks connections to their stores.
    """
    from . import all_connections

    any_failed = False

    for client in all_connections():
        print(
            f'Checking ability to establish HTTP connection to "{client.conn_name}" '
            f"({client.config['url']}) ..."
        )

        try:
            client.ping()
            print("   ... OK")
        except Exception as e:
            print(f"   ... error: {e}")
            any_failed = True
            continue

        print(f'   Querying "{client.conn_name}" for its stores ' "and how to connect to them ...")

        for store in client.stores():
            print(
                "   Checking ability to establish SSH connection to remote "
                f'store "{store.name}" ({store.ssh_host}:{store.path_prefix}) ...'
            )

            try:
                store.get_space_info()
                print("   ... OK")
            except Exception as e:
                print(f"   ... error: {e}")
                any_failed = True

    if any_failed:
        sys.exit(1)

    print()
    print("Everything worked!")


def delete_files(args):
    """
    Request to delete instances of files matching a given query.
    """

    def str_or_huh(x):
        return "???" if x is None else str(x)

    # Let's do it
    client = LibrarianClient(args.conn_name)

    if args.noop:
        print("No-op mode enabled: files will not actually be deleted.")
        print()
        itemtext = "todelete"
        summtext = "would have been deleted"
        mode = "noop"
    else:
        itemtext = "deleted"
        summtext = "were deleted"
        mode = "standard"

    try:
        result = client.delete_file_instances_matching_query(
            args.query, mode=mode, restrict_to_store=args.store
        )
        allstats = result["stats"]
    except RPCError as e:
        die(f"multi-delete failed: {e}")

    n_files = 0
    n_noinst = 0
    n_deleted = 0
    n_error = 0

    for fname, stats in sorted(iter(allstats.items()), key=lambda t: t[0]):
        nd = stats.get("n_deleted", 0)
        nk = stats.get("n_kept", 0)
        ne = stats.get("n_error", 0)

        if nd + nk + ne == 0:
            # This file had no instances. Don't bother printing it.
            n_noinst += 1
            continue

        n_files += 1
        n_deleted += nd
        n_error += ne
        deltext = str_or_huh(stats.get("n_deleted"))
        kepttext = str_or_huh(stats.get("n_kept"))
        errtext = str_or_huh(stats.get("n_error"))

        print(f"{fname}: {itemtext}={deltext} kept={kepttext} error={errtext}")

    if n_files:
        print("")
    print(
        "{:d} files were matched, {:d} had instances; {:d} instances {}".format(
            n_files + n_noinst, n_files, n_deleted, summtext
        )
    )

    if n_error:
        print(f"WARNING: {n_error:d} error(s) occurred; see server logs for information")
        sys.exit(1)

    return


def initiate_offload(args):
    """
    Initiate an "offload": move a bunch of file instances from one store to another.
    """
    # Let's do it
    client = LibrarianClient(args.conn_name)

    try:
        result = client.initiate_offload(args.source_name, args.dest_name)
    except RPCError as e:
        die(f"offload failed to launch: {e}")

    if "outcome" not in result:
        die(f'malformed server response (no "outcome" field): {repr(result)}')

    if result["outcome"] == "store-shut-down":
        print(
            "The store has no file instances needing offloading. It was placed offline "
            "and may now be closed out."
        )
    elif result["outcome"] == "task-launched":
        print(
            f"Task launched, intending to offload "
            f'{str(result.get("instance-count", "???"))} instances'
        )
        print()
        print("A noop-ified command to delete offloaded instances from the store is:")
        print(
            f"  librarian delete-files --noop --store '{args.source_name}' "
            f"'{args.conn_name}' '{{\"at-least-instances\": 2}}'"
        )
    else:
        die(f'malformed server response (unrecognized "outcome" field): {repr(result)}')

    return


def locate_file(args):
    """
    Ask the Librarian where to find a file.
    """
    # Let's do it
    # In case the user has provided directory components:
    file_name = os.path.basename(args.file_name)
    client = LibrarianClient(args.conn_name)

    try:
        result = client.locate_file_instance(file_name)
    except RPCError as e:
        die(f"couldn't locate file: {e}")

    print("{store_ssh_host}:{full_path_on_store}".format(**result))

    return


def offload_helper(args):
    """
    Launch this script to implement the "offload" functionality.
    """
    # Due to how the Librarian has to arrange things, it's possible that the
    # instance that we want to copy was deleted before this script got run. If so,
    # so be it -- don't signal an error.
    if not os.path.exists(args.local_path):
        print(f"source path {args.local_path} does not exist -- doing nothing")
        sys.exit(0)

    # The rare librarian script that does not use the LibrarianClient class!
    try:
        dest = base_store.BaseStore(args.name, args.pp, args.host)
        dest.copy_to_store(args.local_path, args.destrel)
    except Exception as e:
        die(e)

    return


def search_files(args):
    """
    Search for files in the librarian.
    """
    # Let's do it
    client = LibrarianClient(args.conn_name)

    try:
        result = client.search_files(args.search)
    except RPCError as e:
        die(f"search failed: {e}")

    nresults = len(result["results"])
    if nresults == 0:
        # we didn't get anything
        die("No files matched this search")

    print(f"Found {nresults:d} matching files")
    # first go through entries to format file size and remove potential null obsids
    for entry in result["results"]:
        entry["size"] = sizeof_fmt(entry["size"])
        if entry["obsid"] is None:
            entry["obsid"] = "None"

    # now print the results as a table
    print_table(
        result["results"],
        ["name", "create_time", "obsid", "type", "size"],
        ["Name", "Created", "Observation", "Type", "Size"],
    )

    return


def set_file_deletion_policy(args):
    """
    Set the "deletion policy" of one instance of this file.
    """
    # In case they gave a full path:
    file_name = os.path.basename(args.file_name)

    # Let's do it
    client = LibrarianClient(args.conn_name)
    try:
        client.set_one_file_deletion_policy(
            file_name, args.deletion_policy, restrict_to_store=args.store
        )
    except RPCError as e:
        die(f"couldn't alter policy: {e}")

    return


def stage_files(args):
    # sourcery skip: extract-method, use-fstring-for-formatting
    """
    Tell the Librarian to stage files onto the local scratch disk.
    """
    # Let's do it
    client = LibrarianClient(args.conn_name)

    # Get the username. We could make this a command-line option but I think it's
    # better to keep this a semi-secret. Note that the server does absolutely no
    # verification of the values that are passed in.

    import getpass

    user = getpass.getuser()

    # Resolve the destination in case the user provides, say, `.`, where the
    # server is not going to know what that means. This will need elaboration if
    # we add options for the server to come up with a destination automatically or
    # other things like that.
    our_dest = os.path.realpath(args.dest_dir)

    try:
        result = client.launch_local_disk_stage_operation(user, args.search, our_dest)
    except RPCError as e:
        die(f"couldn't start the stage operation: {e}")

    # This is a bit of future-proofing; we might teach the Librarian to choose a
    # "reasonable" output directory on your behalf.
    dest = result["destination"]

    print(
        "Launched operation to stage {:d} instances ({:d} bytes) to {}".format(
            result["n_instances"], result["n_bytes"], dest
        )
    )

    if not args.wait:
        print(f"Operation is complete when {dest}/STAGING-IN-PROGRESS is removed.")
    else:
        # The API call should not return until the progress-marker file is
        # created, so if we don't see that it exists, it should be the case that
        # the staging started and finished before we could check.
        if not os.path.isdir(dest):
            die(
                "cannot wait for staging to complete: destination directory {} not "
                "visible on this machine. Missing network filesystem?".format(dest)
            )

        marker_path = os.path.join(dest, "STAGING-IN-PROGRESS")
        t0 = time.time()
        print("Started waiting for staging to finish at:", time.asctime(time.localtime(t0)))

        while os.path.exists(marker_path):
            time.sleep(3)

        if os.path.exists(os.path.join(dest, "STAGING-SUCCEEDED")):
            print(f"Staging completed successfully ({time.time() - t0:0.1f}s elapsed).")
            sys.exit(0)

        try:
            with open(os.path.join(dest, "STAGING-ERRORS")) as f:
                print(
                    f"Staging completed WITH ERRORS ({time.time() - t0:0.1f}s elapsed)",
                    file=sys.stderr,
                )
                print("", file=sys.stderr)
                for line in f:
                    print(line.rstrip(), file=sys.stderr)
            sys.exit(1)
        except OSError as e:
            if e.errno == 2:
                die(
                    'staging finished but neiher "success" nor "error" indicator was '
                    f"created (no file {dest}/STAGING-ERRORS)"
                )
            raise

    return


def upload(args):
    """
    Upload a file to a Librarian.
    """
    # Argument validation is pretty simple
    if os.path.isabs(args.dest_store_path):
        die("destination path must be relative to store top; " f"got {args.dest_store_path}")

    if args.null_obsid and args.meta != "infer":
        die('illegal to specify --null-obsid when --meta is not "infer"')

    if args.meta == "json-stdin":
        try:
            rec_info = json.load(sys.stdin)
        except Exception as e:
            die(f"cannot parse stdin as JSON data: {e}")
        meta_mode = "direct"
    elif args.meta == "infer":
        rec_info = {}
        meta_mode = "infer"
    else:
        die(f"unexpected metadata-gathering method {args.meta}")

    known_staging_store = None
    known_staging_subdir = None

    if args.pre_staged is not None:
        known_staging_store, known_staging_subdir = args.pre_staged.split(":", 1)

    # Let's do it
    client = LibrarianClient(args.conn_name)

    try:
        client.upload_file(
            args.local_path,
            args.dest_store_path,
            meta_mode,
            rec_info,
            deletion_policy=args.deletion,
            known_staging_store=known_staging_store,
            known_staging_subdir=known_staging_subdir,
            null_obsid=args.null_obsid,
            use_globus=args.use_globus,
            client_id=args.client_id,
            transfer_token=args.transfer_token,
            source_endpoint_id=args.source_endpoint_id,
        )
    except RPCError as e:
        die(f"upload failed: {e}")

    return


def main():
    # make a parser and run the specified command
    parser = generate_parser()
    parsed_args = parser.parse_args()
    parsed_args.func(parsed_args)

    return


if __name__ == "__main__":
    sys.exit(main())
