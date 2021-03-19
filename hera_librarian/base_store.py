# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"""A data storage host that a Librarian knows about.

Librarian clients that want to upload data, etc., SSH into the stores
directly. This class gathers the information and functions used for doing
that.

"""



__all__ = str('''
BaseStore
''').split()

import subprocess
import os.path
import time
import warnings

from . import RPCError

NUM_RSYNC_TRIES = 6


class BaseStore(object):
    """Note that the Librarian server code subclasses this class, so do not change
    its structure without making sure that you're not breaking it.

    """
    name = None
    path_prefix = None
    ssh_host = None

    def __init__(self, name, path_prefix, ssh_host):
        self.name = name
        self.path_prefix = path_prefix
        self.ssh_host = ssh_host

    # Direct store access. All paths sent to SSH commands should be filtered
    # through self._path() to prepend the path_prefix and make sure that we're
    # not accidentally passing absolute paths around.

    def _path(self, *pieces):
        for p in pieces:
            if os.path.isabs(p):
                raise ValueError('store paths must not be absolute; got %r' % (pieces,))
        return os.path.join(self.path_prefix, *pieces)

    def _ssh_slurp(self, command, input=None):
        """SSH to the store host, run a command, and return its standard output. Raise
        an RPCError with standard error output if anything goes wrong.

        You MUST be careful about quoting! `command` is passed as an argument
        to 'bash -c', so it goes through one layer of parsing by the shell on
        the remote host. For instance, filenames containing '>' or ';' or '('
        or ' ' will cause problems unless you quote them appropriately. We do
        *not* launch our SSH process through a shell, so only one layer of
        shell quoting is required -- you'd need two if you were just typing
        the command in a terminal manually. BUT THEN, you're probably writing
        your command string as a Python string, so you probably need another
        layer of Python string literal quoting on top of that!

        """
        argv = ['ssh', self.ssh_host, command]

        if input is None:
            import os
            stdin = open(os.devnull, 'rb')
        else:
            stdin = subprocess.PIPE

        proc = subprocess.Popen(argv, shell=False, stdin=stdin,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if input is None:
            stdin.close()
        stdout, stderr = proc.communicate(input=input)

        if proc.returncode != 0:
            raise RPCError(argv, 'exit code %d; stdout:\n\n%r\n\nstderr:\n\n%r'
                           % (proc.returncode, stdout.decode("utf-8"), stderr.decode("utf-8")))

        return stdout

    def _stream_path(self, store_path):
        """Return a subprocess.Popen instance that streams file contents on its
        standard output. If the file is a flat file, this is well-defined; if
        the file is a directory, the "contents" are its tar-ification, inside
        one level of subdirectory named as the directory is. For instance, if
        the target is a directory "/data/foo/bar", containing files "a" and
        "b", the returned tar file will contain "bar/a" and "bar/b".

        """
        import os
        argv = ['ssh', self.ssh_host, "librarian_stream_file_or_directory.sh '%s'" %
                self._path(store_path)]
        stdin = open(os.devnull, 'rb')
        proc = subprocess.Popen(argv, shell=False, stdin=stdin,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdin.close()
        return proc

    def _rsync_transfer(self, local_path, store_path):
        """Copy a file to a particular path using rsync.

        Parameters
        ----------
        local_path : str
            Path to local file to be copied.
        store_path : str
            Path to store file at on destination host.

        Returns
        -------
        None

        Raises
        ------
        RPCError
            Raised if rsync transfer does not complete successfully.
        """
        # Rsync will nest directories in a way that we don't want if we don't
        # end their names with "/", but it will error if we end a file name
        # with "/". So we have to check:

        if os.path.isdir(local_path) and not local_path.endswith('/'):
            local_suffix = '/'
        else:
            local_suffix = ''

        # flags: archive mode; keep partial transfers. Have SSH work in batch
        # mode and turn off known hosts and host key checking to Just Work
        # without needing prompts. We used to have SSH use compression, but this
        # put too high of a CPU load on the paper1 correlator machine. You could
        # imagine making that an option if it helped with data transfer from
        # Karoo to US.

        argv = [
            'rsync',
            '-aP',
            '-e',
            'ssh -c aes256-gcm@openssh.com -o BatchMode=yes -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no',
            local_path + local_suffix,
            '%s:%s' % (self.ssh_host, self._path(store_path))
        ]
        success = False

        for i in range(NUM_RSYNC_TRIES):
            proc = subprocess.Popen(argv, shell=False, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT)
            output = proc.communicate()[0]

            if proc.returncode == 0:
                success = True
                break

        if not success:
            raise RPCError(argv, 'exit code %d; output:\n\n%r' % (proc.returncode, output))

    def _globus_transfer(
        self,
        local_path,
        store_path,
        client_id,
        transfer_token,
        source_endpoint_id,
        destination_endpoint_id,
        host_path,
    ):
        """Copy a file to a particular path using globus.

        For further information on using the Python API for Globus, refer to the
        Globus SDK docs: https://globus-sdk-python.readthedocs.io/en/stable/

        Parameters
        ----------
        local_path : str
            Path to local file to be copied.
        store_path : str
            Path to store file at on destination host. Note that if the target
            endpoint is a "shared endpoint", this may be relative to the globus
            root directory rather than the filesystem root.
        client_id : str
            The globus client ID to use for the transfer.
        transfer_token : str
            The globus transfer token to use for the transfer.
        source_endpoint_id : str
            The globus endpoint ID of the source store.
        destination_endpoint_id : str
            The globus endpoint ID of the destination store.
        host_path : str, optional
            The `host_path` of the globus store. When using shared endpoints,
            this is the root directory presented to the client.

        Returns
        -------
        None

        Raises
        ------
        RPCError
            Raised if submission of transfer to globus does not complete
            successfully or if globus_sdk package is not installed.
        """
        try:
            import globus_sdk
            from globus_sdk.exc import AuthAPIError
        except ModuleNotFoundError:
            raise RPCError(
                "globus_sdk import",
                "The globus_sdk package must be installed for globus "
                "functionality. Please `pip install globus_sdk` and try again."
            )

        # check that both endpoint IDs have been specified
        if source_endpoint_id is None or destination_endpoint_id is None:
            raise RPCError(
                "globus endpoint check",
                "Both source_endpoint_id and destination_endpoint_id must be "
                "specified to initiate globus transfer."
            )

        # make globus transfer client
        client = globus_sdk.NativeAppAuthClient(client_id)
        try:
            authorizer = globus_sdk.RefreshTokenAuthorizer(transfer_token, client)
        except AuthAPIError:
            raise RPCError(
                "globus authorization",
                "globus authentication failed. Please check client_id and "
                "authentication token and try again."
            )
        tc = globus_sdk.TransferClient(authorizer=authorizer)

        # make a new data transfer object
        basename = os.path.basename(os.path.normpath(local_path))
        tdata = globus_sdk.TransferData(
            tc,
            source_endpoint_id,
            destination_endpoint_id,
            sync_level="checksum",
            label=basename,
            notify_on_succeeded=False,
            notify_on_failed=True,
            notify_on_inactive=True,
        )

        # format the path correctly
        store_path = self._path(store_path)
        if host_path is not None:
            if store_path.startswith(host_path):
                store_path = store_path[len(host_path):]
                # trim off leading "/" if present
                if store_path.startswith("/"):
                    store_path = store_path.lstrip("/")

        # add data to be transferred
        if os.path.isdir(local_path):
            recursive = True
        else:
            recursive = False
        tdata.add_item(local_path, store_path, recursive=recursive)

        # initiate transfer
        transfer_result = tc.submit_transfer(tdata)

        # get task_id and query status until it finishes
        task_id = transfer_result["task_id"]
        while True:
            task = tc.get_task(task_id)
            if task["status"] == "SUCCEEDED":
                return
            elif task["status"] == "FAILED":
                # get the events associated with this transfer to help with debugging
                events = tc.task_event_list(task_id)
                error_string = ""
                for event in events:
                    error_string.append(event["time"] + ": " + event["description"] + "\n")
                raise RPCError("globus transfer", 'events:\n\n%r' % (error_string))
            else:  # task is "ACTIVE"
                time.sleep(5)

    # Modifications of the store host. These should always be paired with
    # appropriate modifications of the Librarian server database, either
    # through an RPC call (if you're a client) or a direct change (if you're
    # the server).

    def copy_to_store(
        self,
        local_path,
        store_path,
        try_globus=False,
        client_id=None,
        transfer_token=None,
        source_endpoint_id=None,
        destination_endpoint_id=None,
        host_path=None,
    ):
        """Transfer a file to a particular path in the store.

        You should not copy files directly to their intended destinations. You
        should use the Librarian `prepare_upload` RPC call to get a staging
        directory and copy your files there; then use the `complete_upload`
        RPC call to tell the Librarian that you're done.

        Parameters
        ----------
        local_path : str
            Path to the local file to upload.
        store_path : str
            Path to store file at on destination host.
        try_globus : bool, optional
            Whether to try to use globus to transfer. If False, or if globus
            fails, automatically fall back on rsync.
        client_id : str, optional
            The globus client ID to use for the transfer.
        transfer_token : str, optional
            The globus transfer token to use for the transfer.
        source_endpoint_id : str, optional
            The globus endpoint ID of the source store.
        destination_endpoint_id : str, optional
            The globus endpoint ID of the destination store.
        host_path : str, optional
            The `host_path` of the globus store. When using shared endpoints,
            this is the root directory presented to the client. Note that this
            may be different from the `path_prefix` for a given store.

        Returns
        -------
        None
        """
        if try_globus:
            try:
                self._globus_transfer(
                    local_path,
                    store_path,
                    client_id,
                    transfer_token,
                    source_endpoint_id,
                    destination_endpoint_id,
                    host_path,
                )
            except RPCError as e:
                # something went wrong with globus--fall back on rsync
                print("Globus transfer failed: {}\nFalling back on rsync...".format(e))
                self._rsync_transfer(local_path, store_path)
        else:
            # use rsync from the get-go
            self._rsync_transfer(local_path, store_path)

    def _chmod(self, store_path, modespec):
        """Change Unix permissions on a path in the store.

        `modespec` is a textual specification that is passed to the `chmod`
        command. This is useful since Librarian "files" can be either Unix
        files or directories, so for many use cases we do not necessarily want
        to be operating in terms of numerical mode specifications. For the
        same reason, we always provide the `-R` option to `chmod`.

        Returns the standard output of `chmod`, which should be empty on
        success. RPCError will be raised if the invoked command exits with a
        failure code.

        """
        return self._ssh_slurp("chmod -R '%s' '%s'" % (modespec, self._path(store_path)))

    def _move(self, source_store_path, dest_store_path, chmod_spec=None):
        """Move a path in the store.

        We make sure that parent directories exist if needed. We refuse to
        overwrite existing files. If `dest_store_path` is an existing
        directory, we refuse to place the source file inside of it.

        I can't actually find a way to get `mv` to indicate an error in the
        case that it refuses to overwrite the destination, so we test that the
        mv succeeded by seeing if the source file disappeared. This approach
        has the important attribute of not being racy.

        If the source file is already in the Librarian or is an upload from a
        different Librarian, it may be read-only. You can't move a read-only
        directory -- because its '..' entry needs to be altered, I think.
        (This is not true for flat files -- the read/write permission that
        matters is usually just that of the containing directory, not the item
        itself.) So, we make the source item writable before moving in case it
        it's a read-only directory.

        If a file was uploaded to this Librarian in read-write mode but we
        want our files to be read-only, the uploaded data need to be made
        read-only -- but, as per the previous paragraph, this must happen
        after moving the data to their final destination. The `chmod_spec`
        argument enables a recursive post-mv `chmod` to support this
        functionality; the `chmod` could be done through a separate call, but
        this way it can all be taken care of in one SSH invocation with a
        minimal window of writeability. In principle it would be nice to have
        *zero* window of writeability but so long as we support "files" that
        are really directories, I believe that is simply not possible.

        """
        dest_parent = os.path.dirname(dest_store_path)
        ssp = self._path(source_store_path)
        dsp = self._path(dest_store_path)

        if chmod_spec is not None:
            piece = " && chmod -R '%s' '%s'" % (chmod_spec, dsp)
        else:
            piece = ''

        return self._ssh_slurp(
            "mkdir -p '%s' && chmod u+w '%s' && mv -nT '%s' '%s' && test ! -e '%s'%s" %
            (self._path(dest_parent), ssp, ssp, dsp, ssp, piece)
        )

    def _delete(self, store_path, chmod_before=False):
        """Delete a path from the store.

        We use the `-r` flag of `rm` to delete recursively, but not the `-f`
        flag, so an error will be raised if the intended path does not exist.
        Note that the standard input of `rm` will not be a terminal, so it
        should never attempt to prompt if the file is read-only.

        The Librarian can be configured to make items read-only on ingest. If
        that happens, in order to delete a directory we need to chmod it
        first. Hence the `chmod_before` flag.

        """
        if chmod_before:
            part1 = "chmod -R u+w '%s' && " % self._path(store_path)
        else:
            part1 = ''
        return self._ssh_slurp(part1 + "rm -r '%s'" % self._path(store_path))

    def _create_tempdir(self, key='libtmp'):
        """Create a temporary directory in the store's root and return its "store
        path".

        """
        # we need to convert the output of _ssh_slurp -- a path in bytes -- to a string
        output = self._ssh_slurp('mktemp -d -p %s %s.XXXXXX' % (self.path_prefix, key)).decode("utf-8")
        fullpath = output.splitlines()[-1].strip()

        if not fullpath.startswith(self.path_prefix):
            raise RPCError('unexpected output from mktemp on %s: %s'
                           % (self.name, fullpath))

        return fullpath[len(self.path_prefix) + 1:]

    # Interrogations of the store -- these don't change anything so they don't
    # necessarily need to be paired with Librarian database modifications.

    def get_info_for_path(self, storepath):
        """`storepath` is a path relative to our `path_prefix`. We assume that we are
        not running on the store host, but can transparently SSH to it.

        """
        import json
        text = self._ssh_slurp(
            "python -c \'import hera_librarian.utils as u; u.print_info_for_path(\"%s\")\'"
            % (self._path(storepath))
        )
        return json.loads(text)

    _cached_space_info = None
    _space_info_timestamp = None

    def get_space_info(self):
        """Get information about how much space is available in the store. We have a
        simpleminded cache since it's nice just to be able to call the
        function, but SSHing into the store every time is going to be a bit
        silly.

        """
        import time
        now = time.time()

        # 30 second lifetime:
        if self._cached_space_info is not None and now - self._space_info_timestamp < 30:
            return self._cached_space_info

        output = self._ssh_slurp('df -B1 %s' % self._path())
        bits = output.splitlines()[-1].split()
        info = {}
        info['used'] = int(bits[2])  # measured in bytes
        info['available'] = int(bits[3])  # measured in bytes
        info['total'] = info['used'] + info['available']

        self._cached_space_info = info
        self._space_info_timestamp = now

        return info

    @property
    def capacity(self):
        """Returns the total capacity of the store, in bytes.

        Accessing this property may trigger an SSH into the store host!

        """
        return self.get_space_info()['total']

    @property
    def space_left(self):
        """Returns the amount of space left in the store, in bytes.

        Accessing this property may trigger an SSH into the store host!

        Note: we can't call this "available" since that conflicts with the
        boolean availability flag in the server.

        """
        return self.get_space_info()['available']

    @property
    def usage_percentage(self):
        """Returns the amount of the storage capacity that is currently used as a
        percentage.

        Accessing this property may trigger an SSH into the store host!

        """
        info = self.get_space_info()
        return 100. * info['used'] / (info['total'])

    def upload_file_to_other_librarian(
        self,
        conn_name,
        rec_info,
        local_store_path,
        remote_store_path=None,
        known_staging_store=None,
        known_staging_subdir=None,
        use_globus=False,
        client_id=None,
        transfer_token=None,
        source_endpoint_id=None,
    ):
        """Upload a given file to a different Librarian.

        This function will SSH into the store host, from which it will launch an
        rsync or globus transfer, and it will not return until everything is
        done! This means that in the real world it may not return for hours, and
        it will not infrequently raise an exception.

        TODO: there is no internal progress tracking; we just block and
        eventually return some textual output from the rsync-within-SSH. This is
        far from ideal. Globus offers some information about transfer progress,
        but we do not (yet) have automated ways of retrieving it.

        Parameters
        ----------
        conn_name : str
            The name of the external librarian to upload a file to.
        rec_info : dict
            A dictionary containing information about the file records being
            transferred. These are needed by the receiving librarian.
        local_store_path : str
            The full path to the file in the local store.
        remote_store_path : str, optional
            The full path to the file destination in the remote store. If not
            specified, it will default to be the same as local_store_path.
        known_staging_store : str, optional
            The store corresponding to the already-uploaded file. Must be specified
            if `known_staging_subdir` is specified.
        known_staging_subdir : str, optional
            The target directory corresponding to the already-uploaded file. Must by
            specified if `known_staging_store` is specified.
        use_globus : bool, optional
            Whether to try to use globus to transfer. If False, or if globus
            fails, automatically fall back on rsync.
        client_id : str, optional
            The globus client ID to use for the transfer.
        transfer_token : str, optional
            The globus transfer token to use for the transfer.
        source_endpoint_id : str, optional
            The globus endpoint ID of the source store. May be omitted, in which
            case we assume it is a "personal" (as opposed to public) client.

        Returns
        -------
        bytes
            The output of the `_ssh_slurp` command to log into the store and
            launch the librarian upload.
        """
        if remote_store_path is None:
            remote_store_path = local_store_path

        if (known_staging_store is None) ^ (known_staging_subdir is None):
            raise ValueError('both known_staging_store and known_staging_subdir must be specified')

        if known_staging_store is None:
            pre_staged_arg = ''
        else:
            pre_staged_arg = ' --pre-staged=%s:%s' % (known_staging_store, known_staging_subdir)

        import json
        rec_text = json.dumps(rec_info)

        command = 'librarian upload --meta=json-stdin%s %s %s %s' % (
            pre_staged_arg, conn_name, self._path(local_store_path), remote_store_path)
        # optional globus additions to the command
        if use_globus:
            command += f" --use_globus --client_id={client_id} --transfer_token={transfer_token}"
            if source_endpoint_id is not None:
                command += f" --source_endpoint_id={source_endpoint_id}"

        # actually run the command
        return self._ssh_slurp(command, input=rec_text.encode("utf-8"))

    def upload_file_to_local_store(self, local_store_path, dest_store, dest_rel):
        """Fire off an rsync process on the store that will upload a given file to
        another store *on the same Librarian*. Like
        `upload_file_to_other_librarian`, this function will SSH into the
        store host launch an rsync, and not return until everything is done.

        `destrel` is the "store path" of where the file should be placed on
        the destination Librarian. This should be a staging directory.

        This function is needed to implement the Librarian's "offload"
        feature.

        """
        c = ("librarian offload-helper --name '%s' --pp '%s' --host '%s' "
             "--destrel '%s' '%s'" % (dest_store.name, dest_store.path_prefix,
                                      dest_store.ssh_host, dest_rel, self._path(local_store_path)))
        return self._ssh_slurp(c)

    def check_stores_connections(self):
        """Tell the store to check its ability to connect to other Librarians and
        *their* stores.

        This just runs the command "librarian check-connections" and returns
        its textual output. In principle this command could be given an option
        to return its output in JSON and we could parse it, but for the
        envisioned use cases text will be OK.

        """
        return self._ssh_slurp('librarian check-connections').decode('utf-8')
