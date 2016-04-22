# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"""A data storage host that a Librarian knows about.

Librarian clients that want to upload data, etc., SSH into the stores
directly. This class gathers the information and functions used for doing
that.

"""

from __future__ import absolute_import, division, print_function, unicode_literals

__all__ = str('''
Store
''').split ()

import subprocess, os.path

from . import RPCError


class Store (object):
    """Note that the Librarian server code subclasses this class, so do not change
    its structure without making sure that you're not breaking it.

    """
    name = None
    path_prefix = None
    ssh_host = None

    def __init__ (self, name, path_prefix, ssh_host):
        self.name = name
        self.path_prefix = path_prefix
        self.ssh_host = ssh_host


    # Direct store access. All paths sent to SSH commands should be filtered
    # through self._path() to prepend the path_prefix and make sure that we're
    # not accidentally passing absolute paths around.

    def _path (self, *pieces):
        for p in pieces:
            if os.path.isabs (p):
                raise ValueError ('store paths must not be absolute; got %r' % (pieces,))
        return os.path.join (self.path_prefix, *pieces)


    def _ssh_slurp (self, command):
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
        proc = subprocess.Popen (argv, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate ()

        if proc.returncode != 0:
            raise RPCError (argv, 'exit code %d; stdout:\n\n%s\n\nstderr:\n\n%s'
                            % (proc.returncode, stdout, stderr))

        return stdout


    # Modifications of the store host. These should always be paired with
    # appropriate modifications of the Librarian server database, either
    # through an RPC call (if you're a client) or a direct change (if you're
    # the server).

    def _copy_to_store (self, local_path, store_path):
        """SCP a file to a particular path in the store.

        """
        # flags: recursive; batch (no-password-asking) mode; compression; preserve
        # times/modes; quiet mode.

        argv = ['scp', '-rBCpq', local_path, '%s:%s' % (self.ssh_host, self._path(store_path))]
        proc = subprocess.Popen (argv, shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output = proc.communicate ()[0]

        if proc.returncode != 0:
            raise RPCError (argv, 'exit code %d; output:\n\n%s' % (proc.returncode, output))


    def _move (self, source_store_path, dest_store_path):
        """Move a file in the store.

        We make sure that parent directories exist if needed.

        """
        dest_parent = os.path.dirname (dest_store_path)
        self._ssh_slurp ("mkdir -p '%s' && mv '%s' '%s'" %
                         (self._path(dest_parent), self._path(source_store_path),
                          self._path(dest_store_path)))


    def _delete (self, store_path):
        """Delete a file instance from the store.

        """
        self._ssh_slurp ("rm -rf '%s'" % self._path(store_path))


    def stage_file_on_store (self, local_path):
        """Stage a file instance on a store.

        This uploads the data to a special staging location. After the upload
        is complete, use a Librarian RPC call to complete the transaction.

        The motivation for this design is that uploads WILL crash without
        completing, and we want to be very sure that we do not accidentally
        propagate broken files. So we do the upload in a two-step fashion with
        sanity checks so that the Librarian can be sure that it's getting good
        data.

        """
        from . import utils
        size = utils.get_size_from_path (local_path)
        md5 = utils.get_md5_from_path (local_path)
        staging_path = 'upload_%s_%s.staging' % (size, md5)

        # If we're trying to copy a directory, a previous failure will result
        # in a directory lying around that our copy will then land *inside*
        # that directory, causing the copy to fail with a bad MD5 and size.
        # This is of course racy, but attempt to protect against that by
        # preemptively blowing away the destination.
        self._delete (staging_path)

        self._copy_to_store (local_path, staging_path)


    # Interrogations of the store -- these don't change anything so they don't
    # necessarily need to be paired with Librarian database modifications.

    def get_info_for_path (self, storepath):
        """`storepath` is a path relative to our `path_prefix`. We assume that we are
        not running on the store host, but can transparently SSH to it.

        """
        import json
        text = self._ssh_slurp ("python -c \'import hera_librarian.utils as u; u.print_info_for_path(\"%s\")\'"
                                % (self._path(storepath)))
        return json.loads(text)


    _cached_space_info = None
    _space_info_timestamp = None

    def get_space_info (self):
        """Get information about how much space is available in the store. We have a
        simpleminded cache since it's nice just to be able to call the
        function, but SSHing into the store every time is going to be a bit
        silly.

        """
        import time
        now = time.time ()

        # 30 second lifetime:
        if self._cached_space_info is not None and now - self._space_info_timestamp < 30:
            return self._cached_space_info

        output = self._ssh_slurp ('df -B1 %s' % self._path())
        bits = output.splitlines ()[-1].split ()
        info = {}
        info['used'] = int(bits[2]) # measured in bytes
        info['available'] = int(bits[3]) # measured in bytes
        info['total'] = info['used'] + info['available']

        self._cached_space_info = info
        self._space_info_timestamp = now

        return info


    @property
    def capacity (self):
        """Returns the total capacity of the store, in bytes.

        Accessing this property may trigger an SSH into the store host!

        """
        return self.get_space_info ()['total']


    @property
    def usage_percentage (self):
        """Returns the amount of the storage capacity that is currently used as a
        percentage.

        Accessing this property may trigger an SSH into the store host!

        """
        info = self.get_space_info ()
        return 100. * info['used'] / (info['total'])


    def upload_file_to_other_librarian (self, conn_name, local_store_path,
                                        remote_store_path=None, type=None,
                                        obsid=None, start_jd=None,
                                        create_time=None):
        """Fire off an SCP process on the store that will upload a given file to a
        different Librarian.

        TODO: we have no way of finding out if the copy succeeds or fails! The
        script should optionally report its outcome to the local Librarian.

        """
        if remote_store_path is None:
            remote_store_path = local_store_path

        command = 'nohup upload_to_librarian.py'

        if type is not None:
            command += ' --type %s' % type

        if obsid is not None:
            command += ' --obsid %s' % obsid

        if start_jd is not None:
            command += ' --start-jd %.20f' % start_jd

        if create_time is not None:
            command += ' --create-time %d' % create_time

        command += ' %s %s %s </dev/null >/tmp/COPYCOMMAND 2>&1 &' % (
            conn_name, self._path(local_store_path), remote_store_path)
        self._ssh_slurp (command)
