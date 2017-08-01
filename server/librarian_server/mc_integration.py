# -*- mode: python; coding: utf-8 -*-
# Copyright 2017 the HERA Collaboration
# Licensed under the BSD License.

"""Integration with the HERA on-site monitor-and-control (M&C) system.

This module is imported even if M&C reporting is disabled. If that's the case,
the various calls are all no-ops, as tested by seeing if `the_mc_manager` is
None. To keep things working, though, for installations that don't have M&C
available, the top level of this module isn't allowed to import the `hera_mc`
package.

"""
from __future__ import absolute_import, division, print_function

__all__ = '''
note_file_created
note_file_upload_succeeded
register_callbacks
'''.split()

import time

from astropy.time import Time
import six
from sqlalchemy.exc import InvalidRequestError

from . import app, db, logger


class MCManager(object):
    """A simple singleton class that checks in with M&C. Only gets created if M&C
    reporting is actually enabled.

    """

    def __init__(self, version_string, git_hash):
        self.version_string = version_string
        self.git_hash = git_hash

        import hera_mc.mc
        self.mc_db = hera_mc.mc.connect_to_mc_db(None)
        self.mc_session = self.mc_db.sessionmaker()

        # We need to report when the last file upload happened. But when we
        # first boot up, we don't know when that is. It would be silly to jump
        # through the hoops to maintain persistent state just to track this
        # one piece of monitoring info, so we default to saying that the last
        # upload just happened. If we used 0, we'd have to report that it's
        # been decades since the first upload, which would mess up the graphs.

        self._last_file_upload_time = time.time()

        # We also need some more detailed information for the "remote" status
        # reporting.

        self._remote_upload_stats = {}
        self._last_report_time = time.time()

    def check_in(self):
        from sqlalchemy import func, outerjoin, select
        from .file import File, FileInstance
        from .store import Store

        astro_now = Time.now()
        unix_now = time.time()

        # First, report our general status info.

        num_files = db.session.query(func.count(File.name)).scalar() or 0

        data_volume_gb = ((db.session.query(func.sum(File.size))
                           .select_from(FileInstance)
                           .outerjoin(File)).scalar() or 0) / 1024**3

        free_space_gb = 0
        for store in db.session.query(Store):
            free_space_gb += store.get_space_info()['available']  # measured in bytes
        free_space_gb /= 1024**3  # bytes => GiB

        upload_min_elapsed = (unix_now - self._last_file_upload_time) / 60

        from .bgtasks import get_unfinished_task_count
        num_processes = get_unfinished_task_count()

        try:
            self.mc_session.add_lib_status(astro_now, num_files, data_volume_gb, free_space_gb,
                                           upload_min_elapsed, num_processes, self.version_string,
                                           self.git_hash)
            self.mc_session.commit()
        except Exception as e:
            logger.error('could not report status to the M&C system: %s', e)
            raise

        # Now report information on our remotes. The Librarian *server*
        # doesn't actually directly know about the other connections defined
        # in ~/.hl_client.cfg, so this is a bit trickier than you might think.
        # We build up a list of remotes to check empirically, seeing what
        # connections have successful uploads. This means that when you
        # restart the Librarian server it will take a little while to
        # re-discover which remotes need monitoring. This will lead to gaps in
        # the M&C coverage, but the aggregate statistics and store-based
        # bandwidth reports still give insight into what's going on during
        # this warmup phase.
        #
        # The other tricky thing is that we, here inside the server, can't
        # monitor the progress of uploads, so we don't know how much bandwidth
        # they're using. All we know is the average value when the upload
        # finally finishes, based on the elapsed time and the file size. This
        # value may in principle be averaged over a span of time that is much
        # longer or shorter than the 15-minute cadence we're charged with
        # reporting. To keep things tractable, the numbers that we report are
        # the number of uploads that completed in the past reporting interval,
        # no matter when they started, and a kinda-fake bandwidth that is just
        # the sum of those files' sizes, divided by the reporting interval. In
        # corner cases the bandwidth will get wonky, but we also have the
        # direct measurements from the pots to look at.

        for conn_name, file_sizes in six.iteritems(self._remote_upload_stats):
            num_file_uploads = len(file_sizes)
            bytes_uploaded = sum(file_sizes)  # this works when the list is empty.
            bandwidth_Mbs = bytes_uploaded * 8 / (1024**2 * (unix_now - self._last_report_time))

            # Now we need to get the ping time. Here on the server we don't
            # have easy access to the hostnames of their remote Librarian's
            # stores, but we can make RPC calls to it. And, what do you know,
            # there's a ping RPC call! So we measure the roundtrip time for
            # that to execute.

            from hera_librarian import LibrarianClient, RPCError
            client = LibrarianClient(conn_name)
            t0 = time.time()

            try:
                client.ping()
            except RPCError as e:
                logger.warning('couldn\'t ping remote "%s": %s', conn_name, e)
                ping_time = 999
            else:
                ping_time = time.time() - t0

            # OK now we're ready to file our report!

            self.mc_session.add_lib_remote_status(astro_now, conn_name, ping_time,
                                                  num_file_uploads, bandwidth_Mbs)
            del file_sizes[:]

        self.mc_session.commit()
        self._last_report_time = time.time()

    def note_file_created(self, file_obj):
        try:
            self.mc_session.add_lib_file(file_obj.name, file_obj.obsid,
                                         file_obj.create_time_astropy,
                                         file_obj.size / 1024**3)
            self.mc_session.commit()
        except InvalidRequestError as e:
            # This can happen if the file's obsid is not registered in the M&C
            # database. TO BE VERIFIED: we have no control over this, right?
            raise

    def note_file_upload_succeeded(self, conn_name, file_size):
        self._last_file_upload_time = time.time()
        self._remote_upload_stats.setdefault(conn_name, []).append(file_size)


the_mc_manager = None


def register_callbacks(version_string, git_hash):
    global the_mc_manager
    the_mc_manager = MCManager(version_string, git_hash)

    from tornado import ioloop
    cb = ioloop.PeriodicCallback(the_mc_manager.check_in, 15 * 60 *
                                 1000)  # measured in milliseconds
    cb.start()
    return cb


# Hooks for other subsystems to send info to M&C without having to worry about
# whether M&C integration is actually activated.

def note_file_created(file_obj):
    if the_mc_manager is None:
        return
    the_mc_manager.note_file_created(file_obj)


def note_file_upload_succeeded(conn_name, file_size):
    if the_mc_manager is None:
        return
    the_mc_manager.note_file_upload_succeeded(conn_name, file_size)
