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
is_file_record_invalid
create_observation_record
note_file_created
note_file_upload_succeeded
register_callbacks
'''.split()

import time

from astropy.time import Time
import six
from sqlalchemy.exc import InvalidRequestError

from . import app, db, is_primary_server, logger
from .dbutil import SQLAlchemyError
from .webutil import ServerError


# M&C severity classes
FATAL, SEVERE, WARNING, INFO = range(1, 5)


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

    def error(self, severity, fmt, *args):
        if len(args):
            text = fmt % args
        else:
            text = str(fmt)

        logger.error('M&C-related error (severity %d): %s', severity, text)

        try:
            self.mc_session.add_subsystem_error(Time.now(), 'lib', severity, text)
        except Exception as e:
            logger.error('could not log error to M&C: %s', e)

        try:
            self.mc_session.commit()
        except SQLAlchemyError as e:
            self.mc_session.rollback()
            logger.error('could not commit error record to M&C: %s (rolled back)', e)
        except Exception as e:
            logger.error('could not commit error record to M&C: %s', e)

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
        for store in Store.query.filter(Store.available):
            free_space_gb += store.get_space_info()['available']  # measured in bytes
        free_space_gb /= 1024**3  # bytes => GiB

        upload_min_elapsed = (unix_now - self._last_file_upload_time) / 60

        from .bgtasks import get_unfinished_task_count
        num_processes = get_unfinished_task_count()

        try:
            self.mc_session.add_lib_status(astro_now, num_files, data_volume_gb, free_space_gb,
                                           upload_min_elapsed, num_processes, self.version_string,
                                           self.git_hash)
        except Exception as e:
            # If that failed it seems unlikely that we'll be able to continue,
            # but let's try.
            self.error(SEVERE, 'could not report status to the M&C system: %s', e)

        try:
            self.mc_session.commit()
        except SQLAlchemyError as e:
            self.mc_session.rollback()
            self.error(SEVERE, 'could not commit status to the M&C system: %s (rolled back)', e)
        except Exception as e:
            self.error(SEVERE, 'could not commit status to the M&C system: %s', e)

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

        try:
            self.mc_session.commit()
        except SQLAlchemyError as e:
            self.mc_session.rollback()
            self.error(SEVERE, 'could not commit ping report to the M&C system: %s', e)

        self._last_report_time = time.time()

    def is_file_record_invalid(self, file_obj):
        """This function is kind-sorta superseded by create_observation_record(), but
        can still have a role to play if file records are uploaded to an
        M&C-enabled librarian -- see the `misc` module.

        """
        if file_obj.obsid is None:
            return False  # this is OK, for maintenance files

        for mc_obs in self.mc_session.get_obs(obsid=file_obj.obsid):
            return False  # if this executes, we got one and the file's OK

        # If we got here, there was no session and something bad is up!
        self.error(SEVERE,
                   'rejecting file %s (obsid %d): its obsid is not in M&C\'s hera_obs table',
                   file_obj.name, file_obj.obsid)
        return True

    def create_observation_record(self, obsid):
        mc_obses = list(self.mc_session.get_obs(obsid=obsid))

        if len(mc_obses) != 1:
            self.error(SEVERE,
                       'expected one M&C record for obsid %d; got %d of them',
                       obsid, len(mc_obses))
            return None

        mc_obs = mc_obses[0]
        from .observation import Observation
        from astropy.time import Time
        start_jd = mc_obs.jd_start
        stop_jd = Time(mc_obs.stoptime, format='gps').jd
        start_lst = mc_obs.lst_start_hr
        return Observation(obsid, start_jd, stop_jd, start_lst)

    def note_file_created(self, file_obj):
        """Tell M&C about a new Librarian file. M&C file records can also have null
        obsids, so we don't need to do anything special for maintenance files.

        """
        try:
            self.mc_session.add_lib_file(file_obj.name, file_obj.obsid,
                                         file_obj.create_time_astropy,
                                         file_obj.size / 1024**3)
        except InvalidRequestError as e:
            # This could happen if the file's obsid were not registered in the
            # M&C database. Which shouldn't happen, but ...
            self.error(SEVERE, 'couldn\'t register file %s (obsid %s) with M&C: %s',
                       file_obj.name, file_obj.obsid, e)

        try:
            self.mc_session.commit()
        except SQLAlchemyError as e:
            self.mc_session.rollback()
            self.error(SEVERE, 'could not commit file creation note to the M&C system: %s', e)

    def note_file_upload_succeeded(self, conn_name, file_size):
        self._last_file_upload_time = time.time()
        self._remote_upload_stats.setdefault(conn_name, []).append(file_size)


the_mc_manager = None


def register_callbacks(version_string, git_hash):
    global the_mc_manager
    the_mc_manager = MCManager(version_string, git_hash)

    if not is_primary_server():
        # Only one server process needs to check in. THIS MEANS THAT
        # BACKGROUND TASK REPORTING WILL BE INACCURATE!!!!
        return

    from tornado import ioloop
    cb = ioloop.PeriodicCallback(the_mc_manager.check_in, 15 * 60 *
                                 1000)  # measured in milliseconds
    cb.start()
    return cb


# Hooks for other subsystems to send info to M&C without having to worry about
# whether M&C integration is actually activated.

def is_file_record_invalid(file_obj):
    """If we're M&C-enabled, we refuse to create files with obsids that are not
    contained in the hera_obs table.

    """
    if the_mc_manager is None:
        return False

    return the_mc_manager.is_file_record_invalid(file_obj)


def create_observation_record(obsid):
    """If we're M&C-enabled, we copy observation records out of the M&C database.
    Otherwise, signal an error. The only ways to create Observations are to
    get them from M&C or for another Librarian to tell us about them, as done
    in the `initiate_upload` API call.

    """
    if not isinstance(obsid, (int, long)):
        raise ValueError('obsid must be integer; got %r' % (obsid, ))  # in case a None slips in

    if the_mc_manager is None:
        raise ServerError('cannot create fresh observations without M&C')

    rec = the_mc_manager.create_observation_record(obsid)
    if rec is None:
        raise ServerError('expected M&C to know about obsid %s but it didn\'t', obsid)

    db.session.add(rec)
    return rec


def note_file_created(file_obj):
    if the_mc_manager is None:
        return
    the_mc_manager.note_file_created(file_obj)


def note_file_upload_succeeded(conn_name, file_size):
    if the_mc_manager is None:
        return
    the_mc_manager.note_file_upload_succeeded(conn_name, file_size)
