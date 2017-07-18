# -*- mode: python; coding: utf-8 -*-
# Copyright 2017 the HERA Collaboration
# Licensed under the BSD License.

"""Integration with the HERA on-site monitor-and-control (M&C) system.

This function is imported even if M&C reporting is disabled. If that's the
case, the various calls are all no-ops, as tested by seeing if
`the_mc_manager` is None. To keep things working, though, for installations
that don't have M&C available, the top level of this module isn't allowed to
import the `hera_mc` package.

"""
from __future__ import absolute_import, division, print_function

__all__ = '''
note_file_upload_finished
register_callbacks
'''.split()

import time
from astropy.time import Time

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

    def check_in(self):
        from sqlalchemy import func, outerjoin, select
        from .file import File, FileInstance
        from .store import Store

        now = Time.now()

        num_files = db.session.query(func.count(File.name)).scalar() or 0

        data_volume_gb = ((db.session.query(func.sum(File.size))
                           .select_from(FileInstance)
                           .outerjoin(File)).scalar() or 0) / 1024**3

        free_space_gb = 0
        for store in db.session.query(Store):
            free_space_gb += store.get_space_info()['available']  # measured in bytes
        free_space_gb /= 1024**3  # bytes => GiB

        upload_min_elapsed = (time.time() - self._last_file_upload_time) / 60

        from .bgtasks import get_unfinished_task_count
        num_processes = get_unfinished_task_count()

        try:
            self.mc_session.add_lib_status(now, num_files, data_volume_gb, free_space_gb,
                                           upload_min_elapsed, num_processes, self.version_string,
                                           self.git_hash)
            self.mc_session.commit()
        except Exception as e:
            logger.error('could not report status to the M&C system: %s', e)
            raise

    def note_file_upload_finished(self):
        self._last_file_upload_time = time.time()


the_mc_manager = None


def register_callbacks(version_string, git_hash):
    global the_mc_manager
    the_mc_manager = MCManager(version_string, git_hash)

    from tornado import ioloop
    cb = ioloop.PeriodicCallback(the_mc_manager.check_in, 10 * 1000)  # measured in milliseconds
    cb.start()
    return cb


# Hooks for other subsystems to send info to M&C without having to worry about
# whether M&C integration is actually activated.

def note_file_upload_finished():
    if the_mc_manager is None:
        return
    the_mc_manager.note_file_upload_finished()
