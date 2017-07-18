# -*- mode: python; coding: utf-8 -*-
# Copyright 2017 the HERA Collaboration
# Licensed under the BSD License.

"""Integration with the HERA on-site monitor-and-control (M&C) system.

This function is imported even if M&C reporting is disabled. If that's the
case, the various calls are all no-ops, as tested by seeing if
`the_mc_manager` is None. To keep things working, though, the top level of
this module can't import the `hera_mc` package.

"""
from __future__ import absolute_import, division, print_function

__all__ = '''
register_callbacks
'''.split()

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


    def check_in(self):
        from sqlalchemy import func, outerjoin, select
        from .file import File, FileInstance

        now = Time.now()

        num_files = db.session.query(func.count(File.name))

        data_volume_gb = (db.session.query(func.sum(File.size))
                          .select_from(FileInstance)
                          .outerjoin(File)).scalar() / 1024**3

        free_space_gb = 0. # TBD

        upload_min_elapsed = 0. # TBD

        num_processes = 0 # TBD

        try:
            self.mc_session.add_lib_status(now, num_files, data_volume_gb, free_space_gb,
                                           upload_min_elapsed, num_processes, self.version_string,
                                           self.git_hash)
        except Exception as e:
            logger.error('could not report status to the M&C system: %s', e)
            raise

        print('down here!')


the_mc_manager = None

def register_callbacks(version_string, git_hash):
    global the_mc_manager
    the_mc_manager = MCManager(version_string, git_hash)

    from tornado import ioloop
    cb = ioloop.PeriodicCallback(the_mc_manager.check_in, 10 * 1000)
    cb.start()
    return cb
