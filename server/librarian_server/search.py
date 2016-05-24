# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"""Searches of the database

This code will likely need a lot of expansion, but we'll start simple.

"""

from __future__ import absolute_import, division, print_function, unicode_literals

__all__ = str('''

''').split ()

from flask import flash, redirect, render_template, url_for

from . import app, db
from .dbutil import NotNull
from .webutil import ServerError, json_api, login_required, optional_arg, required_arg


def select_files (search_string):
    import datetime
    from .file import File

    if search_string == 'special':
        two_weeks_ago = datetime.datetime.utcnow () - datetime.timedelta (days=14)
        return File.query.filter (File.create_time > two_weeks_ago,
                                  File.name.like ('%22130%'))

    raise NotImplementedError ('general searching not actually implemented')


class StandingOrder (db.Model):
    """A StandingOrder describes a rule for copying data from this Librarian to
    another. We save a search and a destination. When new files match that
    search, we automatically start copying them to the destination. We create
    a FileEvent with a name based on the name of the StandingOrder to mark
    when a file has successfully been copied.

    It is assumed that the relevant search has some time limit applied so that
    only files created in the last (e.g.) 7 days match.

    """
    __tablename__ = 'standing_order'

    id = db.Column (db.Integer, primary_key=True, autoincrement=True)
    name = NotNull (db.String (64))
    search = NotNull (db.Text)
    conn_name = NotNull (db.String (64))

    def __init__ (self, name, search, conn_name):
        self.name = name
        self.search = search
        self.conn_name = conn_name
        self._validate ()


    def _validate (self):
        """Check that this object's fields follow our invariants.

        """
        # TODO: validate the search string
        pass


    @property
    def event_type (self):
        return 'standing_order_succeeded:' + self.name


    def get_files_to_copy (self):
        """Generate a list of files that ought to be copied, according to the
        specifications of this StandingOrder.

        """
        from .file import File, FileEvent

        # The core query is something freeform specified by the user.

        query = select_files (self.search)

        # We then layer on a check that the files don't have the specified
        # marker event.

        already_done = (db.session.query (File.name)
                        .filter (FileEvent.name == File.name,
                                 FileEvent.type == self.event_type))
        query = query.filter (~File.name.in_ (already_done))

        # Finally we filter out files that already have copy tasks associated
        # with this standing order.

        from .store import UploaderTask
        from .bgtasks import the_task_manager
        from os.path import basename

        already_launched = set (basename (t.store_path)
                                for t in the_task_manager.tasks
                                if isinstance (t, UploaderTask) and self.name == t.standing_order_name)

        for file in query:
            if file.name not in already_launched:
                yield file


    def maybe_launch_copies (self):
        """Launch any file copy operations that need to happen according to this
        StandingOrder's specification.

        """
        import logging
        from .store import launch_copy_by_file_name

        for file in self.get_files_to_copy ():
            if launch_copy_by_file_name (file.name, self.conn_name,
                                         standing_order_name=self.name, no_instance='return'):
                logging.warn ('standing order %s should copy file %s to %s, but no instances '
                              'of it are available', self.name, file.name, self.conn_name)


# Web user interface

@app.route ('/SOTEST')
@login_required
def SOTEST ():
    so = StandingOrder ('test', 'special', 'offsite-karoo')
    todo = list (so.get_files_to_copy ())
    so.maybe_launch_copies ()

    return render_template (
        'file-listing.html',
        title='STANDING ORDER TEST',
        files=todo,
    )
