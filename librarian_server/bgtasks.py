# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"""Manage background tasks.

These are of course chained to the lifetime of the server process, so we lose
track of them all if the server dies, which of course could happen without
notice at any time.

This system requires Tornado.

"""
from __future__ import absolute_import, division, print_function, unicode_literals

__all__ = str('''
BackgroundTask
submit_background_task
register_background_task_reporter
get_unfinished_task_count
''').split()

import time

from tornado.ioloop import IOLoop


from flask import flash, redirect, render_template, url_for

from . import app, db, logger
from .dbutil import NotNull
from .webutil import ServerError, json_api, login_required, optional_arg, required_arg


class BackgroundTask (object):
    """A class implementing a background task.

    Instances of this task are also used by the Librarian to keep track of its
    background tasks. Override the `thread_function` and `wrapup_function`
    methods to implement your actual task: the `thread_function` is called in
    a background thread and takes a long time, but cannot access the database
    or web services. The `wrapup_function` is fed the results of
    `thread_function` and can access the database, but cannot take very long
    to run.

    You need to submit the task to TaskManager for it to actually run!

    """
    _manager = None
    desc = 'unset description'
    submit_time = None
    start_time = None
    finish_time = None
    exception = None

    def __str__(self):
        return self.desc

    def thread_function(self):
        raise NotImplementedError()

    def wrapup_function(self, thread_retval, thread_exc):
        """This function is passed two arguments: the return value from the
        `thread_function` function, and an exception that it may have raised.
        If the exception is not None, the thread_function failed and
        appropriate action should be taken.

        """
        raise NotImplementedError()

    @property
    def runtime(self):
        if self.start_time is None:
            return float('NaN')
        if self.finish_time is None:
            return time.time() - self.start_time
        return self.finish_time - self.start_time

    @property
    def wait_time(self):
        if self.start_time is None:
            return time.time() - self.submit_time
        return self.start_time - self.submit_time

    @property
    def time_since_completed(self):
        if self.finish_time is None:
            return float('NaN')
        return time.time() - self.finish_time

    @property
    def outcome_str(self):
        if self.exception is None:
            return 'success'
        return str(self.exception)


MAX_PURGE_FREQUENCY = 60  # seconds
MIN_TASK_LIST_LENGTH = 20  # don't purge tasks if more than these are left
TASK_LINGER_TIME = 600  # seconds


def _thread_wrapper(task):
    task.start_time = time.time()

    try:
        retval = task.thread_function()
        exc = None
    except Exception as e:
        retval = None
        exc = e

    task.finish_time = time.time()
    task.exception = exc
    IOLoop.instance().add_callback(_wrapup_wrapper, task, retval, exc)


def _wrapup_wrapper(task, thread_retval, thread_exc):
    try:
        task.wrapup_function(thread_retval, thread_exc)
    except Exception as e:
        logger.warn('exception in %s wrapup function: %s', task, e)
        task.exception = thread_exc = e

    # We let the task linger in task list for a little while so that it's
    # possible to review historical activity. But while we're here, see if
    # there's anything to purge.
    the_task_manager._maybe_purge_tasks()


class TaskManager (object):
    tasks = None
    """This is a list of all tasks that are pending, in processing, or have exited
    recently. Eventually we purge them but it's useful to be able to see the
    server's recent activity.

    """
    worker_pool = None
    """A ThreadPool of workers that can execute the background tasks.

    """
    last_purge = 0
    """The last time that the `tasks` list was purged."""

    def __init__(self):
        self.tasks = []
        self.last_purge = time.time()

    def _maybe_purge_tasks(self):
        now = time.time()

        if now - self.last_purge < MAX_PURGE_FREQUENCY:
            # Don't purge more frequently than every minute.
            return

        self.last_purge = now

        if len(self.tasks) <= MIN_TASK_LIST_LENGTH:
            # Don't bother purging if there aren't more than this many tasks
            # listed.
            return

        self.tasks = [t for t in self.tasks
                      if (t.finish_time is None or
                          (now - t.finish_time) < TASK_LINGER_TIME)]

    def submit(self, task):
        """Submit a task to be run in the background.

        It may not be launched immediately if there are a lot of background
        tasks to deal with.

        apply_async() returns a result object, but we're a web service so we
        can't wait around to see what it is. Instead we run the function in a
        wrapper that uses Tornado's infrastructure to let the main thread know
        what happened to it.

        """
        assert task._manager is None, 'may not submit task to multiple managers'

        self._maybe_purge_tasks()

        task._manager = self

        if self.worker_pool is None:
            import multiprocessing.util
            multiprocessing.util.log_to_stderr(5)
            from multiprocessing.pool import ThreadPool
            self.worker_pool = ThreadPool(app.config.get('n_worker_threads', 8))

        task.submit_time = time.time()
        self.tasks.append(task)
        self.worker_pool.apply_async(_thread_wrapper, (task,))

    def maybe_wait_for_threads_to_finish(self):
        if self.worker_pool is None:
            return

        print('Waiting for background jobs to complete ...')
        self.worker_pool.close()
        self.worker_pool.join()
        print('   ... done.')


the_task_manager = TaskManager()


def submit_background_task(task):
    return the_task_manager.submit(task)


def maybe_wait_for_threads_to_finish():
    the_task_manager.maybe_wait_for_threads_to_finish()


def log_background_task_status():
    active = [t for t in the_task_manager.tasks
              if t.start_time is not None and t.finish_time is None]
    pending = [t for t in the_task_manager.tasks
               if t.start_time is None]
    finished = [t for t in the_task_manager.tasks
                if t.finish_time is not None]

    logger.info('%d background tasks: %d active, %d pending, %d finished',
                len(the_task_manager.tasks), len(active),
                len(pending), len(finished))


def register_background_task_reporter():
    """Create a Tornado PeriodicCallback that will periodically report on the
    status of background tasks.

    The timeout for the callback is measured in milliseconds, so we queue an
    evaluation every 3 minutes.

    """
    from tornado import ioloop

    cb = ioloop.PeriodicCallback(log_background_task_status, 60 * 3 * 1000)
    cb.start()
    return cb


def get_unfinished_task_count():
    """Get the number of active or pending background tasks. This function
    is used by the M&C subsystem.

    """
    n = 0

    for t in the_task_manager.tasks:
        if t.finish_time is None:
            n += 1

    return n


# Web user interface

@app.route('/tasks')
@login_required
def tasks():
    the_task_manager._maybe_purge_tasks()

    active = [t for t in the_task_manager.tasks
              if t.start_time is not None and t.finish_time is None]
    pending = [t for t in the_task_manager.tasks
               if t.start_time is None]
    finished = [t for t in the_task_manager.tasks
                if t.finish_time is not None]

    return render_template(
        'task-listing.html',
        title='Tasks',
        active=active,
        pending=pending,
        finished=finished,
    )
