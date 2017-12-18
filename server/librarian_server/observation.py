# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"Observations."

from __future__ import absolute_import, division, print_function, unicode_literals

__all__ = str('''
ObservingSession
Observation
''').split()

from flask import flash, redirect, render_template, url_for

from hera_librarian.utils import format_jd_as_calendar_date, format_jd_as_iso_date_time
from . import app, db
from .dbutil import NotNull, SQLAlchemyError
from .webutil import ServerError, json_api, login_required, optional_arg, required_arg


class ObservingSession (db.Model):
    """An ObservingSession is a sequence of contiguous, or nearly so, observations
    taken under uniform conditions.

    Ideally there each night of observing results in a batch of Observations
    that can be grouped into one ObservingSession. This might not happen if
    the correlator goes down or the hardware configuration was changed halfway
    through.

    The "id" of an ObservingSession is the obsid of the first observation it
    contains.

    This table doesn't (currently) contain much amazing information itself.
    Its main purposes is so that we can group Observations by their
    session_ids.

    ObservingSessions should not overlap.

    """
    __tablename__ = 'observing_session'

    id = db.Column(db.BigInteger, primary_key=True)
    start_time_jd = NotNull(db.Float(precision='53'))
    stop_time_jd = NotNull(db.Float(precision='53'))
    observations = db.relationship('Observation', back_populates='session')

    def __init__(self, id, start_time_jd, stop_time_jd):
        self.id = id
        self.start_time_jd = start_time_jd
        self.stop_time_jd = stop_time_jd
        self._validate()

    def _validate(self):
        """Check that this object's fields follow our invariants.

        """
        if not (self.start_time_jd < self.stop_time_jd):  # catches NaNs, just in case ...
            raise ValueError('session start time must precede stop time; got %f, %f'
                             % (self.start_time_jd, self.stop_time_jd))

    @property
    def start_time_calendar_date(self):
        "The session start time in YYYY-MM-DD format."
        return format_jd_as_calendar_date(self.start_time_jd)

    @property
    def start_time_iso_date_time(self):
        "The session start time in \"YYYY-MM-DD HH:MM:SS\" format."
        return format_jd_as_iso_date_time(self.start_time_jd)

    @property
    def stop_time_iso_date_time(self):
        "The session stop time in \"YYYY-MM-DD HH:MM:SS\" format."
        return format_jd_as_iso_date_time(self.stop_time_jd)

    @property
    def duration(self):
        "The duration of the session in days."
        return self.stop_time_jd - self.start_time_jd

    @property
    def num_obs(self):
        "The number of Observations associated with this session."
        from sqlalchemy import func
        return (db.session.query(func.count(Observation.obsid))
                .filter(Observation.session_id == self.id)
                .scalar())

    @property
    def num_files(self):
        "The number of Files associated with this session."
        from sqlalchemy import func
        from .file import File
        my_obsids = db.session.query(Observation.obsid).filter(Observation.session_id == self.id)
        return (db.session.query(func.count(File.name))
                .filter(File.obsid.in_(my_obsids))
                .scalar())

    @property
    def num_files_with_instances(self):
        """The number of Files associated with this session for which we have at least
        one FileInstance.

        """
        from sqlalchemy import distinct, func
        from .file import File, FileInstance
        my_obsids = db.session.query(Observation.obsid).filter(Observation.session_id == self.id)
        my_filenames = db.session.query(File.name).filter(File.obsid.in_(my_obsids))
        return (db.session.query(func.count(distinct(FileInstance.name)))
                .filter(FileInstance.name.in_(my_filenames))
                .scalar())

    @property
    def total_size(self):
        "The total size (in bytes) of all Files associated with this session."
        from sqlalchemy import func
        from .file import File
        my_obsids = db.session.query(Observation.obsid).filter(Observation.session_id == self.id)
        return (db.session.query(func.sum(File.size))
                .filter(File.obsid.in_(my_obsids))
                .scalar())

    def to_dict(self):
        return dict(
            id=self.id,
            start_time_jd=self.start_time_jd,
            stop_time_jd=self.stop_time_jd,
        )

    @classmethod
    def from_dict(cls, info):
        id = required_arg(info, int, 'id')
        start = required_arg(info, float, 'start_time_jd')
        stop = required_arg(info, float, 'stop_time_jd')
        return cls(id, start, stop)


class Observation (db.Model):
    """An Observation is a span of time during which we have probably taken data.
    Every File is associated with a single Observation.

    """
    __tablename__ = 'observation'

    obsid = db.Column(db.BigInteger, primary_key=True)
    start_time_jd = NotNull(db.Float(precision='53'))
    # XXX HACK: these should probably be NotNull. But in testing, we are creating
    # observations with add_obs_librarian, and it doesn't know these pieces of
    # information. Yet.
    stop_time_jd = db.Column(db.Float(precision='53'))
    start_lst_hr = db.Column(db.Float(precision='53'))
    session_id = db.Column(db.BigInteger, db.ForeignKey(ObservingSession.id), nullable=True)
    session = db.relationship('ObservingSession', back_populates='observations')
    files = db.relationship('File', back_populates='observation')

    def __init__(self, obsid, start_time_jd, stop_time_jd, start_lst_hr):
        self.obsid = obsid
        self.start_time_jd = start_time_jd
        self.stop_time_jd = stop_time_jd
        self.start_lst_hr = start_lst_hr
        self._validate()

    def _validate(self):
        """Check that this object's fields follow our invariants.

        """
        if self.stop_time_jd is not None and not (self.start_time_jd < self.stop_time_jd):
            raise ValueError('observation start time must precede stop time; got %f, %f'
                             % (self.start_time_jd, self.stop_time_jd))

    @property
    def duration(self):
        """Measured in days."""
        if self.stop_time_jd is None or self.start_time_jd is None:
            return float('NaN')
        return self.stop_time_jd - self.start_time_jd

    @property
    def total_size(self):
        "The total size (in bytes) of all Files associated with this observation."
        from sqlalchemy import func
        from .file import File
        return (db.session.query(func.sum(File.size))
                .filter(File.obsid == self.obsid)
                .scalar())

    def to_dict(self):
        return dict(
            obsid=self.obsid,
            start_time_jd=self.start_time_jd,
            stop_time_jd=self.stop_time_jd,
            start_lst_hr=self.start_lst_hr,
            session_id=self.session_id,
        )

    @classmethod
    def from_dict(cls, info):
        obsid = required_arg(info, int, 'obsid')
        start_jd = required_arg(info, float, 'start_time_jd')
        stop_jd = optional_arg(info, float, 'stop_time_jd')
        start_hr = optional_arg(info, float, 'start_lst_hr')
        sessid = optional_arg(info, int, 'session_id')

        obj = cls(obsid, start_jd, stop_jd, start_hr)
        obj.session_id = sessid
        return obj


# RPC endpoints

@app.route('/api/assign_observing_sessions', methods=['GET', 'POST'])
@json_api
def assign_observing_sessions(args, sourcename=None):
    """This call instructs the Librarian to find all Observations that
    are not currently assigned to an ObservingSession, look at their timing
    information to infer when the sessions in fact occurred, and create
    session objects and assign Observations to them.

    The optional "minimum_start_jd" and "maximum_start_jd" arguments can be
    used to limit the range of observations that are considered for session
    assignment. These can be useful if session assignment needs to happen when
    a new day's worth of data has been partially ingested.

    This should not be called while observing is ongoing! I'd like to avoid
    modifying records after creation when possible, and if we create a session
    whilst taking observations, the new observations will end up being
    associated with a new session. (Either that, or we'll have to implement
    logic to detect that certain observations represent a continuation of a
    previous session, which sounds tricky.)

    """
    minimum_start_jd = optional_arg(args, float, 'minimum_start_jd')
    maximum_start_jd = optional_arg(args, float, 'maximum_start_jd')

    new_sess_info = []
    retval = {'new_sessions': new_sess_info}

    # Build a list of all prior sessions so we can see if any Observations
    # must be assigned to preexisting sessions.

    existing_sessions = list(ObservingSession.query.all())

    # For all Observations without a session, either assign them to a
    # preexisting one (if they fall inside), or save them for followup.

    examine_obs = []
    query = Observation.query.filter(Observation.session_id == None)
    if minimum_start_jd is not None:
        query = query.filter(Observation.start_time_jd >= minimum_start_jd)
    if maximum_start_jd is not None:
        query = query.filter(Observation.start_time_jd <= maximum_start_jd)

    for obs in query.order_by(Observation.start_time_jd.asc()):
        # TODO: we've got some N^2 scaling here; we could do a better job.
        for sess in existing_sessions:
            if (obs.start_time_jd >= sess.start_time_jd and
                obs.start_time_jd <= sess.stop_time_jd and
                    (obs.stop_time_jd is None or (obs.stop_time_jd <= sess.stop_time_jd))):
                obs.session_id = sess.id
                break
        else:
            # This branch is triggered if the 'break' clause is not called,
            # i.e., this obs does not overlap an existing session.
            examine_obs.append(obs)

    try:
        db.session.commit()  # if there are any obs matching existing sessions
    except SQLAlchemyError:
        db.session.rollback()
        app.log_exception(sys.exc_info())
        raise ServerError('failed to commit obs changes to database; see logs for details')

    if not len(examine_obs):
        return retval

    # Now, create new sessions for the unassigned observations. From our SQL
    # query, examine_obs is ordered by start_time_jd. `gap_tol` is the size of
    # time gap that we allow before declaring that a new session has started,
    # in units of the smallest time gap between observations under
    # consideration.

    import numpy as np

    gap_tol = 20
    start_jds = np.array([o.start_time_jd for o in examine_obs])
    djds = np.diff(start_jds)
    i0 = 0
    n = len(examine_obs)

    while len(examine_obs[i0:]):
        if i0 == n - 1:
            # This is worrisome, but all we can do is trust that this is
            # legitimately a session that lasted only a single observation.
            i1 = i0 + 1
        else:
            # Set the allowed time gap, clamping to be between 1 minute and 0.5 day.
            gap = djds[i0] * gap_tol
            gap = np.clip(gap, 1. / 1440, 0.5)

            # i1 is used in Python slicing to mark the end of this session. We
            # know that we have at least two Observations, so we can set it past
            # i0.
            i1 = i0 + 1
            while i1 < n and start_jds[i1] - start_jds[i1 - 1] < gap:
                i1 += 1

        # OK, we now have a bunch of observations that we've decided are in
        # the same session. We can create an ObservingSession and assign them.

        sess_obs = examine_obs[i0:i1]
        start = sess_obs[0].start_time_jd
        if sess_obs[-1].stop_time_jd is None:
            raise ServerError(
                'new observations must have recorded stop times (ID %s)', sess_obs[0].obsid)
        stop = sess_obs[-1].stop_time_jd
        sess = ObservingSession(sess_obs[0].obsid, start, stop)
        db.session.add(sess)

        try:
            db.session.commit()
        except SQLAlchemyError:
            db.session.rollback()
            app.log_exception(sys.exc_info())
            raise ServerError('failed to commit obs changes to database (2); see logs for details')

        new_sess_info.append(dict(
            id=sess.id,
            start_time_jd=start,
            stop_time_jd=stop,
            n_obs=len(sess_obs),
        ))

        for obs in sess_obs:
            obs.session_id = sess.id

        i0 = i1

    try:
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        app.log_exception(sys.exc_info())
        raise ServerError('failed to commit obs changes to database (3); see logs for details')

    return retval


@app.route('/api/describe_session_without_event', methods=['GET', 'POST'])
@json_api
def describe_session_without_event(args, sourcename=None):
    """Return information about the files in a session that does not contain a
    particular FileEvent.

    This awfully-specific API call is part of the framework that allows the
    RTP to ask the Librarian about new files that need processing. The RTP
    needs to learn about all of the observations in a session at once since it
    cares about "neighbors" in its processing. Within a session, it cares
    about uv files that need processing; for each of those files, it needs to
    be told:

      date        -- the start (??) Julian Date of the relevant Observation
      pol         -- the polarization of the data ("xx" or "yy")
      store_path  -- the path of a file instance *within* a store
      path_prefix -- the store's path prefix, used to construct full paths
      host        -- the hostname of the store
      length      -- the duration of the observation in days

    Of course, we only want to notify the RTP about data that it's not already
    aware of. Once the RTP learns of a file it never deletes that information,
    so we just need to keep track of sessions about which RTP has not been
    notified. We have the caller give us the name of a FileEvent to use to
    maintain that information (so in principle we could have two RTP instances
    that each kept track separately, etc.).

    As one final bit of hack, the caller specifies the "source" string of the
    files that it cares about. For the RTP this will generally be whatever
    "source" raw correlator data have.

    """
    source = required_arg(args, unicode, 'source')
    event_type = required_arg(args, unicode, 'event_type')

    # Search for files that (1) are assigned to a session, (2) come from the
    # desired source, and (3) do not (yet) have the notification event.

    from .file import File, FileEvent, FileInstance

    already_done_file_names = (db.session.query(File.name)
                               .join(FileEvent)
                               .filter(FileEvent.type == event_type,
                                       File.name == FileEvent.name))
    files_of_interest = (File.query.join(Observation)
                         .filter(Observation.session_id != None,
                                 File.source == source,
                                 File.name.notin_(already_done_file_names)))

    file = files_of_interest.first()
    if file is None:
        # All currently known sessions have been reported.
        return {'any_matching': False}

    sessid = file.observation.session_id

    # As a huge hack, we don't currently know the duration of individual
    # observations, so we set up to infer them from the spacing of all of
    # observations in the session.

    import numpy as np
    obs = list(Observation.query.filter(Observation.session_id == sessid))
    start_jds = np.array(sorted(o.start_time_jd for o in obs))
    djds = np.diff(start_jds)
    typ_djd = np.median(djds)

    def get_len(o):
        if o.stop_time_jd is not None:
            return o.duration
        return typ_djd

    djds = dict((o.obsid, get_len(o)) for o in obs)

    # Now collect information from relevant FileInstances. To get everything
    # we need to need to do a *big* join: FileInstance (store_path), File
    # (source), Observation (start_time, session_id), Store (path_prefix,
    # ssh_host).

    from .store import Store
    from hera_librarian import utils

    records = []
    seen_names = set()

    for inst, f, obs, store in (db.session.query(FileInstance, File, Observation, Store)
                                .filter(Observation.session_id == sessid,
                                        File.name == FileInstance.name,
                                        File.source == source,
                                        Observation.obsid == File.obsid,
                                        Store.id == FileInstance.store)).order_by(Observation.start_time_jd.asc()):
        if f.name in seen_names:
            continue

        records.append({
            'date': obs.start_time_jd,
            'pol': utils.get_pol_from_path(f.name),
            'store_path': inst.store_path,
            'path_prefix': store.path_prefix,
            'host': store.ssh_host,
            'length': djds[f.obsid],
        })
        seen_names.add(f.name)

    return {
        'any_matching': True,
        'info': records,
        'session_id': sessid,
    }


# Web user interface

@app.route('/observations')
@login_required
def observations():
    q = Observation.query.order_by(Observation.start_time_jd.desc()).limit(50)
    return render_template(
        'obs-listing.html',
        title='Observations',
        obs=q
    )


@app.route('/observations/<int:obsid>')
@login_required
def specific_observation(obsid):
    obs = Observation.query.get(obsid)
    if obs is None:
        flash('No such observation %r known' % obsid)
        return redirect(url_for('observations'))

    from .file import File

    files = list(File.query.filter(File.obsid == obsid).order_by(File.name.asc()))

    return render_template(
        'obs-individual.html',
        title='Observation %d' % obsid,
        obs=obs,
        files=files,
    )


@app.route('/sessions/all')
@login_required
def sessions_all():
    q = list(ObservingSession.query.order_by(ObservingSession.start_time_jd.desc()))
    return render_template(
        'session-listing-all.html',
        title='All Observing Sessions',
        sess=q
    )


@app.route('/sessions/recent')
@login_required
def sessions_recent():
    q = list(ObservingSession.query.order_by(ObservingSession.start_time_jd.desc()).limit(30))
    return render_template(
        'session-listing-recent.html',
        title='Recent Observing Sessions',
        sess=q
    )


@app.route('/sessions/<int:id>')
@login_required
def specific_session(id):
    sess = ObservingSession.query.get(id)
    if sess is None:
        flash('No such observing session %r known' % id)
        return redirect(url_for('sessions'))

    obs = list(Observation.query.filter(Observation.session_id ==
                                        id).order_by(Observation.start_time_jd.asc()))

    return render_template(
        'session-individual.html',
        title='Observing Session %d' % id,
        sess=sess,
        obs=obs,
    )
