# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"Observations."

from __future__ import absolute_import, division, print_function, unicode_literals

__all__ = str('''
ObservingSession
Observation
''').split ()

from flask import flash, redirect, render_template, url_for

from . import app, db
from .dbutil import NotNull
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

    TODO: we ought to synchronize these records between Librarians when
    copying data, but currently don't. Implementation of that is blocking on
    figuring out a coherent story for general Librarian data syncing. We can
    get away with this since Observations are not *required* to be associated
    with a session in the DB.

    """
    __tablename__ = 'observing_session'

    id = db.Column (db.Integer, primary_key=True)
    start_time_jd = NotNull (db.Float)
    stop_time_jd = NotNull (db.Float)
    observations = db.relationship ('Observation', back_populates='session')

    def __init__ (self, id, start_time_jd, stop_time_jd):
        self.id = id
        self.start_time_jd = start_time_jd
        self.stop_time_jd = stop_time_jd


    @property
    def duration (self):
        "The duration of the session in days."
        return self.stop_time_jd - self.start_time_jd


class Observation (db.Model):
    """An Observation is a span of time during which we have probably taken data.
    Every File is associated with a single Observation.

    """
    __tablename__ = 'observation'

    obsid = db.Column (db.Integer, primary_key=True)
    start_time_jd = NotNull (db.Float)
    # XXX HACK: these should probably be NotNull. But in testing, we are creating
    # observations with add_obs_librarian, and it doesn't know these pieces of
    # information. Yet.
    stop_time_jd = db.Column (db.Float)
    start_lst_hr = db.Column (db.Float)
    session_id = db.Column (db.Integer, db.ForeignKey (ObservingSession.id), nullable=True)
    session = db.relationship ('ObservingSession', back_populates='observations')
    files = db.relationship ('File', back_populates='observation')

    def __init__ (self, obsid, start_time_jd, stop_time_jd, start_lst_hr):
        self.obsid = obsid
        self.start_time_jd = start_time_jd
        self.stop_time_jd = stop_time_jd
        self.start_lst_hr = start_lst_hr

    @property
    def duration (self):
        """Measured in days."""
        if self.stop_time_jd is None or self.start_time_jd is None:
            return float ('NaN')
        return self.stop_time_jd - self.start_time_jd


# RPC endpoints

@app.route ('/api/create_or_update_observation', methods=['GET', 'POST'])
@json_api
def create_or_update_observation (args, sourcename=None):
    obsid = required_arg (args, int, 'obsid')
    start_time_jd = required_arg (args, float, 'start_time_jd')
    stop_time_jd = optional_arg (args, float, 'stop_time_jd')
    start_lst_hr = optional_arg (args, float, 'start_lst_hr')

    obs = Observation (obsid, start_time_jd, stop_time_jd, start_lst_hr)
    db.session.merge (obs)
    db.session.commit ()
    return {}


@app.route ('/api/assign_observing_sessions', methods=['GET', 'POST'])
@json_api
def assign_observing_sessions (args, sourcename=None):
    """This call instructs the Librarian to find all Observations that
    are not currently assigned to an ObservingSession, look at their timing
    information to infer when the sessions in fact occurred, and create
    session objects and assign Observations to them.

    This should not be called while observing is ongoing! I'd like to avoid
    modifying records after creation when possible, and if we create a session
    whilst taking observations, the new observations will end up being
    associated with a new session. (Either that, or we'll have to implement
    logic to detect that certain observations represent a continuation of a
    previous session, which sounds tricky.)

    """
    new_sess_info = []
    retval = {'new_sessions': new_sess_info}

    # Build a list of all prior sessions so we can see if any Observations
    # must be assigned to preexisting sessions.

    existing_sessions = list (ObservingSession.query.all ())

    # For all Observations without a session, either assign them to a
    # preexisting one (if they fall inside), or save them for followup.

    examine_obs = []

    for obs in Observation.query.filter (Observation.session_id == None).order_by (Observation.start_time_jd.asc ()):
        # TODO: we've got some N^2 scaling here; we could do a better job.
        for sess in existing_sessions:
            if (obs.start_time_jd >= sess.start_time_jd and
                (obs.stop_time_jd is None or (obs.stop_time_jd <= sess.stop_time_jd))):
                obs.session_id = sess.id
                break
        else:
            # This branch is triggered if the 'break' clause is not called,
            # i.e., this obs does not overlap an existing session.
            examine_obs.append (obs)

    db.session.commit () # if there are any obs matching existing sessions

    if not len (examine_obs):
        return retval

    # Now, create new sessions for the unassigned observations. From our SQL
    # query, examine_obs is ordered by start_time_jd. `gap_tol` is the size of
    # time gap that we allow before declaring that a new session has started,
    # in units of the smallest time gap between observations under
    # consideration.

    import numpy as np

    gap_tol = 20
    start_jds = np.array ([o.start_time_jd for o in examine_obs])
    djds = np.diff (start_jds)
    i0 = 0
    n = len (examine_obs)

    while len (examine_obs[i0:]):
        if i0 == n - 1:
            # This is worrisome, but all we can do is trust that this is
            # legitimately a session that lasted only a single observation.
            i1 = i0 + 1
        else:
            # Set the allowed time gap, clamping to be between 1 minute and 0.5 day.
            gap = djds[i0] * gap_tol
            gap = np.clip (gap, 1./1440, 0.5)

            # i1 is used in Python slicing to mark the end of this session. We
            # know that we have at least two Observations, so we can set it past
            # i0.
            i1 = i0 + 1
            while i1 < n and start_jds[i1] - start_jds[i1-1] < gap:
                i1 += 1

        # OK, we now have a bunch of observations that we've decided are in
        # the same session. We can create an ObservingSession and assign them.
        # TEMPORARY?: We don't fill in stop times, so we need to guess them
        # from the gaps between the observations. Life would be way easier if
        # we just filled them in to the Observation rows from the start.

        sess_obs = examine_obs[i0:i1]

        if len (sess_obs) == 1:
            # We're inferring stop times from start time gaps so we can't work
            # with a one-Obs session. Shouldn't happen anyway, but ...
            raise ServerError ('not implemented: making session out of single observation')

        start = sess_obs[0].start_time_jd
        typ_djd = np.median (djds[i0:i1-1]) # note: fewer DJDs than start times since they're differential

        if sess_obs[-1].stop_time_jd is not None:
            stop = sess_obs[-1].stop_time_jd
        else:
            stop = sess_obs[-1].start_time_jd + typ_djd

        sess = ObservingSession (sess_obs[0].obsid, start, stop)
        db.session.add (sess)
        db.session.commit ()

        new_sess_info.append (dict (
            start_time_jd = start,
            stop_time_jd = stop,
            n_obs = len (sess_obs),
        ))

        for obs in sess_obs:
            obs.session_id = sess.id

        i0 = i1

    db.session.commit ()
    return retval


@app.route ('/api/describe_session_without_event', methods=['GET', 'POST'])
@json_api
def describe_session_without_event (args, sourcename=None):
    """Return information about the files in a session that does not contain a
    particular FileEvent.

    This awfully-specific API call is part of the framework that allows the
    RTP to ask the Librarian about new files that need processing. The RTP
    needs to learn about all of the observations in a session at once since it
    cares about "neighbors" in its processing. Within a session, it cares
    about uv files that need processing; for each of those files, it needs to
    be told:

      date   -- the start (??) Julian Date of the relevant Observation
      pol    -- the polarization of the data ("xx" or "yy")
      path   -- the full path to a file instance in a store
      host   -- the hostname of the store
      length -- the duration of the observation in days

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
    source = required_arg (args, unicode, 'source')
    event_type = required_arg (args, unicode, 'event_type')

    # Search for a file that (1) is assigned to a session and (2) does not
    # have the notification event

    from .file import File, FileEvent, FileInstance

    already_done_file_names = (db.session.query (File.name)
                               .join (FileEvent)
                               .filter (FileEvent.type == event_type,
                                        File.name == FileEvent.name))
    files_of_interest = (File.query.join (Observation)
         .filter (Observation.session_id != None,
                  File.name.notin_ (already_done_file_names)))

    file = files_of_interest.first ()
    if file is None:
        # All currently known sessions have been reported.
        return {'any_matching': False}

    sessid = file.observation.session_id

    # As a huge hack, we don't currently know the duration of individual
    # observations, so we set up to infer them from the spacing of all of
    # observations in the session.

    import numpy as np
    obs = list (Observation.query.filter (Observation.session_id == sessid))
    start_jds = np.array (sorted (o.start_time_jd for o in obs))
    djds = np.diff (start_jds)
    typ_djd = np.median (djds)

    def get_len (o):
        if o.stop_time_jd is not None:
            return o.duration
        return typ_djd

    djds = dict ((o.obsid, get_len (o)) for o in obs)

    # Now collect information from relevant FileInstances. XXX: missing
    # instance handling. NEED: sessid, obsid, pol, path, host => Observation
    # (sessid), Store (host), FileInstance (name, parent_dirs, store_id), File
    # (obsid). FILTER: source, sessid, at most one instance per file.

    from .store import Store
    from hera_librarian import utils

    records = []
    seen_names = set ()

    for inst, f, obs, store in (db.session.query (FileInstance, File, Observation, Store)
                                .filter (Observation.session_id == sessid,
                                         File.name == FileInstance.name,
                                         File.source == source,
                                         Observation.obsid == File.obsid,
                                         Store.id == FileInstance.store)).order_by (Observation.start_time_jd.asc ()):
        if f.name in seen_names:
            continue

        records.append ({
            'date': obs.start_time_jd,
            'pol': utils.get_pol_from_path (f.name),
            'path': inst.full_path_on_store (),
            'host': store.ssh_host,
            'length': djds[f.obsid],
        })
        seen_names.add (f.name)

    return {
        'any_matching': True,
        'info': records,
    }


# Web user interface

@app.route ('/observations')
@login_required
def observations ():
    q = Observation.query.order_by (Observation.start_time_jd.desc ()).limit (50)
    return render_template (
        'obs-listing.html',
        title='Observations',
        obs=q
    )


@app.route ('/observations/<int:obsid>')
@login_required
def specific_observation (obsid):
    obs = Observation.query.get (obsid)
    if obs is None:
        flash ('No such observation %r known' % obsid)
        return redirect (url_for ('observations'))

    from .file import File

    files = list (File.query.filter (File.obsid == obsid).order_by (File.name.asc ()))

    return render_template (
        'obs-individual.html',
        title='Observation %d' % obsid,
        obs=obs,
        files=files,
    )


@app.route ('/sessions')
@login_required
def sessions ():
    q = ObservingSession.query.order_by (ObservingSession.start_time_jd.desc ()).limit (50)
    return render_template (
        'session-listing.html',
        title='Observing Sessions',
        sess=q
    )


@app.route ('/sessions/<int:id>')
@login_required
def specific_session (id):
    sess = ObservingSession.query.get (id)
    if sess is None:
        flash ('No such observing session %r known' % id)
        return redirect (url_for ('sessions'))

    obs = list (Observation.query.filter (Observation.session_id == id).order_by (Observation.start_time_jd.asc ()))

    return render_template (
        'session-individual.html',
        title='Observing Session %d' % id,
        sess=sess,
        obs=obs,
    )
