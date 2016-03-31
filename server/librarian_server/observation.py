# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"Observations."

from __future__ import absolute_import, division, print_function, unicode_literals

__all__ = str('''
Observation
''').split ()

from flask import flash, redirect, render_template, url_for

from . import app, db
from .dbutil import NotNull
from .webutil import RPCError, json_api, login_required


class Observation (db.Model):
    """An Observation is a span of time during which we have probably taken data.
    Every File is associated with a single Observation.

    """
    __tablename__ = 'observation'

    obsid = db.Column (db.Integer, primary_key=True)
    start_time_jd = NotNull (db.Float)
    stop_time_jd = NotNull (db.Float)
    start_lst_hr = NotNull (db.Float)


    def __init__ (self, obsid, start_time_jd, stop_time_jd, start_lst_hr):
        self.obsid = obsid
        self.start_time_jd = start_time_jd
        self.stop_time_jd = stop_time_jd
        self.start_lst_hr = start_lst_hr

    @property
    def duration (self):
        """Measured in days."""
        return self.stop_time_jd - self.start_time_jd


# TODO: RPC endpoints


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
