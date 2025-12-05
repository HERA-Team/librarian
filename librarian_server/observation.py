# Copyright 2016 the HERA Collaboration
# Licensed under the BSD License.

"Observations."


__all__ = str(
    """
ObservingSession
Observation
"""
).split()

from hera_librarian.utils import format_jd_as_calendar_date, format_jd_as_iso_date_time

from . import db
from .dbutil import NotNull
from .webutil import optional_arg, required_arg


class ObservingSession(db.Model):
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

    __tablename__ = "observing_session"

    id = db.Column(db.BigInteger, primary_key=True)
    start_time_jd = NotNull(db.Float(precision="53"))
    stop_time_jd = NotNull(db.Float(precision="53"))
    observations = db.relationship("Observation", back_populates="session")

    def __init__(self, id, start_time_jd, stop_time_jd):
        self.id = id
        self.start_time_jd = start_time_jd
        self.stop_time_jd = stop_time_jd
        self._validate()

    def _validate(self):
        """Check that this object's fields follow our invariants."""
        if not (self.start_time_jd < self.stop_time_jd):  # catches NaNs, just in case ...
            raise ValueError(
                "session start time must precede stop time; got %f, %f"
                % (self.start_time_jd, self.stop_time_jd)
            )

    @property
    def start_time_calendar_date(self):
        "The session start time in YYYY-MM-DD format."
        return format_jd_as_calendar_date(self.start_time_jd)

    @property
    def start_time_iso_date_time(self):
        'The session start time in "YYYY-MM-DD HH:MM:SS" format.'
        return format_jd_as_iso_date_time(self.start_time_jd)

    @property
    def stop_time_iso_date_time(self):
        'The session stop time in "YYYY-MM-DD HH:MM:SS" format.'
        return format_jd_as_iso_date_time(self.stop_time_jd)

    @property
    def duration(self):
        "The duration of the session in days."
        return self.stop_time_jd - self.start_time_jd

    @property
    def num_obs(self):
        "The number of Observations associated with this session."
        from sqlalchemy import func

        return (
            db.session.query(func.count(Observation.obsid))
            .filter(Observation.session_id == self.id)
            .scalar()
        )

    @property
    def num_files(self):
        "The number of Files associated with this session."
        from sqlalchemy import func

        from .file import File

        my_obsids = db.session.query(Observation.obsid).filter(Observation.session_id == self.id)
        return db.session.query(func.count(File.name)).filter(File.obsid.in_(my_obsids)).scalar()

    @property
    def num_files_with_instances(self):
        """The number of Files associated with this session for which we have at least
        one FileInstance.

        """
        from sqlalchemy import distinct, func

        from .file import File, FileInstance

        my_obsids = db.session.query(Observation.obsid).filter(Observation.session_id == self.id)
        my_filenames = db.session.query(File.name).filter(File.obsid.in_(my_obsids))
        return (
            db.session.query(func.count(distinct(FileInstance.name)))
            .filter(FileInstance.name.in_(my_filenames))
            .scalar()
        )

    @property
    def total_size(self):
        "The total size (in bytes) of all Files associated with this session."
        from sqlalchemy import func

        from .file import File

        my_obsids = db.session.query(Observation.obsid).filter(Observation.session_id == self.id)
        return db.session.query(func.sum(File.size)).filter(File.obsid.in_(my_obsids)).scalar()

    def to_dict(self):
        return dict(id=self.id, start_time_jd=self.start_time_jd, stop_time_jd=self.stop_time_jd)

    @classmethod
    def from_dict(cls, info):
        _id = required_arg(info, int, "id")
        start = required_arg(info, float, "start_time_jd")
        stop = required_arg(info, float, "stop_time_jd")
        return cls(_id, start, stop)


class Observation(db.Model):
    """An Observation is a span of time during which we have probably taken data.
    Every File is associated with a single Observation.

    """

    __tablename__ = "observation"

    obsid = db.Column(db.BigInteger, primary_key=True)
    start_time_jd = NotNull(db.Float(precision="53"))
    # XXX HACK: these should probably be NotNull. But in testing, we are creating
    # observations with add_obs_librarian, and it doesn't know these pieces of
    # information. Yet.
    stop_time_jd = db.Column(db.Float(precision="53"))
    start_lst_hr = db.Column(db.Float(precision="53"))
    session_id = db.Column(db.BigInteger, db.ForeignKey(ObservingSession.id), nullable=True)
    session = db.relationship("ObservingSession", back_populates="observations")
    files = db.relationship("File", back_populates="observation")

    def __init__(self, obsid, start_time_jd, stop_time_jd, start_lst_hr):
        self.obsid = obsid
        self.start_time_jd = start_time_jd
        self.stop_time_jd = stop_time_jd
        self.start_lst_hr = start_lst_hr
        self._validate()

    def _validate(self):
        """Check that this object's fields follow our invariants."""
        if self.stop_time_jd is not None and not (self.start_time_jd < self.stop_time_jd):
            raise ValueError(
                "observation start time must precede stop time; got %f, %f"
                % (self.start_time_jd, self.stop_time_jd)
            )

    @property
    def duration(self):
        """Measured in days."""
        if self.stop_time_jd is None or self.start_time_jd is None:
            return float("NaN")
        return self.stop_time_jd - self.start_time_jd

    @property
    def total_size(self):
        "The total size (in bytes) of all Files associated with this observation."
        from sqlalchemy import func

        from .file import File

        return db.session.query(func.sum(File.size)).filter(File.obsid == self.obsid).scalar()

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
        obsid = required_arg(info, int, "obsid")
        start_jd = required_arg(info, float, "start_time_jd")
        stop_jd = optional_arg(info, float, "stop_time_jd")
        start_hr = optional_arg(info, float, "start_lst_hr")
        sessid = optional_arg(info, int, "session_id")

        obj = cls(obsid, start_jd, stop_jd, start_hr)
        obj.session_id = sessid
        return obj
