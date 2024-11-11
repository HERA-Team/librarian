"""
ORM for 'errors' table, describing (potentially critical) errors
that need to be remedied by an outside entity.
"""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from hera_librarian.errors import ErrorCategory, ErrorSeverity

from .. import database as db


class Error(db.Base):
    """
    Represents an error that needs to be fixed.
    """

    __tablename__ = "errors"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True, unique=True)
    "The unique ID of this error."
    severity = db.Column(db.Enum(ErrorSeverity), nullable=False)
    "The severity of this error."
    category = db.Column(db.Enum(ErrorCategory), nullable=False)
    "The category of this error."
    message = db.Column(db.String, nullable=False)
    "The message describing this error."
    raised_time = db.Column(db.DateTime, nullable=False)
    "The time at which this error was raised."
    cleared_time = db.Column(db.DateTime, nullable=True)
    "The time at which this error was cleared."
    cleared = db.Column(db.Boolean, nullable=False)
    "Whether or not this error has been cleared."
    caller = db.Column(db.String(256), nullable=True)
    "The caller that raised this error."

    @classmethod
    def new_error(
        self,
        severity: ErrorSeverity,
        category: ErrorCategory,
        message: str,
        caller: Optional[str] = None,
    ) -> "Error":
        """
        Create a new error object.

        Parameters
        ----------
        severity : ErrorSeverity
            The severity of this error.
        category : ErrorCategory
            The category of this error.
        message : str
            The message describing this error.
        """
        return Error(
            severity=severity,
            category=category,
            message=message,
            raised_time=datetime.now(timezone.utc),
            cleared_time=None,
            cleared=False,
            caller=caller,
        )

    def clear(self, session: Session) -> None:
        """
        Clear this error.

        Parameters
        ----------
        session : Session
            The database session to use.
        """

        self.cleared = True
        self.cleared_time = datetime.now(timezone.utc)
        session.commit()
