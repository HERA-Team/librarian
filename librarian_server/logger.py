"""
Logging setup. Use this as 'from logger import log'
"""

import inspect
import logging as log

from sqlalchemy.orm import Session

from hera_librarian.errors import ErrorCategory, ErrorSeverity

from .settings import server_settings

logging_level = log.getLevelName(server_settings.log_level)

log.basicConfig(
    encoding="utf-8",
    level=logging_level,
    format="(%(module)s:%(funcName)s) [%(asctime)s] {%(levelname)s}:%(message)s",
)

error_severity_to_logging_level = {
    ErrorSeverity.CRITICAL: log.CRITICAL,
    ErrorSeverity.ERROR: log.ERROR,
    ErrorSeverity.WARNING: log.WARNING,
    ErrorSeverity.INFO: log.INFO,
}

log.debug("Logging set up.")


def log_to_database(
    severity: ErrorSeverity, category: ErrorCategory, message: str, session: Session
) -> None:
    """
    Log an error to the database.

    Parameters
    ----------

    severity : ErrorSeverity
        The severity of this error.
    category : ErrorCategory
        The category of this error.
    message : str
        The message describing this error.
    session : Session
        The database session to use.

    Notes
    -----

    Automatically stores the above frame's file name, function, and line number in
    the 'caller' field of the error.
    """

    # Avoid circular imports.
    from .orm.errors import Error

    log_level = error_severity_to_logging_level[severity]
    log.log(log_level, message)

    caller = (
        inspect.stack()[1].filename
        + ":"
        + inspect.stack()[1].function
        + ":"
        + str(inspect.stack()[1].lineno)
    )

    error = Error.new_error(severity, category, message, caller=caller)

    session.add(error)
    session.commit()
