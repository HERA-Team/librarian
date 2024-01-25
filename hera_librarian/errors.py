"""
Error enumeraion for the librarian. Categories of errors.
"""

from enum import Enum

class ErrorSeverity(Enum):
    """
    Severity of errors.
    """

    CRITICAL = "critical"
    "Critical errors are those that need to be fixed immediately."

    ERROR = "error"
    "Errors are those that need to be fixed, but are not critical."

    WARNING = "warning"
    "Warnings are errors that are not critical, but still need to be fixed."

    INFO = "info"
    "Informational errors are those that are not critical and do not need to be fixed."


class ErrorCategory(Enum):
    """
    Categories of errors.
    """

    DATA_INTEGRITY = "data_integrity"
    "Data integrity errors are those that indicate that the data on the librarian is not correct (has the wrong checksum)."

    DATA_AVAILABILITY = "data_availability"
    "Data availability errors are those that indicate that data available in the database is not available on the librarian."

    CONFIGURATION = "configuration"
    "Configuration errors are those that indicate that the librarian has been configured incorrectly."
