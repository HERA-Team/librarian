"""
Exceptions for the hera_librarian client library.
"""


class LibrarianHTTPError(Exception):
    def __init__(self, url, status_code, reason, suggested_remedy):
        super(LibrarianHTTPError, self).__init__(
            f"HTTP request to {url} failed with status code {status_code} and reason {reason}."
        )
        self.url = url
        self.status_code = status_code
        self.reason = reason
        self.suggested_remedy = suggested_remedy


class LibrarianError(Exception):
    def __init__(self, message):
        super(LibrarianError, self).__init__(message)


class LibrarianClientRemovedFunctionality(Exception):
    def __Init__(self, name, message):
        super(LibrarianClientRemovedFunctionality, self).__init__(
            f"{name} is no longer avaialble in Librarian v2.0. {message}"
        )
