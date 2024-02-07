"""
Tests the user administration capabilities of the client.
"""

import pytest

from hera_librarian import LibrarianClient
from hera_librarian.authlevel import AuthLevel
from hera_librarian.exceptions import LibrarianHTTPError


def test_create_user(server, admin_client):
    admin_client.create_user("test_user", "test_password", AuthLevel.READONLY)

    user = admin_client.get_user("test_user")

    assert user.username == "test_user"
    assert user.permission == AuthLevel.READONLY

    admin_client.update_user("test_user", "new_password", AuthLevel.READWRITE)

    low_level_client = LibrarianClient(
        host=admin_client.host,
        port=admin_client.port,
        user="test_user",
        password="new_password",
    )

    low_level_client.change_password(
        current_password="new_password", new_password="fake_password"
    )

    with pytest.raises(LibrarianHTTPError):
        low_level_client.ping(require_login=True)

    low_level_client = LibrarianClient(
        host=admin_client.host,
        port=admin_client.port,
        user="test_user",
        password="fake_password",
    )

    low_level_client.ping(require_login=True)

    admin_client.delete_user("test_user")

    with pytest.raises(LibrarianHTTPError):
        admin_client.get_user("test_user")

    return
