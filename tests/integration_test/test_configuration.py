"""
Tests that we can successfully set up a server.
"""

import requests

from hera_librarian import LibrarianClient


def test_server(server):
    """
    Tests that the server fixture is working.
    """
    assert server


def test_simple_ping(server):
    """
    Tests that the server is up at all.
    """

    response = requests.get(f"http://localhost:{server.id}/")

    # Just check we got something (even if its a 404)
    assert response.status_code


def test_ping_server(librarian_client):
    """
    Tests that we can ping the server.
    """

    assert librarian_client.ping()
