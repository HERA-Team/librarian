"""
Tests that we can successfully set up a server.
"""

import requests
from hera_librarian import LibrarianClient


def test_server(start_server):
    """
    Tests that the server fixture is working.
    """
    assert start_server
    

def test_simple_ping(start_server):
    """
    Tests that the server is up at all.
    """

    response = requests.get(f"http://localhost:{start_server.id}/")

    # Just check we got something (even if its a 404)
    assert response.status_code
    

def test_ping_server(start_server):
    """
    Tests that we can ping the server.
    """

    client = LibrarianClient(
        conn_name="test",
        conn_config={
            "url": f"http://localhost:{start_server.id}/",
            "authenticator": None
        }
    )

    assert client.ping()