"""
A simple test for the secondary server to make sure
all the configurations are worked out. This uses both
the primary and secondary server.
"""

from hera_librarian.models.ping import PingRequest, PingResponse


def test_secondary_server_simple(
    test_client,
    librarian_client,
):
    # Note librarian_client is an instance of LibrarianClient,
    # and test_client is an instance of TestClient from FastAPI.
    # So we need to integrate with them differently.

    librarian_client.ping()

    response = test_client.post(
        "/api/v2/ping",
        content=PingRequest().model_dump_json(),
    )

    assert response.status_code == 200

    decoded_response = PingResponse.model_validate_json(response.content)
