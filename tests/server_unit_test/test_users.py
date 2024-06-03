"""
Tests the /users endpoints.
"""

from hera_librarian.authlevel import AuthLevel
from hera_librarian.models.users import (
    UserAdministrationChangeResponse,
    UserAdministrationCreationRequest,
    UserAdministrationDeleteRequest,
    UserAdministrationGetRequest,
    UserAdministrationGetResponse,
    UserAdministrationPasswordChange,
    UserAdministrationUpdateRequest,
)


def test_create_user(test_server, test_client):
    response = test_client.post_with_auth(
        "/api/v2/users/create",
        headers={"Content-Type": "application/json"},
        content=UserAdministrationCreationRequest(
            username="test_user",
            password="test_password",
            permission=AuthLevel.READONLY,
        ).model_dump_json(),
    )

    assert response.status_code == 201
    response = UserAdministrationChangeResponse.model_validate_json(response.content)

    # See if the user was actually created.

    response = test_client.post_with_auth(
        "/api/v2/users/get",
        headers={"Content-Type": "application/json"},
        content=UserAdministrationGetRequest(username="test_user").model_dump_json(),
    )

    assert response.status_code == 200

    response = UserAdministrationGetResponse.model_validate_json(response.content)

    assert response.username == "test_user"

    # See if we can create a user with this username again.

    response = test_client.post_with_auth(
        "/api/v2/users/create",
        headers={"Content-Type": "application/json"},
        content=UserAdministrationCreationRequest(
            username="test_user",
            password="test_password",
            permission=AuthLevel.READONLY,
        ).model_dump_json(),
    )

    assert response.status_code == 400

    response = test_client.post_with_auth(
        "/api/v2/users/update",
        headers={"Content-Type": "application/json"},
        content=UserAdministrationUpdateRequest(
            username="test_user",
            password="new_password",
            permission=AuthLevel.READWRITE,
        ).model_dump_json(),
    )

    # See if _using_ the test_user we can create an account, we shouldn't be able to!
    response = test_client.post(
        "/api/v2/users/create",
        headers={"Content-Type": "application/json"},
        content=UserAdministrationCreationRequest(
            username="test_user",
            password="new_password",
            permission=AuthLevel.READONLY,
        ).model_dump_json(),
        auth=("test_user", "new_password"),
    )

    assert response.status_code == 401

    # But they can change their -own- password
    response = test_client.post(
        "/api/v2/users/password_update",
        headers={"Content-Type": "application/json"},
        content=UserAdministrationPasswordChange(
            username="test_user",
            password="new_password",
            new_password="new_new_password",
        ).model_dump_json(),
        auth=("test_user", "new_password"),
    )

    assert response.status_code == 200

    # Clean up

    response = test_client.post_with_auth(
        "/api/v2/users/delete",
        headers={"Content-Type": "application/json"},
        content=UserAdministrationDeleteRequest(username="test_user").model_dump_json(),
    )

    assert response.status_code == 200

    response = UserAdministrationChangeResponse.model_validate_json(response.content)

    # Check we can't get the user anymore

    response = test_client.post_with_auth(
        "/api/v2/users/get",
        headers={"Content-Type": "application/json"},
        content=UserAdministrationGetRequest(username="test_user").model_dump_json(),
    )

    assert response.status_code == 400
