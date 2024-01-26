"""
Test the search endpoint.
"""

import datetime

from hera_librarian.models.errors import (ErrorSearchFailedResponse,
                                          ErrorSearchRequest,
                                          ErrorSearchResponses)
from hera_librarian.models.search import (FileSearchFailedResponse,
                                          FileSearchRequest,
                                          FileSearchResponse,
                                          FileSearchResponses)


def test_search_by_filename(test_server_with_many_files_and_errors, test_client):
    request = FileSearchRequest(name="many_server_example_file_0.txt")

    response = test_client.post(
        "/api/v2/search/file",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
    )

    assert response.status_code == 200

    response = FileSearchResponses.model_validate_json(response.content)


def test_search_by_created_time(test_server_with_many_files_and_errors, test_client):
    request = FileSearchRequest(
        create_time_window=(
            datetime.datetime.utcnow() - datetime.timedelta(days=1),
            datetime.datetime.utcnow(),
        )
    )

    response = test_client.post(
        "/api/v2/search/file",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
    )

    assert response.status_code == 200

    response = FileSearchResponses.model_validate_json(response.content)


def test_search_by_source(test_server_with_many_files_and_errors, test_client):
    request = FileSearchRequest(source="test")

    response = test_client.post(
        "/api/v2/search/file",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
    )

    assert response.status_code == 200

    response = FileSearchResponses.model_validate_json(response.content)


def test_search_by_uploader(test_server_with_many_files_and_errors, test_client):
    request = FileSearchRequest(uploader="test")

    response = test_client.post(
        "/api/v2/search/file",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
    )

    assert response.status_code == 200

    response = FileSearchResponses.model_validate_json(response.content)


def test_failed_search(test_server_with_many_files_and_errors, test_client):
    request = FileSearchRequest(name="this_file_does_not_exist")

    response = test_client.post(
        "/api/v2/search/file",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
    )

    assert response.status_code == 404

    response = FileSearchFailedResponse.model_validate_json(response.content)


def test_error_all_search(
    test_server_with_many_files_and_errors, test_client, test_orm
):
    request = ErrorSearchRequest(include_resolved=False)

    response = test_client.post(
        "/api/v2/search/error",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
    )

    assert response.status_code == 200

    response = ErrorSearchResponses.model_validate_json(response.content)

    for model in response.root:
        assert model.cleared is False


def test_failed_error_search(test_server_with_many_files_and_errors, test_client):
    request = ErrorSearchRequest(id=-1)

    response = test_client.post(
        "/api/v2/search/error",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
    )

    assert response.status_code == 404

    response = ErrorSearchFailedResponse.model_validate_json(response.content)
