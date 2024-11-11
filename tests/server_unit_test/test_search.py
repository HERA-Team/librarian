"""
Test the search endpoint.
"""

import datetime

from hera_librarian.models.errors import (
    ErrorCategory,
    ErrorSearchFailedResponse,
    ErrorSearchRequest,
    ErrorSearchResponses,
    ErrorSeverity,
)
from hera_librarian.models.search import (
    FileSearchFailedResponse,
    FileSearchRequest,
    FileSearchResponse,
    FileSearchResponses,
)


def test_search_by_filename(test_server_with_many_files_and_errors, test_client):
    request = FileSearchRequest(name="many_server_example_file_0.txt")

    response = test_client.post_with_auth(
        "/api/v2/search/file",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
    )

    assert response.status_code == 200

    response = FileSearchResponses.model_validate_json(response.content)


def test_search_by_created_time(test_server_with_many_files_and_errors, test_client):
    request = FileSearchRequest(
        create_time_window=(
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1),
            datetime.datetime.now(datetime.timezone.utc),
        )
    )

    response = test_client.post_with_auth(
        "/api/v2/search/file",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
    )

    assert response.status_code == 200

    response = FileSearchResponses.model_validate_json(response.content)


def test_search_by_source(test_server_with_many_files_and_errors, test_client):
    request = FileSearchRequest(source="test")

    response = test_client.post_with_auth(
        "/api/v2/search/file",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
    )

    assert response.status_code == 200

    response = FileSearchResponses.model_validate_json(response.content)


def test_search_by_uploader(test_server_with_many_files_and_errors, test_client):
    request = FileSearchRequest(uploader="test")

    response = test_client.post_with_auth(
        "/api/v2/search/file",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
    )

    assert response.status_code == 200

    response = FileSearchResponses.model_validate_json(response.content)


def test_failed_search(test_server_with_many_files_and_errors, test_client):
    request = FileSearchRequest(name="this_file_does_not_exist")

    response = test_client.post_with_auth(
        "/api/v2/search/file",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
    )

    assert response.status_code == 404

    response = FileSearchFailedResponse.model_validate_json(response.content)


def test_error_all_search(
    test_server_with_many_files_and_errors, test_client, test_orm
):
    def make_request(request):
        response = test_client.post_with_auth(
            "/api/v2/search/error",
            headers={"Content-Type": "application/json"},
            content=request.model_dump_json(),
        )

        assert response.status_code == 200

        return ErrorSearchResponses.model_validate_json(response.content).root

    response = make_request(ErrorSearchRequest(include_resolved=False))

    for model in response:
        assert model.cleared is False

    response = make_request(ErrorSearchRequest(include_resolved=True))

    includes_cleared = False
    for model in response:
        includes_cleared = includes_cleared or model.cleared

    assert includes_cleared

    response = make_request(ErrorSearchRequest(max_results=1))

    assert len(response) == 1

    response = make_request(ErrorSearchRequest(severity=ErrorSeverity.CRITICAL))

    for model in response:
        assert model.severity == ErrorSeverity.CRITICAL

    response = make_request(ErrorSearchRequest(category=ErrorCategory.CONFIGURATION))

    for model in response:
        assert model.category == ErrorCategory.CONFIGURATION


def test_failed_error_search(test_server_with_many_files_and_errors, test_client):
    request = ErrorSearchRequest(id=-1)

    response = test_client.post_with_auth(
        "/api/v2/search/error",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
    )

    assert response.status_code == 404

    response = ErrorSearchFailedResponse.model_validate_json(response.content)

    request = ErrorSearchRequest(
        create_time_window=[datetime.datetime.min, datetime.datetime.min]
    )

    response = test_client.post_with_auth(
        "/api/v2/search/error",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
    )

    assert response.status_code == 404

    response = ErrorSearchFailedResponse.model_validate_json(response.content)
