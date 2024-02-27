"""
Tests the CreateClone background service.
"""

from hera_librarian.models.admin import (
    AdminStoreManifestRequest,
    AdminStoreManifestResponse,
)


def test_create_local_clone_with_valid(
    test_client, test_server_with_valid_file, test_orm
):
    """
    Tests that we can create a local clone with a valid file.
    """

    from librarian_background.create_clone import CreateLocalClone

    # Get a store to check
    _, get_session, _ = test_server_with_valid_file

    with get_session() as session:
        stores = session.query(test_orm.StoreMetadata).all()

        from_store = [store.name for store in stores if store.ingestable][0]
        to_store = [store.name for store in stores if not store.ingestable][0]
        empty = [
            store.name
            for store in stores
            if store.store_manager.report_full_fraction == 0.0
        ][0]

    clone_task = CreateLocalClone(
        name="Local clone",
        clone_from=from_store,
        clone_to=[empty, to_store],
        age_in_days=1,
    )

    assert clone_task()

    clones = []

    with get_session() as session:
        instances = session.query(test_orm.Instance).all()

        for instance in instances:
            assert instance.store.name != empty

            if instance.store.name == to_store:
                clones.append(instance)

    assert len(clones) > 0

    # Generate the manifest
    response = test_client.post_with_auth(
        "/api/v2/admin/store_manifest",
        content=AdminStoreManifestRequest(store_name=to_store).model_dump_json(),
    )

    assert response.status_code == 200

    manifest = AdminStoreManifestResponse.model_validate_json(response.content)

    assert len(manifest.store_files) == len(clones)


def test_create_local_clone_with_invalid(
    test_client, test_server_with_invalid_file, test_orm
):
    """
    Tests that we can create a local clone with a valid file.
    """

    from librarian_background.create_clone import CreateLocalClone

    # Get a store to check
    _, get_session, _ = test_server_with_invalid_file

    with get_session() as session:
        stores = session.query(test_orm.StoreMetadata).all()

        from_store = [store.name for store in stores if store.ingestable][0]
        to_store = [store.name for store in stores if not store.ingestable][0]

    clone_task = CreateLocalClone(
        name="Local clone",
        clone_from=from_store,
        clone_to=to_store,
        age_in_days=1,
    )

    assert clone_task() == False


def test_create_local_clone_with_missing(
    test_client, test_server_with_missing_file, test_orm
):
    """
    Tests that we can create a local clone with a valid file.
    """

    from librarian_background.create_clone import CreateLocalClone

    # Get a store to check
    _, get_session, _ = test_server_with_missing_file

    with get_session() as session:
        stores = session.query(test_orm.StoreMetadata).all()

        from_store = [store.name for store in stores if store.ingestable][0]
        to_store = [store.name for store in stores if not store.ingestable][0]

    clone_task = CreateLocalClone(
        name="Local clone",
        clone_from=from_store,
        clone_to=to_store,
        age_in_days=1,
    )

    assert clone_task() == False
