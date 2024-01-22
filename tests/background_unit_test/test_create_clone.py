"""
Tests the CreateClone background service.
"""


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

    clone_task = CreateLocalClone(
        name="Local clone",
        clone_from=from_store,
        clone_to=to_store,
        age_in_days=1,
    )

    assert clone_task()


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
