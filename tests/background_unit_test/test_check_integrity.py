"""
Tests the CheckIntegrity background task.
"""


def test_check_integrity(test_client, test_server_with_valid_file, test_orm):
    """
    Check the integrity of our test database.
    """

    from librarian_background.check_integrity import CheckIntegrity

    # Get a store to check
    _, get_session, _ = test_server_with_valid_file

    with get_session() as session:
        store = session.query(test_orm.StoreMetadata).first().name

    integrity_task = CheckIntegrity(
        name="Integrity check", store_name=store, age_in_days=1
    )

    assert integrity_task()


def test_check_integrity_failure(test_client, test_server_with_invalid_file, test_orm):
    """
    Check the integrity of our test database when we have a bad file.
    """

    from librarian_background.check_integrity import CheckIntegrity

    # Get a store to check
    _, get_session, _ = test_server_with_invalid_file

    with get_session() as session:
        store = session.query(test_orm.StoreMetadata).first().name

    integrity_task = CheckIntegrity(
        name="Integrity check", store_name=store, age_in_days=1
    )
    assert integrity_task() == False

    # Go check the database!
    with get_session() as session:
        corrupt_file = session.query(test_orm.CorruptFile).first()
        assert corrupt_file is not None
        assert corrupt_file.count >= 1


def test_check_integrity_invalid_store(test_client, test_server, test_orm):
    """
    Check we get a CancelJob when we don't have a valid server.
    """

    from schedule import CancelJob

    from librarian_background.check_integrity import CheckIntegrity

    integrity_task = CheckIntegrity(
        name="Integrity check", store_name="invalid_store", age_in_days=1
    )
    assert integrity_task() == CancelJob


def test_check_integrity_missing_store(
    test_client, test_server_with_missing_file, test_orm
):
    """
    Check we get a CancelJob when we don't have a valid server.
    """

    from librarian_background.check_integrity import CheckIntegrity

    # Get a store to check
    _, get_session, _ = test_server_with_missing_file

    with get_session() as session:
        store = session.query(test_orm.StoreMetadata).first().name

    integrity_task = CheckIntegrity(
        name="Integrity check", store_name=store, age_in_days=1
    )
    assert integrity_task() == False
