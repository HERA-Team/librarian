"""
Tests for background services.
"""


def test_check_integrity(test_client, test_server_with_valid_file, test_orm):
    """
    Check the integrity of our test database.
    """

    from librarian_background.check_integrity import CheckIntegrity

    # Get a store to check
    _, session, _ = test_server_with_valid_file
    store = session.query(test_orm.StoreMetadata).first()

    integrity_task = CheckIntegrity(
        name="Integrity check", store_name=store.name, age_in_days=1
    )
    assert integrity_task()


def test_check_integrity_failure(test_client, test_server_with_invalid_file, test_orm):
    """
    Check the integrity of our test database when we have a bad file.
    """

    from librarian_background.check_integrity import CheckIntegrity

    # Get a store to check
    _, session, _ = test_server_with_invalid_file
    store = session.query(test_orm.StoreMetadata).first()

    integrity_task = CheckIntegrity(
        name="Integrity check", store_name=store.name, age_in_days=1
    )
    assert integrity_task() == False


def test_check_integrity_invalid_store(test_client, test_server, test_orm):
    """
    Check we get a CancelJob when we don't have a valid server.
    """

    from librarian_background.check_integrity import CheckIntegrity
    from schedule import CancelJob

    integrity_task = CheckIntegrity(
        name="Integrity check", store_name="invalid_store", age_in_days=1
    )
    assert integrity_task() == CancelJob


def test_check_integrity_missing_store(test_client, test_server_with_missing_file, test_orm):
    """
    Check we get a CancelJob when we don't have a valid server.
    """

    from librarian_background.check_integrity import CheckIntegrity

    # Get a store to check
    _, session, _ = test_server_with_missing_file
    store = session.query(test_orm.StoreMetadata).first()

    integrity_task = CheckIntegrity(
        name="Integrity check", store_name=store.name, age_in_days=1
    )
    assert integrity_task() == False