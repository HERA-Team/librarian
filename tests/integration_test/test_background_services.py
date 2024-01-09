"""
Tests for background services.
"""


def test_check_integrity(test_client, test_server_with_file, test_orm):
    """
    Check the integrity of our test database.
    """

    from librarian_background.check_integrity import CheckIntegrity

    # Get a store to check
    _, session, _ = test_server_with_file
    store = session.query(test_orm.StoreMetadata).first()

    integrity_task = CheckIntegrity(
        name="Integrity check", store_name=store.name, age_in_days=1
    )
    integrity_task()
