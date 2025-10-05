"""Migration that converts string IDs to UUIDs."""


def mongration(migration):
    """Convert _id field from string to UUID."""
    migration.convert_to_uuid_phase(
        database="test_db",
        collection="uuid_test_collection",
        field="_id",
        keep_legacy=False
    )
