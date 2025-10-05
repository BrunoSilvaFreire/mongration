"""Migration that converts IDs to UUIDs while keeping legacy field."""


def mongration(migration):
    """Convert _id field from string to UUID, keeping old value."""
    migration.convert_to_uuid_phase(
        database="test_db",
        collection="uuid_legacy_collection",
        field="_id",
        keep_legacy=True
    )
