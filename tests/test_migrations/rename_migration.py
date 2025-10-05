"""Migration that renames a collection."""


def mongration(migration):
    """Rename test_collection to renamed_collection."""
    phase = migration.phase("Rename collection")
    phase.from_collection("test_db", "test_collection")
    phase.rename_collection("renamed_collection")
