"""Migration that attempts to read from non-existent collection."""


def mongration(migration):
    """Attempt to process a collection that doesn't exist."""
    phase = migration.phase("Process missing source")
    phase.from_collection("test_db", "non_existent_collection")
    phase.use_python(lambda doc: {**doc, "processed": True})
    phase.into_collection("test_db", "output_collection")
