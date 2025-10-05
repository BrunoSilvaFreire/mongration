"""Migration that uses collection source with filter."""


def mongration(migration):
    """Filter source collection to only process matching documents."""
    phase = migration.phase("Filtered collection source")
    phase.from_collection("test_db", "mixed_collection", filter={"type": "important"})
    phase.use_aggregation([{"$match": {}}])
    phase.into_collection("test_db", "filtered_collection")
