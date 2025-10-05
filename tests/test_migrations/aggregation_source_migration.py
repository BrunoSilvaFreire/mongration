"""Migration that uses aggregation as source."""


def mongration(migration):
    """Use aggregation source to pre-process data."""
    phase = migration.phase("Aggregation source")
    phase.from_aggregation("test_db", "test_collection", [
        {"$group": {
            "_id": "$category",
            "count": {"$sum": 1},
            "total_score": {"$sum": "$score"}
        }}
    ])
    phase.use_python(lambda doc: doc)
    phase.into_collection("test_db", "aggregated_source")
