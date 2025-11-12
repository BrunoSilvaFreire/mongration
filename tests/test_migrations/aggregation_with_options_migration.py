"""Migration with aggregation options."""


def mongration(migration):
    """Aggregation with custom options."""
    phase = migration.phase("Aggregation with options")
    
    phase.from_collection("test_db", "test_collection")
    phase.use_aggregation(
        [
            {"$match": {"value": {"$gt": 50}}},
            {"$group": {"_id": None, "total": {"$sum": "$value"}, "count": {"$sum": 1}}}
        ],
        options={"allowDiskUse": True, "maxTimeMS": 5000}
    )
    
    phase.into_collection("test_db", "aggregation_options_result")

