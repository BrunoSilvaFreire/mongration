"""Migration that uses streaming aggregation for large datasets."""


def mongration(migration):
    """Use streaming aggregation to process data in batches."""
    phase = migration.phase("Stream large data")
    phase.from_collection("test_db", "large_collection")
    
    # Group by ranges of 100
    phase.use_stream([
        {"$group": {
            "_id": {"$floor": {"$divide": ["$index", 100]}},
            "count": {"$sum": 1},
            "total_value": {"$sum": "$value"}
        }}
    ], batch_size=100)
    phase.into_collection("test_db", "streamed_collection")
