"""Migration that performs data aggregation."""


def mongration(migration):
    """Aggregate test data from source_collection and compute statistics by category."""
    
    phase = migration.phase("Aggregate test data")
    
    # Aggregate data from source_collection (created by test setup)
    # Group by category and compute average age
    phase.from_aggregation(
        database="test_db",
        collection="source_collection",
        aggregation=[
            {
                "$group": {
                    "_id": "$category",
                    "category": {"$first": "$category"},
                    "count": {"$sum": 1},
                    "average_age": {"$avg": "$age"},
                    "total_age": {"$sum": "$age"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "category": 1,
                    "count": 1,
                    "average_age": 1,
                    "total_age": 1
                }
            }
        ]
    )
    
    # Set up a pass-through operation to process the aggregation results
    phase.use_python(lambda doc: doc)
    
    phase.into_collection("test_db", "aggregated_collection")
