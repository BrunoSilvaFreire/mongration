"""Migration with multiple dependent phases in a chain."""


def mongration(migration):
    """Multiple phases with dependencies forming a chain."""
    # Phase 1: Extract data matching criteria
    extract = migration.phase("Extract")
    extract.from_collection("test_db", "test_collection")
    extract.use_aggregation([{"$match": {"age": {"$gte": 30}}}])
    
    # Phase 2: Transform extracted data
    transform = migration.phase("Transform")
    transform.from_phase(extract)
    transform.use_python(lambda doc: {**doc, "category": "senior"})
    
    # Phase 3: Load transformed data
    load = migration.phase("Load")
    load.from_phase(transform)
    load.into_collection("test_db", "senior_users")
