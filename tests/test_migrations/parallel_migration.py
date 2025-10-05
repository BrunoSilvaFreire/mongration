"""Migration with parallel independent phases."""


def mongration(migration):
    """Two independent phases that can run in parallel."""
    # Phase 1: Process source 1
    phase1 = migration.phase("Process Source 1")
    phase1.from_collection("test_db", "source1")
    phase1.use_aggregation([{"$match": {}}])
    phase1.into_collection("test_db", "result1")
    
    # Phase 2: Process source 2 (independent of phase 1)
    phase2 = migration.phase("Process Source 2")
    phase2.from_collection("test_db", "source2")
    phase2.use_aggregation([{"$match": {}}])
    phase2.into_collection("test_db", "result2")
