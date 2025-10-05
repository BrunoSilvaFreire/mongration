"""Migration that creates multiple indexes on a collection."""


def mongration(migration):
    """Create multiple indexes on a test collection."""
    # Assume collection already exists with data
    
    # Phase 1: Create simple index
    phase1 = migration.phase("Create name index")
    phase1.create_index({"name": 1}, "test_db", "test_collection")
    
    # Phase 2: Create compound index
    phase2 = migration.phase("Create compound index")
    phase2.wait_for_phase(phase1)
    phase2.create_index({"age": 1, "score": -1}, "test_db", "test_collection")
    
    # Phase 3: Create another index
    phase3 = migration.phase("Create email index")
    phase3.wait_for_phase(phase2)
    phase3.create_index({"email": 1}, "test_db", "test_collection")

