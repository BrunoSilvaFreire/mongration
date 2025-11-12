"""Simple migration for CLI testing - creates a third test collection."""


def mongration(migration):
    """Create a simple test collection."""
    phase = migration.phase("Create CLI test collection 3")
    
    # Generate a single document
    phase.use_generator(lambda doc: [{"name": "CLI Test 3", "value": 300}])
    
    # Write to collection
    phase.into_collection("test_db", "cli_test_collection_3")
