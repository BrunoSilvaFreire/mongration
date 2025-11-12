"""Simple migration for CLI testing - creates another test collection."""


def mongration(migration):
    """Create a simple test collection."""
    phase = migration.phase("Create CLI test collection 2")
    
    # Generate a single document
    phase.use_generator(lambda doc: [{"name": "CLI Test 2", "value": 200}])
    
    # Write to collection
    phase.into_collection("test_db", "cli_test_collection_2")
