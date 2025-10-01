"""Simple migration that creates a test collection."""


def mongration(migration):
    """Create a simple test collection with some documents."""
    phase = migration.phase("Create test collection")
    
    # Generate some test documents
    def generate_documents(doc):
        return [
            {"name": "Test Document 1", "value": 100},
            {"name": "Test Document 2", "value": 200},
            {"name": "Test Document 3", "value": 300},
        ]
    
    phase.use_generator(lambda doc: generate_documents(doc)[0])
    phase.into_collection("test_db", "test_collection")
