"""Migration that creates a collection with an index."""


def mongration(migration):
    """Create a collection and add an index on the name field."""
    
    # Phase 1: Create collection with test data
    create_phase = migration.phase("Create collection")
    
    def generate_documents(doc):
        return {"name": "Test Document", "value": 123, "category": "test"}
    
    create_phase.use_generator(generate_documents)
    create_phase.into_collection("test_db", "indexed_collection")
    
    # Phase 2: Create index on the collection
    index_phase = migration.phase("Create index")
    index_phase.wait_for_phase(create_phase)
    index_phase.create_index(
        index={"name": 1},
        database="test_db",
        collection="indexed_collection"
    )
