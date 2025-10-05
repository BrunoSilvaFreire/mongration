"""Migration that uses generator operation to create documents."""


def mongration(migration):
    """Generate documents from scratch without a source."""
    phase = migration.phase("Generate documents")
    
    def generate_docs(doc):
        for i in range(10):
            yield {"item_id": i, "value": i * 10, "category": "generated"}
    
    phase.use_generator(generate_docs)
    phase.into_collection("test_db", "generated_collection")
