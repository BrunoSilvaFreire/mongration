"""Migration that simulates destination write failure."""


def mongration(migration):
    """Migration designed to fail at destination write."""
    phase = migration.phase("Destination write failure")
    phase.from_collection("test_db", "test_collection")
    
    def create_invalid_doc(doc):
        """Create a document that will fail to write."""
        # Create a document with invalid BSON type (if possible)
        # or just create a problematic structure
        return {
            "_id": doc.get("_id"),
            "invalid_field": float('inf')  # Invalid in BSON
        }
    
    phase.use_python(create_invalid_doc)
    phase.into_collection("test_db", "destination_failure_output")
