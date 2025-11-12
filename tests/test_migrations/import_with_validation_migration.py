"""Migration that imports data with validation."""
import json


async def json_file_iterator(file_path):
    """Async iterator for JSON documents."""
    with open(file_path, 'r') as f:
        docs = json.load(f)
        for doc in docs:
            yield doc


def mongration(migration):
    """Import documents from JSON file with validation."""
    import tempfile
    import os
    
    phase = migration.phase("Import with validation")
    
    # Validation function
    def validate_and_transform(doc):
        """Validate document has required fields."""
        if 'name' not in doc:
            raise ValueError("Document missing 'name' field")
        if 'value' not in doc:
            raise ValueError("Document missing 'value' field")
        
        # Add validation metadata
        return {
            **doc,
            'validated': True,
            'validation_timestamp': 'test_timestamp'
        }
    
    import_path = os.path.join(tempfile.gettempdir(), "mongrations_validated_import.json")
    phase.import_from(import_path, validate_and_transform, json_file_iterator)
    phase.into_collection("test_db", "validated_collection")

