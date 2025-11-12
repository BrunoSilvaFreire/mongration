"""Migration that imports data from a file."""
import json


async def json_file_iterator(file_path):
    """Async iterator for reading JSON documents from a file."""
    with open(file_path, 'r') as f:
        data = json.load(f)
        if isinstance(data, list):
            for doc in data:
                yield doc
        else:
            yield data


def mongration(migration):
    """Import documents from JSON file."""
    import tempfile
    import os
    
    def identity_transform(doc):
        """Pass through documents unchanged."""
        return doc
    
    phase = migration.phase("Import from file")
    
    # Use a predefined test file path
    import_path = os.path.join(tempfile.gettempdir(), "mongrations_import_test.json")
    phase.import_from(import_path, identity_transform, json_file_iterator)
    phase.into_collection("test_db", "imported_collection")

