"""Migration that imports from a non-existent file."""
import json


async def json_file_iterator(file_path):
    """Async iterator for reading JSON documents from a file."""
    with open(file_path, 'r') as f:  # This will raise FileNotFoundError
        data = json.load(f)
        if isinstance(data, list):
            for doc in data:
                yield doc
        else:
            yield data


def mongration(migration):
    """Import from a file that doesn't exist."""
    phase = migration.phase("Import from missing file")
    
    def identity_transform(doc):
        return doc
    
    # File path that doesn't exist
    missing_file = "/tmp/this_file_does_not_exist.json"
    phase.import_from(missing_file, identity_transform, json_file_iterator)
    phase.into_collection("test_db", "missing_import_output")
