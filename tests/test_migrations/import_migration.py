"""Migration that imports data from a file."""


def mongration(migration):
    """Import documents from JSON file."""
    import tempfile
    import os
    
    phase = migration.phase("Import from file")
    
    # Use a predefined test file path
    import_path = os.path.join(tempfile.gettempdir(), "mongrations_import_test.json")
    phase.import_from(import_path)
    phase.into_collection("test_db", "imported_collection")
