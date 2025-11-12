"""Migration that exports collection data to a file."""


def mongration(migration):
    """Export collection data to JSON file."""
    import tempfile
    import os
    
    phase = migration.phase("Export to file")
    phase.from_collection("test_db", "export_test_collection")
    
    # Use identity transform to pass documents through
    phase.use_python(lambda doc: doc)
    
    # Write to file destination
    export_path = os.path.join(tempfile.gettempdir(), "mongrations_export_test.json")
    phase.into_file(export_path, mode='w', encoding='utf-8')

