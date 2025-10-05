"""Migration that exports collection data to a file."""


def mongration(migration):
    """Export collection data to JSON file."""
    import tempfile
    import os
    
    phase = migration.phase("Export to file")
    phase.from_collection("test_db", "test_collection")
    
    # Create temp file for export
    export_path = os.path.join(tempfile.gettempdir(), "mongrations_export_test.json")
    phase.export_to(export_path)
