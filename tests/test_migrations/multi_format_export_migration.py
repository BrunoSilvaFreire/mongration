"""Migration that exports to multiple formats."""


def mongration(migration):
    """Export collection data to multiple file formats."""
    import tempfile
    import os
    
    # Export to JSON format
    json_phase = migration.phase("Export to JSON")
    json_phase.from_collection("test_db", "multi_export_collection")
    json_phase.use_python(lambda doc: doc)  # Identity transform
    json_path = os.path.join(tempfile.gettempdir(), "mongrations_export_multi.json")
    json_phase.into_file(json_path, mode='w', encoding='utf-8')
    
    # Export to CSV-like format (as text lines)
    def export_csv(doc):
        """Export as CSV format."""
        return f"{doc.get('name', '')},{doc.get('data', '')}"
    
    csv_phase = migration.phase("Export to CSV")
    csv_phase.from_collection("test_db", "multi_export_collection")
    csv_phase.use_python(export_csv)
    csv_path = os.path.join(tempfile.gettempdir(), "mongrations_export_multi.csv")
    csv_phase.into_file(csv_path, mode='w', encoding='utf-8')


