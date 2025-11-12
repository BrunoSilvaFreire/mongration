"""Migration that exports data with transformation."""


def mongration(migration):
    """Export collection data to JSON file with transformation."""
    import tempfile
    import os
    
    def export_transform(doc):
        """Transform document before export."""
        return {
            'name': doc['name'].upper(),
            'score': doc['score'],
            'grade': 'A' if doc['score'] >= 90 else 'B' if doc['score'] >= 80 else 'C'
        }
    
    phase = migration.phase("Export with transformation")
    phase.from_collection("test_db", "transform_export_collection")
    phase.use_python(export_transform)
    
    # Write to file destination
    export_path = os.path.join(tempfile.gettempdir(), "mongrations_export_transform_test.json")
    phase.into_file(export_path, mode='w', encoding='utf-8')

