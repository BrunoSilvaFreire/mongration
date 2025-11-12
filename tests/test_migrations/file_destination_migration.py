"""Migration that writes data to a file destination."""


def mongration(migration):
    """Transform and write collection data to a file."""
    import tempfile
    import os
    
    phase = migration.phase("Transform and write to file")
    phase.from_collection("test_db", "file_dest_collection")
    
    def format_document(doc):
        """Format document for file output."""
        return {
            'name': doc.get('name', '').upper(),
            'processed': True,
            'original_value': doc.get('value', 0)
        }
    
    phase.use_python(format_document)
    
    # Write to file destination
    file_path = os.path.join(tempfile.gettempdir(), "mongrations_file_dest_output.jsonl")
    phase.into_file(file_path, mode='w', encoding='utf-8', batch_size=10)
