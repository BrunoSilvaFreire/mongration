"""Migration that fails during transformation."""


def mongration(migration):
    """Migration with transformation that raises an error."""
    phase = migration.phase("Transformation with error")
    phase.from_collection("test_db", "error_collection")
    
    def problematic_transform(doc):
        """Transformation that fails on certain documents."""
        # This will fail if 'required_field' is missing
        value = doc['required_field']  # KeyError if missing
        return {**doc, "transformed": value * 2}
    
    phase.use_python(problematic_transform)
    phase.into_collection("test_db", "error_output")
