"""Migration with failing dependency."""


def mongration(migration):
    """Migration where one phase fails, blocking dependent phases."""
    
    # First phase that will fail
    failing_phase = migration.phase("Failing Phase")
    failing_phase.from_collection("test_db", "test_collection")
    
    def failing_transform(doc):
        """Transform that raises an error."""
        raise ValueError("Intentional failure in dependency")
    
    failing_phase.use_python(failing_transform)
    failing_phase.into_collection("test_db", "failed_output")
    
    # Dependent phase that should not execute
    dependent_phase = migration.phase("Dependent Phase")
    dependent_phase.from_phase(failing_phase)
    dependent_phase.use_python(lambda doc: {**doc, "processed": True})
    dependent_phase.into_collection("test_db", "dependent_output")
