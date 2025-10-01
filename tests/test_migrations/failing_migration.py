"""Migration that will fail."""


def mongration(migration):
    phase = migration.phase("Failing operation")
    # Use a generator that will fail during iteration
    def failing_generator(doc):
        raise ZeroDivisionError("Intentional failure for testing")
        
    phase.use_generator(failing_generator)
    phase.into_collection("test_db", "failing_collection")
