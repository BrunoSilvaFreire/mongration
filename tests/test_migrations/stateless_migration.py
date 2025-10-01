"""Stateless migration for testing."""


def mongration(migration):
    migration.mark_stateless()
    phase = migration.phase("Stateless operation")
    phase.use_generator(lambda doc: {"stateless": True})
    phase.into_collection("test_db", "test_collection")
