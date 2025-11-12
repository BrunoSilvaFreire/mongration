"""Migration that uses delete collection operation."""


def mongration(migration):
    """Delete a collection as part of migration."""
    from mongrations.operations.delete_collection_operation import DeleteCollectionOperation
    
    phase = migration.phase("Delete old collection")
    phase.from_collection("test_db", "collection_to_delete")
    phase._operation = DeleteCollectionOperation("test_db", "collection_to_delete")
