"""Migration that transforms document schema."""


def mongration(migration):
    """Transform old schema to new schema."""
    phase = migration.phase("Schema transformation")
    phase.from_collection("test_db", "old_schema_collection")
    
    def transform_schema(doc):
        # Old schema: {first_name, last_name, dob}
        # New schema: {name: {first, last}, birth_date}
        return {
            "_id": doc["_id"],
            "name": {
                "first": doc.get("first_name", ""),
                "last": doc.get("last_name", "")
            },
            "birth_date": doc.get("dob"),
            "migrated": True
        }
    
    phase.use_python(transform_schema)
    phase.into_collection("test_db", "new_schema_collection")
