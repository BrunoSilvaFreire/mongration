"""Migration that uses Python document transformation."""


def mongration(migration):
    """Transform documents using Python function."""
    phase = migration.phase("Transform with Python")
    phase.from_collection("test_db", "test_collection")
    
    # Add computed fields
    def transform(doc):
        doc["name_upper"] = doc["name"].upper()
        score = doc.get("score", 0)
        if score >= 90:
            doc["score_grade"] = "A"
        elif score >= 80:
            doc["score_grade"] = "B"
        else:
            doc["score_grade"] = "C"
        return doc
    
    phase.use_python(transform)
    phase.into_collection("test_db", "transformed_collection")
