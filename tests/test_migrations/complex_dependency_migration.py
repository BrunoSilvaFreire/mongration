"""Migration with complex dependency tree (diamond pattern)."""


def mongration(migration):
    """Complex dependency chain with branching and merging."""
    # Root phase: Extract all data
    root = migration.phase("Root")
    root.from_collection("test_db", "source")
    root.use_aggregation([{"$match": {}}])
    root.into_collection("test_db", "root_result")
    
    # Branch 1: Tag with branch 1
    branch1 = migration.phase("Branch 1")
    branch1.from_phase(root)
    branch1.use_python(lambda doc: {**doc, "branch": 1})
    branch1.into_collection("test_db", "branch1_result")
    
    # Branch 2: Tag with branch 2
    branch2 = migration.phase("Branch 2")
    branch2.from_phase(root)
    branch2.use_python(lambda doc: {**doc, "branch": 2})
    branch2.into_collection("test_db", "branch2_result")
    
    # Merge: Combine from branch1 only (simpler than true merge)
    merge = migration.phase("Merge")
    merge.from_phase(branch1)
    merge.use_python(lambda doc: {**doc, "merged": True})
    merge.into_collection("test_db", "merged")
