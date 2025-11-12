"""Migration with multiple graph roots."""


def mongration(migration):
    """Migration with multiple independent starting points."""
    
    # Root 1: Process collection A
    phase_a = migration.phase("Process Collection A")
    phase_a.from_collection("test_db", "source_a")
    phase_a.use_python(lambda doc: {**doc, "source": "a"})
    phase_a.into_collection("test_db", "result_a")
    
    # Root 2: Process collection B (independent)
    phase_b = migration.phase("Process Collection B")
    phase_b.from_collection("test_db", "source_b")
    phase_b.use_python(lambda doc: {**doc, "source": "b"})
    phase_b.into_collection("test_db", "result_b")
    
    # Root 3: Process collection C (independent)
    phase_c = migration.phase("Process Collection C")
    phase_c.from_collection("test_db", "source_c")
    phase_c.use_python(lambda doc: {**doc, "source": "c"})
    phase_c.into_collection("test_db", "result_c")
    
    # Dependent phase: Merge all results
    merge_phase = migration.phase("Merge Results")
    merge_phase.from_phase(phase_a)
    # Note: Multiple dependencies would need special handling
    merge_phase.use_python(lambda doc: {**doc, "merged": True})
    merge_phase.into_collection("test_db", "merged_results")
