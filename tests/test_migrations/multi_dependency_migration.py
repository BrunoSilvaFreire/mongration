"""Migration with phase having multiple dependencies."""


def mongration(migration):
    """Migration where one phase depends on multiple source phases."""
    
    # Source phase A
    phase_a = migration.phase("Process Source A")
    phase_a.from_collection("test_db", "source_a")
    phase_a.use_python(lambda doc: {**doc, "from_a": True})
    
    # Source phase B  
    phase_b = migration.phase("Process Source B")
    phase_b.from_collection("test_db", "source_b")
    phase_b.use_python(lambda doc: {**doc, "from_b": True})
    
    # Phase that depends on A - will wait for A to complete first
    merge_a_phase = migration.phase("Process A Results")
    merge_a_phase.from_phase(phase_a)
    merge_a_phase.use_python(lambda doc: {**doc, "processed_a": True})
    merge_a_phase.into_collection("test_db", "multi_dep_result")
    
    # Phase that depends on B - will wait for B to complete first
    merge_b_phase = migration.phase("Process B Results")
    merge_b_phase.from_phase(phase_b)
    merge_b_phase.use_python(lambda doc: {**doc, "processed_b": True})
    merge_b_phase.into_collection("test_db", "multi_dep_result_b")

