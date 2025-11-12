"""Migration with a long dependency chain."""


def mongration(migration):
    """Migration with multiple sequential phases forming a long chain."""
    
    # Phase 1: Extract
    phase1 = migration.phase("Extract Data")
    phase1.from_collection("test_db", "chain_source")
    phase1.use_python(lambda doc: {**doc, "step": 1})
    
    # Phase 2: Transform (depends on phase 1)
    phase2 = migration.phase("First Transform")
    phase2.from_phase(phase1)
    phase2.use_python(lambda doc: {**doc, "step": 2, "transformed": True})
    
    # Phase 3: Enrich (depends on phase 2)
    phase3 = migration.phase("Enrich Data")
    phase3.from_phase(phase2)
    phase3.use_python(lambda doc: {**doc, "step": 3, "enriched": True})
    
    # Phase 4: Filter (depends on phase 3)
    phase4 = migration.phase("Filter Data")
    phase4.from_phase(phase3)
    phase4.use_python(lambda doc: {**doc, "step": 4} if doc.get("enriched") else None)
    
    # Phase 5: Load (depends on phase 4)
    phase5 = migration.phase("Load Data")
    phase5.from_phase(phase4)
    phase5.use_python(lambda doc: {**doc, "step": 5, "final": True})
    phase5.into_collection("test_db", "chain_result")
