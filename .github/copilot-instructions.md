# Mongrations - AI Agent Instructions

## Project Overview
Mongrations is a Python framework for managing MongoDB schema migrations with a phase-based, dependency-aware execution model. It uses Motor (async MongoDB driver) and executes migrations as directed acyclic graphs (DAGs) where phases can run concurrently when dependencies allow.

## Architecture: The Big Picture

### Core Flow: Mongration → Phases → Graph → Engine
1. **Mongration Script** (`tests/test_migrations/*.py`): User-defined Python functions that create a `Mongration` object and define phases
2. **Phase Building** (`mongrations/phase.py`): Each phase specifies a Source → Operation → Destination pipeline
3. **Graph Construction** (`mongrations/graph.py`, `mongrations/loading.py`): Phases are converted to a dependency graph for parallel execution
4. **Async Execution** (`mongrations/engine/asyncio_engine.py`): Graph traversal executes phases concurrently using `asyncio.gather()`
5. **State Tracking** (`mongrations/plan.py`): Migration status (ABSENT/WORK_IN_PROGRESS/COMPLETED/FAILED) persists in `mongrations.state` collection

### Key Components

**Source-Operation-Destination Pattern** (mongrations/io/, mongrations/operations/):
- Sources: `CollectionSource`, `AggregationSource`, `FileSource`, phase-to-phase via `Pipe`
- Operations: `AggregationOperation`, `StreamingAggregationOperation`, `DocumentPythonOperation`, `GeneratorPythonOperation`, `IndexOperation`
- Destinations: `CollectionDestination`, `FileDestination`, `Pipe` (for phase chaining)

**Phase Dependencies** (`Phase.from_phase()`):
- Use `phase1.from_phase(phase2)` to create dependencies - phase1 waits for phase2's output
- Dependencies automatically create `Pipe` destinations connecting phases
- Graph execution ensures dependent phases wait for their sources to complete

**Stateless vs Stateful Migrations**:
- Stateful (default): Tracked in DB, run once, status persists
- Stateless (`migration.mark_stateless()`): No state tracking, runs every time

## Development Workflow

### Essential Commands (Taskfile)
```bash
task setup          # Create venv, install Poetry deps (always run first)
task test           # Run Behave BDD tests with auto MongoDB
task test:debug     # Debug tests with debugpy on port 5678
task clean          # Remove test artifacts and MongoDB container
task mongodb:up     # Manually start test MongoDB (usually not needed)
```

**Critical**: Always use `task` commands, not direct Poetry/pytest. The Taskfile manages PYTHONPATH and venv activation.

### Test Infrastructure (tests/)
- **BDD Tests**: Uses Behave (not pytest) - see `tests/features/mongrations.feature`
- **MongoDB Fixture** (`tests/fixtures/mongodb_fixture.py`): Auto-starts Docker Compose MongoDB on first test run
- **Test Migrations** (`tests/test_migrations/`): Example migrations used by tests (simple_migration.py, aggregation_migration.py, etc.)
- **Exit Code 201**: Common test failure - check `tests/features/environment.py` logging setup and MongoDB container status
### Writing tests
- All "mongration" scripts must be placed in `tests/test_migrations/`
- Procedurally generated mongrations are forbidden. All invoked mongrations must be static files.
- Adding multiple steps for specific mongrations is forbidden, for example, all of the following steps are not allowed:
    - When I run a migration creating multiple indexes
    - When I run a migration creating compound index
    - When I run a migration creating unique index
    - When I run a migration creating background index
- Instead, use the `I run the mongration "{mongration_file_path_with_extension}"` step.
- Always run the tests by invoking `task test`. Do not run `behave` or `pytest` directly.
### Debugging Tests
1. Set breakpoint in code
2. Run `task test:debug` (waits for debugger on 0.0.0.0:5678)
3. Use VS Code "Python: Attach" debug config or the available task

## Migration Script Patterns

### Basic Migration Structure
```python
def mongration(migration):
    # Required function name - receives Mongration object
    phase = migration.phase("Phase Name")
    
    # Pattern 1: Collection → Aggregation → Collection
    phase.from_collection("db", "source_col")
    phase.use_aggregation([{"$match": {...}}, {"$group": {...}}])
    phase.into_collection("db", "dest_col")
    
    # Pattern 2: Generator → Collection (no source needed)
    phase.use_generator(lambda doc: [{"field": "value"}])
    phase.into_collection("db", "col")
    
    # Pattern 3: Collection → Python Transform → Collection
    phase.from_collection("db", "col")
    phase.use_python(lambda doc: {**doc, "new_field": "value"})
    phase.into_collection("db", "col2")
```

### Phase Dependencies
```python
phase1 = migration.phase("Extract")
phase1.from_collection("db", "source")
phase1.use_aggregation([...])

phase2 = migration.phase("Transform")
phase2.from_phase(phase1)  # Reads phase1's output via Pipe
phase2.use_python(lambda doc: transform(doc))
phase2.into_collection("db", "dest")
```

### Built-in Helpers
- `migration.convert_to_uuid_phase(db, col, field)`: Complex multi-phase UUID conversion (see `mongrations/mongration.py:36-109`)
- `phase.create_index({"field": 1}, db, col)`: Index creation operation
- `phase.use_stream(sub_agg, batch_size)`: Streaming aggregation for large datasets

## Project Conventions

### Logging
- Every module uses `logger = logging.getLogger(__name__)`
- Configure in test setup: `logging.getLogger('mongrations').setLevel(logging.DEBUG)`
- Available loggers: `mongrations.program`, `mongrations.phase`, `mongrations.engine.*`, `mongrations.operations.*`

### Naming
- Migration scripts: `{descriptive_name}_migration.py` with `mongration(migration)` function
- Phase names: Descriptive strings like "Convert UUID", "Aggregate data"
- Phase sanitized names: Replace spaces with hyphens for IDs

### State Management
- State stored in `mongrations` database, `state` collection
- Document structure: `{_id: index, name: migration_name, status: "COMPLETED", phases_ran: [{phase, num_documents_iterated}]}`
- Use `mongrate manipulate` command to force-set migration status (useful for fixing broken states)

## CLI Usage
```bash
# Run single migration
mongrate run --url mongodb://localhost:27017 --mongration path/to/migration.py

# Run all migrations in directory
mongrate run --url mongodb://localhost:27017 --mongrations-dir path/to/migrations/

# Dry run (doesn't change DB or save state)
mongrate run --dry-run --mongration migration.py

# Force set migration status
mongrate manipulate --url mongodb://... --mongration migration.py --status COMPLETED
```

## Common Pitfalls

1. **Auto-configuration**: If phase has single dependency, calling `use_*()` auto-configures the pipe. Multi-dependency phases need manual configuration.
2. **Generator vs Python operation**: `use_generator()` creates docs from nothing; `use_python()` transforms input docs
3. **Phase execution order**: Graph determines order via dependencies, not definition order
4. **Async context**: All operations run in async context - use `await` in custom operations
5. **Test isolation**: `tests/features/environment.py` drops test databases before each scenario
6. **PYTHONPATH**: Taskfile sets this - don't run commands directly without proper PYTHONPATH

## When Adding Features

- **New Operation Type**: Inherit from `Operation` (mongrations/operations/operation.py), implement `invoke()` and `accepts_dependency_output()`
- **New Source/Destination**: Implement Source/Destination protocols (mongrations/io/source.py, mongrations/io/destination.py)
- **Test Migration**: Add to `tests/test_migrations/`, add BDD scenario to `mongrations.feature`, implement steps in `tests/features/steps/`
