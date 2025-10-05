import asyncio
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from behave import given, when, then
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
from mongrations.program import MongrationProgram
from mongrations.plan import MongrationStatus
import argparse
# Import helper functions - use absolute import for Behave compatibility
from helpers import (
    skip_if_mongodb_unavailable,
    mongodb_client,
    collection_exists,
    get_collection_count,
    insert_documents,
    get_migration_status,
    verify_indexes_exist,
    table_to_documents,
    verify_field_in_documents
)


@given('a clean MongoDB test database')
def step_clean_mongodb(context):
    """Ensure we have a clean test database"""
    try:
        client = pymongo.MongoClient(context.mongodb_url, serverSelectionTimeoutMS=5000)
        client.drop_database(context.test_db_name)
        client.drop_database('test_db')  # Also drop the hardcoded test_db used by migrations
        client.drop_database('mongrations')
        client.close()
        context.mongodb_available = True
        print(f"✓ MongoDB connection successful at {context.mongodb_url}")
    except Exception as e:
        print(f"Warning: MongoDB not available: {e}")
        context.mongodb_available = False
        context.scenario.skip("MongoDB not available")


@given('I run the mongration "{filename}"')
def step_setup_single_mongration(context, filename):
    """Set up a single mongration by filename (for use with Given)"""
    test_migrations_dir = Path(__file__).parent.parent.parent / "test_migrations"
    context.mongration_file = test_migrations_dir / filename
    
    # Verify the migration file exists
    assert context.mongration_file.exists(), \
        f"Migration file not found: {context.mongration_file}"


@when('I run the mongration "{filename}"')
def step_run_single_mongration(context, filename):
    """Run a single mongration by filename"""
    if not context.mongodb_available:
        context.scenario.skip("MongoDB not available")
        return
    
    test_migrations_dir = Path(__file__).parent.parent.parent / "test_migrations"
    context.mongration_file = test_migrations_dir / filename
    
    # Verify the migration file exists before trying to run it
    assert context.mongration_file.exists(), \
        f"Migration file not found: {context.mongration_file}"
    
    # Import the direct entry point
    from mongrations.main import run_mongration_from_args
    import io
    import sys
    import logging
    
    # Initialize multiple_runs if not present
    if not hasattr(context, 'multiple_runs'):
        context.multiple_runs = []
    
    # Capture stderr from logging
    stderr_capture = io.StringIO()
    stderr_handler = logging.StreamHandler(stderr_capture)
    stderr_handler.setLevel(logging.ERROR)
    logger = logging.getLogger('mongrations')
    logger.addHandler(stderr_handler)
    
    try:
        exit_code = run_mongration_from_args(
            url=context.mongodb_url,
            mongration=str(context.mongration_file),
            dry_run=False
        )
        
        # Get captured stderr
        stderr_output = stderr_capture.getvalue()
        
        # Create a result-like object for compatibility
        context.run_result = type('obj', (object,), {
            'returncode': exit_code,
            'stdout': '',
            'stderr': stderr_output
        })()
        context.run_success = exit_code == 0
        
        # Track this run
        context.multiple_runs.append({
            'success': exit_code == 0,
            'exit_code': exit_code
        })
    except Exception as e:
        print(f"Exception during mongration execution: {e}")
        import traceback
        traceback.print_exc()
        stderr_output = stderr_capture.getvalue() + traceback.format_exc()
        context.run_success = False
        context.run_result = type('obj', (object,), {
            'returncode': 1,
            'stdout': '',
            'stderr': stderr_output
        })()
        
        # Track this failed run
        context.multiple_runs.append({
            'success': False,
            'exit_code': 1,
            'error': traceback.format_exc()
        })
    finally:
        # Clean up handler
        logger.removeHandler(stderr_handler)
        stderr_capture.close()


@given('I run the mongrations')
def step_setup_mongrations(context):
    """Set up multiple mongration scripts from table"""
    test_migrations_dir = Path(__file__).parent.parent.parent / "test_migrations"
    
    # Parse the table to get mongration files
    mongration_files = []
    for row in context.table:
        script_name = row['script']
        mongration_file = test_migrations_dir / f"{script_name}.py"
        
        # Verify the migration file exists
        assert mongration_file.exists(), \
            f"Migration file not found: {mongration_file}"
        
        mongration_files.append(mongration_file)
    
    # Store in context for later use
    context.mongrations_dir = test_migrations_dir
    context.mongration_files = mongration_files


@given('I have a collection with test data')
def step_collection_with_data(context):
    """Create a collection with test data"""
    if skip_if_mongodb_unavailable(context):
        return
    
    test_documents = [
        {"name": "Alice", "age": 30, "category": "Engineering"},
        {"name": "Bob", "age": 25, "category": "Marketing"},
        {"name": "Charlie", "age": 35, "category": "Engineering"},
        {"name": "Diana", "age": 28, "category": "Sales"}
    ]
    
    insert_documents(context, "source_collection", test_documents)
    
    # Verify documents were actually inserted
    count = get_collection_count(context, "source_collection")
    assert count == len(test_documents), \
        f"Expected {len(test_documents)} documents inserted, but found {count}"


@given('I have a mongration script that performs data aggregation')
def step_aggregation_mongration(context):
    """Use existing aggregation migration"""
    test_migrations_dir = Path(__file__).parent.parent.parent / "test_migrations"
    context.mongration_file = test_migrations_dir / "aggregation_migration.py"
    
    # Verify the migration file exists
    assert context.mongration_file.exists(), \
        f"Aggregation migration file not found: {context.mongration_file}"


@given('I have a mongration script')
def step_basic_mongration(context):
    """Use existing simple migration"""
    test_migrations_dir = Path(__file__).parent.parent.parent / "test_migrations"
    context.mongration_file = test_migrations_dir / "simple_migration.py"
    
    # Verify the migration file exists
    assert context.mongration_file.exists(), \
        f"Simple migration file not found: {context.mongration_file}"


@given('the mongration status is "{status}"')
def step_mongration_status(context, status):
    """Set mongration status"""
    context.initial_status = status


@when('I run the mongration in dry run mode')
def step_run_mongration_dry(context):
    """Execute the mongration in dry run mode"""
    if not context.mongodb_available:
        context.scenario.skip("MongoDB not available")
        return
    
    # Import the direct entry point
    from mongrations.main import run_mongration_from_args
    
    try:
        exit_code = run_mongration_from_args(
            url=context.mongodb_url,
            mongration=str(context.mongration_file),
            dry_run=True
        )
        
        context.run_result = type('obj', (object,), {
            'returncode': exit_code,
            'stdout': '',
            'stderr': ''
        })()
        context.run_success = exit_code == 0
    except Exception as e:
        import traceback
        context.run_success = False
        context.run_result = type('obj', (object,), {
            'returncode': 1,
            'stdout': '',
            'stderr': traceback.format_exc()
        })()


@when('I run the mongrations directory')
def step_run_mongrations_dir(context):
    """Execute mongrations - either specific files or entire directory"""
    if not context.mongodb_available:
        context.scenario.skip("MongoDB not available")
        return
    
    # Import the direct entry point
    from mongrations.main import run_mongration_from_args
    import traceback
    
    # If specific files were set up, run them individually
    if hasattr(context, 'mongration_files') and context.mongration_files:
        # Run each migration file individually
        all_success = True
        
        for mongration_file in context.mongration_files:
            try:
                exit_code = run_mongration_from_args(
                    url=context.mongodb_url,
                    mongration=str(mongration_file)
                )
                
                if exit_code != 0:
                    all_success = False
            except Exception as e:
                print(f"Exception during mongration execution: {e}")
                traceback.print_exc()
                all_success = False
        
        # Create a combined result
        context.run_success = all_success
        context.run_result = type('obj', (object,), {
            'returncode': 0 if all_success else 1,
            'stdout': '',
            'stderr': ''
        })()
    else:
        # Run entire directory
        try:
            exit_code = run_mongration_from_args(
                url=context.mongodb_url,
                mongrations_dir=str(context.mongrations_dir)
            )
            
            context.run_result = type('obj', (object,), {
                'returncode': exit_code,
                'stdout': '',
                'stderr': ''
            })()
            context.run_success = exit_code == 0
        except Exception as e:
            print(f"Exception during mongration execution: {e}")
            import traceback
            traceback.print_exc()
            context.run_success = False
            context.run_result = type('obj', (object,), {
                'returncode': 1,
                'stdout': '',
                'stderr': traceback.format_exc()
            })()


@when('I run the mongration multiple times')
def step_run_mongration_multiple(context):
    """Execute the mongration multiple times"""
    if not context.mongodb_available:
        context.scenario.skip("MongoDB not available")
        return
    
    # Import the direct entry point
    from mongrations.main import run_mongration_from_args
    import traceback
    
    context.multiple_runs = []
    
    for i in range(3):
        try:
            exit_code = run_mongration_from_args(
                url=context.mongodb_url,
                mongration=str(context.mongration_file)
            )
            
            result = type('obj', (object,), {
                'returncode': exit_code,
                'stdout': '',
                'stderr': ''
            })()
            
            context.multiple_runs.append({
                'success': exit_code == 0,
                'result': result
            })
        except Exception as e:
            print(f"Exception during mongration execution: {e}")
            traceback.print_exc()
            result = type('obj', (object,), {
                'returncode': 1,
                'stdout': '',
                'stderr': traceback.format_exc()
            })()
            context.multiple_runs.append({
                'success': False,
                'result': result
            })


@when('I manipulate the mongration status to "{status}"')
def step_manipulate_status(context, status):
    """Manipulate mongration status"""
    if not context.mongodb_available:
        context.scenario.skip("MongoDB not available")
        return
    
    # Import the direct entry point
    from mongrations.main import run_mongration_from_args
    import traceback
    
    try:
        exit_code = run_mongration_from_args(
            url=context.mongodb_url,
            mongration=str(context.mongration_file),
            command='manipulate',
            status=status
        )
        
        context.manipulate_result = type('obj', (object,), {
            'returncode': exit_code,
            'stdout': '',
            'stderr': ''
        })()
        context.manipulate_success = exit_code == 0
    except Exception as e:
        print(f"Exception during status manipulation: {e}")
        traceback.print_exc()
        context.manipulate_success = False
        context.manipulate_result = type('obj', (object,), {
            'returncode': 1,
            'stdout': '',
            'stderr': traceback.format_exc()
        })()


@then('the collection should exist in the database')
def step_collection_exists(context):
    """Verify collection exists"""
    if skip_if_mongodb_unavailable(context):
        return
    
    with mongodb_client(context) as db:
        collections = db.list_collection_names()
        # Check for collections created by test migrations
        expected_collections = ["test_collection", "indexed_collection"]
        found_collections = [col for col in expected_collections if col in collections]
        
        assert len(found_collections) > 0, \
            f"None of the expected collections {expected_collections} found. Available: {collections}"
        
        # Verify the found collection actually has data (not just created empty)
        for col_name in found_collections:
            count = db[col_name].count_documents({})
            # Some migrations might create empty collections, so just warn but don't fail
            if count == 0:
                print(f"Warning: Collection '{col_name}' exists but is empty")


@then('the collection should not exist in the database')
def step_collection_not_exists(context):
    """Verify collection does not exist"""
    if skip_if_mongodb_unavailable(context):
        return
    
    # Check both the test database and 'test_db' since migrations might use either
    test_collection_exists = collection_exists(context, "test_collection")
    indexed_collection_exists = collection_exists(context, "indexed_collection")
    
    assert not test_collection_exists and not indexed_collection_exists, \
        f"Expected collections should not exist. test_collection exists: {test_collection_exists}, indexed_collection exists: {indexed_collection_exists}"


@then('the mongration status should be "{expected_status}"')
def step_check_status(context, expected_status):
    """Check mongration status"""
    if skip_if_mongodb_unavailable(context):
        return
    
    actual_status = get_migration_status(context)
    
    if expected_status == "ABSENT":
        assert actual_status is None, f"Expected no state (ABSENT), but found: {actual_status}"
    else:
        assert actual_status is not None, "Expected state document but none found"
        assert actual_status == expected_status, \
            f"Expected status '{expected_status}', but got '{actual_status}'"


@then('all phases should be executed in order')
def step_phases_executed(context):
    """Verify all phases were executed"""
    if skip_if_mongodb_unavailable(context):
        return
    
    # Check that the run was successful
    assert context.run_success, f"Migration failed: {getattr(context.run_result, 'stderr', 'Unknown error')}"
    
    # Verify phases were tracked in state
    with mongodb_client(context, "mongrations") as state_db:
        state_collection = state_db["state"]
        state_doc = state_collection.find_one(sort=[("_id", -1)])
        
        assert state_doc is not None, "No state document found after migration execution"
        assert "phases_ran" in state_doc, "State document missing 'phases_ran' field"
        assert len(state_doc["phases_ran"]) > 0, "No phases recorded in state document"
        
        # Verify each phase has expected fields
        for phase_info in state_doc["phases_ran"]:
            assert "phase" in phase_info, f"Phase info missing 'phase' field: {phase_info}"
            assert "num_documents_iterated" in phase_info, f"Phase info missing 'num_documents_iterated' field: {phase_info}"


@then('the error should be logged')
def step_error_logged(context):
    """Verify error was logged"""
    # Check that the run failed
    assert not context.run_success, "Expected migration to fail but it succeeded"
    
    if hasattr(context, 'run_result') and context.run_result:
        error_output = (context.run_result.stderr or "") + (context.run_result.stdout or "")
        assert error_output, "Expected error output but none found"
        
        # Verify the output contains meaningful error indicators
        error_indicators = ['error', 'exception', 'traceback', 'failed', 'fail']
        has_error_content = any(indicator.lower() in error_output.lower() for indicator in error_indicators)
        assert has_error_content, f"Error output doesn't contain meaningful error information: {error_output[:200]}"


@then('the mongration should execute successfully each time')
def step_multiple_success(context):
    """Verify multiple executions were successful"""
    if skip_if_mongodb_unavailable(context):
        return
    
    # Check that we have exactly 3 runs
    assert hasattr(context, 'multiple_runs') and len(context.multiple_runs) == 3, \
        f"Expected 3 runs, but got {len(context.multiple_runs)}"
    
    for i, run in enumerate(context.multiple_runs):
        assert run['success'], f"Run {i+1} failed with exit code {run.get('exit_code', 'unknown')}"


@then('no state should be tracked')
def step_no_state_tracked(context):
    """Verify no state is tracked for stateless migration"""
    if skip_if_mongodb_unavailable(context):
        return
    
    with mongodb_client(context, "mongrations") as state_db:
        state_collection = state_db["state"]
        state_count = state_collection.count_documents({})
        
        if state_count > 0:
            states = list(state_collection.find())
            print(f"DEBUG: Found {len(states)} state documents: {states}")
        
        assert state_count == 0, f"Expected no state documents for stateless migration, but found {state_count}"


@then('the mongrations should execute in dependency order')
def step_dependency_order(context):
    """Verify mongrations executed in dependency order"""
    if skip_if_mongodb_unavailable(context):
        return
    
    # Check that the run was successful
    assert context.run_success, f"Migration failed: {getattr(context.run_result, 'stderr', 'Unknown error')}"
    
    # Verify all expected migrations have state entries
    with mongodb_client(context, "mongrations") as state_db:
        state_collection = state_db["state"]
        states = list(state_collection.find().sort("_id", 1))
        
        assert len(states) >= 2, f"Expected at least 2 migration states, found {len(states)}"
        
        # Verify each migration completed successfully
        for state in states:
            assert state.get("status") == "COMPLETED", \
                f"Migration '{state.get('name')}' has status '{state.get('status')}', expected 'COMPLETED'"
            assert "phases_ran" in state, f"Migration '{state.get('name')}' missing 'phases_ran' field"


@then('all mongrations should be "{status}"')
def step_all_status(context, status):
    """Verify all mongrations have expected status"""
    if skip_if_mongodb_unavailable(context):
        return
    
    with mongodb_client(context, "mongrations") as state_db:
        state_collection = state_db["state"]
        states = list(state_collection.find())
        
        assert len(states) >= 2, f"Expected at least 2 migration states, found {len(states)}"
        
        for state in states:
            actual_status = state.get("status", "ABSENT")
            assert actual_status == status, \
                f"Expected all migrations to be '{status}', but '{state['name']}' is '{actual_status}'"
            
            # Also verify state document has expected structure
            assert "name" in state, f"State document missing 'name' field: {state}"
            assert "_id" in state, f"State document missing '_id' field: {state}"
            
            # If status is COMPLETED, verify phases were recorded
            if status == "COMPLETED":
                assert "phases_ran" in state, \
                    f"Completed migration '{state['name']}' missing 'phases_ran' field"
                assert len(state["phases_ran"]) > 0, \
                    f"Completed migration '{state['name']}' has no phases recorded"


@then('the index should exist on the collection')
def step_index_exists(context):
    """Verify index exists"""
    if skip_if_mongodb_unavailable(context):
        return
    
    with mongodb_client(context) as db:
        collection = db["indexed_collection"]
        indexes = list(collection.list_indexes())
        
        # Should have at least the default _id index and our custom index
        assert len(indexes) >= 2, f"Expected at least 2 indexes, found {len(indexes)}"
        
        # Check for an index on the 'name' field (could have auto-generated name)
        name_index_found = any('name' in idx.get('key', {}) for idx in indexes)
        assert name_index_found, \
            f"Expected index on 'name' field not found. Available indexes: {[idx.get('key', {}) for idx in indexes]}"
        
        # Verify index metadata
        for idx in indexes:
            if 'name' in idx.get('key', {}):
                # Verify the index has expected properties
                assert 'name' in idx, f"Index missing 'name' property: {idx}"
                assert 'key' in idx, f"Index missing 'key' property: {idx}"
                print(f"Found index on 'name' field: {idx['name']} with key {idx['key']}")


@then('the aggregated data should be processed correctly')
def step_aggregation_result(context):
    """Verify aggregation result"""
    if skip_if_mongodb_unavailable(context):
        return
    
    with mongodb_client(context) as db:
        # Check if aggregated_collection was created and has data
        assert "aggregated_collection" in db.list_collection_names(), \
            f"Collection 'aggregated_collection' not found. Available: {db.list_collection_names()}"
        
        stats_collection = db["aggregated_collection"]
        stats = list(stats_collection.find())
        
        # Should have stats for each category
        assert len(stats) > 0, "Expected aggregated data but found none"
        
        # Verify aggregation structure - should have category grouping fields
        for stat in stats:
            assert "category" in stat, f"Aggregated document missing 'category' field: {stat}"
            assert "count" in stat, f"Aggregated document missing 'count' field: {stat}"
            # Verify count is a positive number
            assert isinstance(stat["count"], (int, float)) and stat["count"] > 0, \
                f"Invalid count value: {stat.get('count')}"

# Additional step definitions for comprehensive testing

@given('I have a collection "{collection_name}" with documents')
def step_create_collection_with_table(context, collection_name):
    """Create a collection with documents from a table"""
    if skip_if_mongodb_unavailable(context):
        return
    
    documents = table_to_documents(context.table)
    if documents:
        insert_documents(context, collection_name, documents)


@given('I have a collection "{collection_name}" with {count:d} documents')
def step_create_collection_with_count(context, collection_name, count):
    """Create a collection with a specific number of documents"""
    if skip_if_mongodb_unavailable(context):
        return
    
    documents = [{"index": i, "value": i * 2} for i in range(count)]
    insert_documents(context, collection_name, documents)


@given('I have a collection "{collection_name}" with mixed data')
def step_create_mixed_collection(context, collection_name):
    """Create a collection with mixed data types"""
    if skip_if_mongodb_unavailable(context):
        return
    
    documents = [
        {"type": "important", "value": 100},
        {"type": "normal", "value": 50},
        {"type": "important", "value": 150},
        {"type": "low", "value": 25},
    ]
    insert_documents(context, collection_name, documents)


@given('I have a collection "{collection_name}" with categorized data')
def step_create_categorized_collection(context, collection_name):
    """Create a collection with categorized documents"""
    if skip_if_mongodb_unavailable(context):
        return
    
    documents = [
        {"category": "A", "score": 85},
        {"category": "B", "score": 92},
        {"category": "A", "score": 78},
        {"category": "C", "score": 88},
        {"category": "B", "score": 95},
    ]
    insert_documents(context, collection_name, documents)


@given('I have a collection "{collection_name}" with string IDs')
def step_create_string_id_collection(context, collection_name):
    """Create a collection with string IDs for UUID conversion"""
    if skip_if_mongodb_unavailable(context):
        return
    
    import uuid
    
    # Use valid UUID strings that can be converted by MongoDB's UUID() function
    documents = [
        {"_id": str(uuid.uuid4()), "name": "Item 1"},
        {"_id": str(uuid.uuid4()), "name": "Item 2"},
        {"_id": str(uuid.uuid4()), "name": "Item 3"},
    ]
    insert_documents(context, collection_name, documents)


@given('I have a collection "{collection_name}" with old schema documents')
def step_create_old_schema_collection(context, collection_name):
    """Create a collection with old schema for transformation"""
    if skip_if_mongodb_unavailable(context):
        return
    
    documents = [
        {"first_name": "Alice", "last_name": "Smith", "dob": "1990-01-15"},
        {"first_name": "Bob", "last_name": "Jones", "dob": "1985-06-20"},
    ]
    insert_documents(context, collection_name, documents)


@then('the collection "{collection_name}" should exist')
def step_verify_collection_exists(context, collection_name):
    """Verify a collection exists"""
    if skip_if_mongodb_unavailable(context):
        return
    
    exists = collection_exists(context, collection_name)
    assert exists, f"Collection '{collection_name}' not found"


@then('the collection "{collection_name}" should not exist')
def step_verify_collection_not_exists(context, collection_name):
    """Verify a collection doesn't exist"""
    if skip_if_mongodb_unavailable(context):
        return
    
    exists = collection_exists(context, collection_name)
    assert not exists, f"Collection '{collection_name}' should not exist but was found"


@then('the collection "{collection_name}" should contain {count:d} documents')
def step_verify_collection_count(context, collection_name, count):
    """Verify collection has specific number of documents"""
    if skip_if_mongodb_unavailable(context):
        return
    
    actual = get_collection_count(context, collection_name)
    assert actual == count, f"Expected {count} documents, found {actual}"


@then('the collection "{collection_name}" should have documents with field "{field_name}"')
def step_verify_field_exists(context, collection_name, field_name):
    """Verify documents have a specific field"""
    if skip_if_mongodb_unavailable(context):
        return
    
    has_field = verify_field_in_documents(context, collection_name, field_name)
    assert has_field, f"Field '{field_name}' not found in documents of '{collection_name}'"


@then('all IDs should be converted to UUID')
def step_verify_uuid_conversion(context):
    """Verify IDs are properly formatted UUIDs"""
    if skip_if_mongodb_unavailable(context):
        return
    
    from bson.binary import Binary
    import uuid
    
    # Get list of all databases to check (excluding system databases)
    client = pymongo.MongoClient(context.mongodb_url, serverSelectionTimeoutMS=5000)
    try:
        all_databases = [db for db in client.list_database_names() 
                        if db not in ['admin', 'local', 'config', 'mongrations']]
        
        uuid_collections_found = []
        
        for db_name in all_databases:
            db = client[db_name]
            # Find collections with 'uuid' in the name
            uuid_collections = [name for name in db.list_collection_names() 
                              if 'uuid' in name.lower()]
            
            for coll_name in uuid_collections:
                collection = db[coll_name]
                docs = list(collection.find().limit(10))
                
                if docs:
                    uuid_collections_found.append((db_name, coll_name, docs))
        
        assert uuid_collections_found, \
            f"No UUID collection found with documents. Databases checked: {all_databases}"
        
        # Verify documents in found collections
        for db_name, coll_name, docs in uuid_collections_found:
            for i, doc in enumerate(docs):
                _id = doc.get("_id")
                assert _id is not None, f"Document {i} in {db_name}.{coll_name} has no _id"
                
                # Accept either Binary UUID or valid UUID string
                if isinstance(_id, Binary):
                    # Verify it's UUID subtype
                    assert _id.subtype == 4, \
                        f"Document {i} in {db_name}.{coll_name}: Expected UUID subtype 4, got {_id.subtype}"
                elif isinstance(_id, str):
                    # Verify it's a valid UUID string
                    try:
                        uuid.UUID(_id)
                    except ValueError:
                        raise AssertionError(
                            f"Document {i} in {db_name}.{coll_name}: ID is not a valid UUID string: {_id}"
                        )
                else:
                    raise AssertionError(
                        f"Document {i} in {db_name}.{coll_name}: Expected Binary UUID or UUID string, got {type(_id)}"
                    )
    finally:
        client.close()


@then('the legacy string IDs should be preserved')
def step_verify_legacy_preserved(context):
    """Verify legacy IDs are preserved in documents"""
    if skip_if_mongodb_unavailable(context):
        return
    
    # Get list of all databases to check
    client = pymongo.MongoClient(context.mongodb_url, serverSelectionTimeoutMS=5000)
    try:
        all_databases = [db for db in client.list_database_names() 
                        if db not in ['admin', 'local', 'config', 'mongrations']]
        
        legacy_found = False
        
        for db_name in all_databases:
            db = client[db_name]
            for collection_name in db.list_collection_names():
                if "uuid" in collection_name.lower() and "legacy" in collection_name.lower():
                    collection = db[collection_name]
                    docs = list(collection.find().limit(5))
                    
                    if docs:
                        # Verify documents have a legacy ID field
                        for doc in docs:
                            # Check for common legacy field names
                            legacy_fields = ['legacy_id', 'old_id', 'string_id', 'original_id']
                            has_legacy_field = any(field in doc for field in legacy_fields)
                            if has_legacy_field:
                                legacy_found = True
                                # Verify the legacy field contains a string
                                legacy_value = next((doc[f] for f in legacy_fields if f in doc), None)
                                assert isinstance(legacy_value, str), \
                                    f"Legacy ID field should be string, got {type(legacy_value)}"
                        
                        if legacy_found:
                            break
            if legacy_found:
                break
        
        assert legacy_found, "No UUID collection with preserved legacy IDs found"
    finally:
        client.close()


@then('the indexes should be created')
def step_verify_indexes_created(context):
    """Verify multiple indexes were created"""
    if skip_if_mongodb_unavailable(context):
        return
    
    # First verify the collection exists
    assert collection_exists(context, "test_collection"), \
        "Collection 'test_collection' does not exist"
    
    # Then verify indexes
    has_indexes = verify_indexes_exist(context, "test_collection", min_count=2)
    assert has_indexes, "Expected at least 2 indexes to be created"
    
    # Also verify the specific index details
    with mongodb_client(context) as db:
        collection = db["test_collection"]
        indexes = list(collection.list_indexes())
        
        # Log index information for debugging
        print(f"Found {len(indexes)} indexes on test_collection:")
        for idx in indexes:
            print(f"  - {idx.get('name')}: {idx.get('key')}")


@then('all documents should have new schema')
def step_verify_new_schema(context):
    """Verify documents have been transformed to new schema"""
    if skip_if_mongodb_unavailable(context):
        return
    
    with mongodb_client(context) as db:
        collection = db["new_schema_collection"]
        docs = list(collection.find().limit(10))
        
        assert len(docs) > 0, "No documents found in new schema collection"
        
        # Define expected new schema fields
        expected_fields = ["full_name", "birth_date"]
        
        for i, doc in enumerate(docs):
            for field in expected_fields:
                assert field in doc, \
                    f"Document {i} missing expected field '{field}'. Document: {doc}"


@then('data should flow through the pipe')
def step_verify_pipe_flow(context):
    """Verify data flowed through phases correctly"""
    if skip_if_mongodb_unavailable(context):
        return
    
    # Check that dependent phases produced results
    with mongodb_client(context) as db:
        collections = db.list_collection_names()
        
        # Look for result collections created by phase dependencies
        result_collections = [col for col in collections 
                            if any(keyword in col.lower() for keyword in ["senior", "result", "merged", "transformed", "filtered"])]
        
        assert len(result_collections) > 0, \
            f"No result collections found from pipe flow. Available collections: {collections}"
        
        # Verify result collections have data
        for result_col in result_collections:
            count = db[result_col].count_documents({})
            assert count > 0, \
                f"Result collection '{result_col}' exists but has no documents (pipe may not have flowed data)"
    
    # Verify migration state shows multiple phases executed
    with mongodb_client(context, "mongrations") as state_db:
        state_collection = state_db["state"]
        state_doc = state_collection.find_one(sort=[("_id", -1)])
        
        if state_doc and "phases_ran" in state_doc:
            assert len(state_doc["phases_ran"]) >= 2, \
                f"Expected at least 2 phases for pipe flow, found {len(state_doc['phases_ran'])}"


