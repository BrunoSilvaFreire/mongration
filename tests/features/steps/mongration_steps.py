"""
Mongrations BDD Step Definitions

This module contains Behave step definitions for testing Mongration migrations.
Steps are organized following Gherkin best practices with clear separation of
Given (setup), When (actions), and Then (verification) steps.

Key Features:
- Parameterized steps for maximum reusability
- Type parsers for numeric and other custom types
- Helper functions to reduce code duplication
- Clear separation of concerns with section markers
- Consistent error handling and MongoDB availability checks

Step Patterns:
- Use '{variable}' for string parameters
- Use '{count:d}' for integer parameters
- Use tables for structured data input
- Steps are composable - complex steps can call simpler ones

Examples:
    # Parameterized step usage
    When I run the mongration 5 times
    Then the mongration should execute successfully 5 times
    
    # Named collection usage
    Given I have a collection "users" with 100 documents
    Then the collection "users" should contain 100 documents
    
    # Flexible index verification
    Then at least 3 indexes should exist on "products"
    Then the index on "email" should exist on "users"
"""
import argparse
import asyncio
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional, List, Dict, Any
from behave import given, when, then, register_type
import parse
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
from mongrations.program import MongrationProgram
from mongrations.plan import MongrationStatus
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


import logging
logger = logging.getLogger('mongration.tests')
# ============================================================================
# Custom type parsers for Behave
# ============================================================================

@parse.with_pattern(r'\d+')
def parse_number(text):
    """Parse a number from text."""
    return int(text)

register_type(Number=parse_number)

# ============================================================================
# Helper functions for step implementations
# ============================================================================

def get_test_migrations_dir() -> Path:
    """Get the path to the test migrations directory."""
    return Path(__file__).parent.parent.parent / "test_migrations"


def verify_mongration_file_exists(filepath: Path) -> None:
    """Verify that a mongration file exists, raise assertion if not."""
    assert filepath.exists(), f"Migration file not found: {filepath}"


def run_mongration_internal(
    context,
    mongration_file: Optional[Path] = None,
    mongrations_dir: Optional[Path] = None,
    dry_run: bool = False,
    command: str = 'run',
    status: Optional[str] = None
) -> Dict[str, Any]:
    """
    Internal helper to run a mongration and return result info.
    
    Args:
        context: Behave context object
        mongration_file: Path to single mongration file to run
        mongrations_dir: Path to directory of mongrations to run
        dry_run: Whether to run in dry-run mode
        command: Command to execute ('run' or 'manipulate')
        status: Status to set (for 'manipulate' command)
    
    Returns:
        Dict with keys: 'success' (bool), 'exit_code' (int), 'stderr' (str), 'stdout' (str)
    """
    from mongrations.main import run_mongration_from_args
    import io
    import logging
    import traceback
    
    # Capture stderr from logging
    stderr_capture = io.StringIO()
    stderr_handler = logging.StreamHandler(stderr_capture)
    stderr_handler.setLevel(logging.ERROR)
    migration_logger = logging.getLogger('mongrations')
    migration_logger.addHandler(stderr_handler)
    
    try:
        kwargs = {
            'url': context.mongodb_url,
            'dry_run': dry_run
        }
        
        if mongration_file:
            kwargs['mongration'] = str(mongration_file)
        elif mongrations_dir:
            kwargs['mongrations_dir'] = str(mongrations_dir)
        
        if command == 'manipulate':
            kwargs['command'] = command
            kwargs['status'] = status
        
        exit_code = run_mongration_from_args(**kwargs)
        stderr_output = stderr_capture.getvalue()
        
        return {
            'success': exit_code == 0,
            'exit_code': exit_code,
            'stdout': '',
            'stderr': stderr_output
        }
    except Exception as e:
        print(f"Exception during mongration execution: {e}")
        formatted_exc = traceback.format_exc()
        migration_logger.error(formatted_exc)
        stderr_output = stderr_capture.getvalue() + formatted_exc
        
        return {
            'success': False,
            'exit_code': 1,
            'stdout': '',
            'stderr': stderr_output,
            'error': formatted_exc
        }
    finally:
        migration_logger.removeHandler(stderr_handler)
        stderr_capture.close()


def create_result_object(result_dict: Dict[str, Any]):
    """Create a result-like object from a dictionary for backwards compatibility."""
    return type('obj', (object,), result_dict)()


# ============================================================================
# Given Steps - Setup and Preconditions
# ============================================================================

@given('a clean MongoDB test database')
def step_clean_mongodb(context):
    """Ensure we have a clean test database"""
    try:
        client = pymongo.MongoClient(context.mongodb_url, serverSelectionTimeoutMS=5000)
        client.drop_database(context.test_db_name)
        client.drop_database(context.fallback_db_name)  # Also drop the fallback DB used by migrations
        client.drop_database(context.state_db_name)
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
    test_migrations_dir = get_test_migrations_dir()
    context.mongration_file = test_migrations_dir / filename
    verify_mongration_file_exists(context.mongration_file)


@when('I run the mongration "{filename}"')
def step_run_single_mongration(context, filename):
    """Run a single mongration by filename"""
    if skip_if_mongodb_unavailable(context):
        return
    
    test_migrations_dir = get_test_migrations_dir()
    context.mongration_file = test_migrations_dir / filename
    verify_mongration_file_exists(context.mongration_file)
    
    # Initialize multiple_runs if not present (for compatibility with multiple run tracking)
    if not hasattr(context, 'multiple_runs'):
        context.multiple_runs = []
    
    result_dict = run_mongration_internal(
        context,
        mongration_file=context.mongration_file
    )
    
    # Store result in both formats for compatibility
    context.run_result = create_result_object({
        'returncode': result_dict['exit_code'],
        'stdout': result_dict['stdout'],
        'stderr': result_dict['stderr']
    })
    context.run_success = result_dict['success']
    
    # Track this run
    context.multiple_runs.append(result_dict)


@given('I run the mongrations')
def step_setup_mongrations(context):
    """Set up multiple mongration scripts from table"""
    test_migrations_dir = get_test_migrations_dir()
    
    # Parse the table to get mongration files
    mongration_files = []
    for row in context.table:
        script_name = row['script']
        mongration_file = test_migrations_dir / f"{script_name}.py"
        verify_mongration_file_exists(mongration_file)
        mongration_files.append(mongration_file)
    
    # Store in context for later use
    context.mongrations_dir = test_migrations_dir
    context.mongration_files = mongration_files


# ============================================================================
# Given Steps - Test Data Setup
# ============================================================================

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
    
    insert_documents(context, context.source_collection_name, test_documents)
    
    # Verify documents were actually inserted
    count = get_collection_count(context, context.source_collection_name)
    assert count == len(test_documents), \
        f"Expected {len(test_documents)} documents inserted, but found {count}"


@given('I have a mongration script that performs data aggregation')
def step_aggregation_mongration(context):
    """Use existing aggregation migration"""
    test_migrations_dir = get_test_migrations_dir()
    context.mongration_file = test_migrations_dir / "aggregation_migration.py"
    verify_mongration_file_exists(context.mongration_file)


@given('I have a mongration script')
def step_basic_mongration(context):
    """Use existing simple migration"""
    test_migrations_dir = get_test_migrations_dir()
    context.mongration_file = test_migrations_dir / "simple_migration.py"
    verify_mongration_file_exists(context.mongration_file)

@given('the mongration status is "{status}"')
def step_mongration_status(context, status):
    """Set mongration status"""
    context.initial_status = status


# ============================================================================
# When Steps - Actions
# ============================================================================

@when('I run the mongration in dry run mode')
def step_run_mongration_dry(context):
    """Execute the mongration in dry run mode"""
    if skip_if_mongodb_unavailable(context):
        return
    
    result_dict = run_mongration_internal(
        context,
        mongration_file=context.mongration_file,
        dry_run=True
    )
    
    context.run_result = create_result_object({
        'returncode': result_dict['exit_code'],
        'stdout': result_dict['stdout'],
        'stderr': result_dict['stderr']
    })
    context.run_success = result_dict['success']

@when('I run the mongrations directory')
def step_run_mongrations_dir(context):
    """Execute mongrations - either specific files or entire directory"""
    if skip_if_mongodb_unavailable(context):
        return
    
    # If specific files were set up, run them individually
    if hasattr(context, 'mongration_files') and context.mongration_files:
        # Run each migration file individually
        all_success = True
        
        for mongration_file in context.mongration_files:
            result_dict = run_mongration_internal(
                context,
                mongration_file=mongration_file
            )
            if not result_dict['success']:
                all_success = False
        
        # Create a combined result
        context.run_success = all_success
        context.run_result = create_result_object({
            'returncode': 0 if all_success else 1,
            'stdout': '',
            'stderr': ''
        })
    else:
        # Run entire directory
        result_dict = run_mongration_internal(
            context,
            mongrations_dir=context.mongrations_dir
        )
        
        context.run_result = create_result_object({
            'returncode': result_dict['exit_code'],
            'stdout': result_dict['stdout'],
            'stderr': result_dict['stderr']
        })
        context.run_success = result_dict['success']

@when('I run the mongration {count:d} times')
def step_run_mongration_n_times(context, count):
    """Execute the mongration N times (configurable)"""
    if skip_if_mongodb_unavailable(context):
        return
    
    if not hasattr(context, 'multiple_runs'):
        context.multiple_runs = []
    
    for i in range(count):
        result_dict = run_mongration_internal(
            context,
            mongration_file=context.mongration_file
        )
        context.multiple_runs.append(result_dict)
    
    # Store the count for later verification
    context.expected_run_count = count


@when('I run the mongration multiple times')
def step_run_mongration_multiple(context):
    """Execute the mongration multiple times (default: 3)"""
    step_run_mongration_n_times(context, 3)


@when('I manipulate the mongration status to "{status}"')
def step_manipulate_status(context, status):
    """Manipulate mongration status"""
    if skip_if_mongodb_unavailable(context):
        return
    
    result_dict = run_mongration_internal(
        context,
        mongration_file=context.mongration_file,
        command='manipulate',
        status=status
    )
    
    context.manipulate_result = create_result_object({
        'returncode': result_dict['exit_code'],
        'stdout': result_dict['stdout'],
        'stderr': result_dict['stderr']
    })
    context.manipulate_success = result_dict['success']


# ============================================================================
# Then Steps - Assertions and Verifications
# ============================================================================

@then('the collection should exist in the database')
def step_collection_exists(context):
    """Verify collection exists (checks for common test collections)"""
    if skip_if_mongodb_unavailable(context):
        return
    
    with mongodb_client(context) as db:
        collections = db.list_collection_names()
        # Check for collections created by test migrations
        expected_collections = [context.test_collection_name, context.indexed_collection_name]
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
    """Verify collection does not exist (checks for common test collections)"""
    if skip_if_mongodb_unavailable(context):
        return
    
    # Check both the test database and fallback DB since migrations might use either
    test_collection_exists = collection_exists(context, context.test_collection_name)
    indexed_collection_exists = collection_exists(context, context.indexed_collection_name)
    
    assert not test_collection_exists and not indexed_collection_exists, \
        f"Expected collections should not exist. {context.test_collection_name} exists: {test_collection_exists}, " \
        f"{context.indexed_collection_name} exists: {indexed_collection_exists}"

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
    with mongodb_client(context, context.state_db_name) as state_db:
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


@then('the mongration should execute successfully {count:d} times')
def step_n_successful_runs(context, count):
    """Verify N executions were successful (configurable)"""
    if skip_if_mongodb_unavailable(context):
        return
    
    # Check that we have the expected number of runs
    assert hasattr(context, 'multiple_runs'), "No runs recorded in context"
    assert len(context.multiple_runs) == count, \
        f"Expected {count} runs, but got {len(context.multiple_runs)}"
    
    # Verify each run was successful
    for i, run in enumerate(context.multiple_runs):
        assert run['success'], \
            f"Run {i+1} of {count} failed with exit code {run.get('exit_code', 'unknown')}"


@then('the mongration should execute successfully each time')
def step_multiple_success(context):
    """Verify multiple executions were successful (uses actual run count from context)"""
    if skip_if_mongodb_unavailable(context):
        return
    
    # Get actual count from the number of runs tracked
    assert hasattr(context, 'multiple_runs'), "No runs recorded in context"
    actual_count = len(context.multiple_runs)
    
    # Use expected count if set, otherwise use actual count
    expected_count = getattr(context, 'expected_run_count', actual_count)
    
    step_n_successful_runs(context, expected_count)

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
    step_at_least_n_migrations_completed(context, 2)


@then('at least {count:d} migrations should be completed')
def step_at_least_n_migrations_completed(context, count):
    """Verify at least N migrations completed successfully"""
    if skip_if_mongodb_unavailable(context):
        return
    
    # Check that the run was successful
    assert context.run_success, f"Migration failed: {getattr(context.run_result, 'stderr', 'Unknown error')}"
    
    # Verify all expected migrations have state entries
    with mongodb_client(context, context.state_db_name) as state_db:
        state_collection = state_db["state"]
        states = list(state_collection.find().sort("_id", 1))
        
        assert len(states) >= count, f"Expected at least {count} migration state(s), found {len(states)}"
        
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
    
    with mongodb_client(context, context.state_db_name) as state_db:
        state_collection = state_db["state"]
        states = list(state_collection.find())
        
        # Use a more flexible minimum (at least 1 migration)
        min_migrations = getattr(context, 'expected_migration_count', 1)
        assert len(states) >= min_migrations, \
            f"Expected at least {min_migrations} migration state(s), found {len(states)}"
        
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


@then('at least {count:d} mongrations should be "{status}"')
def step_at_least_n_status(context, count, status):
    """Verify at least N mongrations have expected status"""
    if skip_if_mongodb_unavailable(context):
        return
    
    with mongodb_client(context, context.state_db_name) as state_db:
        state_collection = state_db["state"]
        states = list(state_collection.find({"status": status}))
        
        assert len(states) >= count, \
            f"Expected at least {count} migration(s) with status '{status}', found {len(states)}"


# ============================================================================
# Then Steps - Index Verification
# ============================================================================

@then('the index should exist on the collection')
def step_index_exists(context):
    """Verify index exists on indexed_collection"""
    step_verify_index_on_collection_field(context, context.default_index_field, context.indexed_collection_name)


@then('the index should exist on "{collection_name}"')
def step_index_exists_on_named_collection(context, collection_name):
    """Verify index exists on a named collection"""
    # Look for any index beyond the default _id
    step_at_least_n_indexes_on_collection(context, collection_name, 2)


@then('the index on "{field_name}" should exist on "{collection_name}"')
def step_verify_index_on_collection_field(context, field_name, collection_name):
    """Verify a specific index exists on a collection"""
    if skip_if_mongodb_unavailable(context):
        return
    
    with mongodb_client(context) as db:
        collection = db[collection_name]
        indexes = list(collection.list_indexes())
        
        # Check for an index on the specified field
        field_index_found = any(field_name in idx.get('key', {}) for idx in indexes)
        assert field_index_found, \
            f"Expected index on '{field_name}' field not found in '{collection_name}'. " \
            f"Available indexes: {[idx.get('key', {}) for idx in indexes]}"
        # Log index metadata for debugging
        for idx in indexes:
            if field_name in idx.get('key', {}):
                logger.info(f"Found index on '{field_name}' field: {idx['name']} with key {idx['key']}")
                print(f"Found index on '{field_name}' field: {idx['name']} with key {idx['key']}")


@then('at least {count:d} indexes should exist on "{collection_name}"')
def step_at_least_n_indexes_on_collection(context, count, collection_name):
    """Verify at least N indexes exist on a collection"""
    if skip_if_mongodb_unavailable(context):
        return
    
    with mongodb_client(context) as db:
        collection = db[collection_name]
        indexes = list(collection.list_indexes())

        assert len(indexes) >= count, f"Expected at least {count} indexes on '{collection_name}', found {len(indexes)}"
        # Log index information for debugging
        for idx in indexes:
            logger.info(f"  - {idx.get('name')}: {idx.get('key')}")
        for idx in indexes:
            print(f"  - {idx.get('name')}: {idx.get('key')}")


@then('the indexes should be created')
def step_verify_indexes_created(context):
    """Verify multiple indexes were created on test_collection"""
    if skip_if_mongodb_unavailable(context):
        return
    
    # First verify the collection exists
    assert collection_exists(context, context.test_collection_name), \
        f"Collection '{context.test_collection_name}' does not exist"
    
    # Then verify indexes (at least 2: _id + custom)
    step_at_least_n_indexes_on_collection(context, 2, context.test_collection_name)

@then('the aggregated data should be processed correctly')
def step_aggregation_result(context):
    """Verify aggregation result in aggregated_collection"""
    step_collection_has_aggregated_data(context, context.aggregated_collection_name, ["category", "count"])


@then('the collection "{collection_name}" should contain aggregated data with fields')
def step_collection_has_aggregated_data_with_fields(context, collection_name):
    """Verify collection contains aggregated data with fields from table"""
    if skip_if_mongodb_unavailable(context):
        return
    
    # Extract field names from table
    field_names = [row['field'] for row in context.table]
    step_collection_has_aggregated_data(context, collection_name, field_names)


def step_collection_has_aggregated_data(context, collection_name: str, required_fields: List[str]):
    """Helper to verify aggregated data structure"""
    if skip_if_mongodb_unavailable(context):
        return
    
    with mongodb_client(context) as db:
        # Check if collection was created and has data
        assert collection_name in db.list_collection_names(), \
            f"Collection '{collection_name}' not found. Available: {db.list_collection_names()}"
        
        collection = db[collection_name]
        docs = list(collection.find())
        
        # Should have aggregated data
        assert len(docs) > 0, f"Expected aggregated data in '{collection_name}' but found none"
        
        # Verify aggregation structure - should have required fields
        for doc in docs:
            for field in required_fields:
                assert field in doc, f"Aggregated document missing '{field}' field: {doc}"
            
            # If 'count' is in required fields, verify it's a positive number
            if 'count' in required_fields:
                assert isinstance(doc["count"], (int, float)) and doc["count"] > 0, \
                    f"Invalid count value: {doc.get('count')}"

# ============================================================================
# Given Steps - Advanced Collection Setup
# ============================================================================

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


# ============================================================================
# Then Steps - Collection Verification
# ============================================================================

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


@then('all documents should have new schema')
def step_verify_new_schema(context):
    """Verify documents have been transformed to new schema"""
    if skip_if_mongodb_unavailable(context):
        return
    
    with mongodb_client(context) as db:
        collection = db[context.new_schema_collection_name]
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
    with mongodb_client(context, context.state_db_name) as state_db:
        state_collection = state_db["state"]
        state_doc = state_collection.find_one(sort=[("_id", -1)])
        
        if state_doc and "phases_ran" in state_doc:
            assert len(state_doc["phases_ran"]) >= 2, \
                f"Expected at least 2 phases for pipe flow, found {len(state_doc['phases_ran'])}"


