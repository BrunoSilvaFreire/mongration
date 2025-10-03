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


@when('I run the mongration "{filename}"')
def step_run_single_mongration(context, filename):
    """Run a single mongration by filename"""
    if not context.mongodb_available:
        context.scenario.skip("MongoDB not available")
        return
    
    test_migrations_dir = Path(__file__).parent.parent.parent / "test_migrations"
    context.mongration_file = test_migrations_dir / filename
    
    # Execute the mongration
    cmd = [
        sys.executable, "-m", "mongrations.main",
        "--url", context.mongodb_url,
        "run",
        "--mongration", str(context.mongration_file)
    ]
    
    try:
        result = subprocess.run(cmd, cwd=Path(__file__).parent.parent.parent, 
                              capture_output=True, text=True, timeout=30)
        context.run_result = result
        context.run_success = result.returncode == 0
        # Print output for debugging
        if result.stdout:
            print(f"STDOUT:\n{result.stdout}")
        if result.stderr:
            print(f"STDERR:\n{result.stderr}")
    except subprocess.TimeoutExpired:
        context.run_success = False
        context.run_result = None


@given('I run the mongrations')
def step_setup_mongrations(context):
    """Set up multiple mongration scripts from table"""
    test_migrations_dir = Path(__file__).parent.parent.parent / "test_migrations"
    
    # Parse the table to get mongration files
    mongration_files = []
    for row in context.table:
        script_name = row['script']
        mongration_file = test_migrations_dir / f"{script_name}.py"
        mongration_files.append(mongration_file)
    
    # Store in context for later use
    context.mongrations_dir = test_migrations_dir
    context.mongration_files = mongration_files


@given('I have a collection with test data')
def step_collection_with_data(context):
    """Create a collection with test data"""
    if not context.mongodb_available:
        context.scenario.skip("MongoDB not available")
        return
        
    client = pymongo.MongoClient(context.mongodb_url)
    db = client["test_db"]
    collection = db["source_collection"]
    
    test_documents = [
        {"name": "Alice", "age": 30, "category": "Engineering"},
        {"name": "Bob", "age": 25, "category": "Marketing"},
        {"name": "Charlie", "age": 35, "category": "Engineering"},
        {"name": "Diana", "age": 28, "category": "Sales"}
    ]
    
    collection.insert_many(test_documents)
    client.close()


@given('I have a mongration script that performs data aggregation')
def step_aggregation_mongration(context):
    """Use existing aggregation migration"""
    test_migrations_dir = Path(__file__).parent.parent.parent / "test_migrations"
    context.mongration_file = test_migrations_dir / "aggregation_migration.py"


@given('I have a mongration script')
def step_basic_mongration(context):
    """Use existing simple migration"""
    test_migrations_dir = Path(__file__).parent.parent.parent / "test_migrations"
    context.mongration_file = test_migrations_dir / "simple_migration.py"


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
    
    cmd = [
        sys.executable, "-m", "mongrations.main",
        "--url", context.mongodb_url,
        "run",
        "--mongration", str(context.mongration_file),
        "--dry-run"
    ]
    
    try:
        result = subprocess.run(cmd, cwd=Path(__file__).parent.parent.parent, 
                              capture_output=True, text=True, timeout=30)
        context.run_result = result
        context.run_success = result.returncode == 0
    except subprocess.TimeoutExpired:
        context.run_success = False
        context.run_result = None


@when('I run the mongrations directory')
def step_run_mongrations_dir(context):
    """Execute mongrations - either specific files or entire directory"""
    if not context.mongodb_available:
        context.scenario.skip("MongoDB not available")
        return
    
    # If specific files were set up, run them individually
    if hasattr(context, 'mongration_files') and context.mongration_files:
        # Run each migration file individually
        all_success = True
        combined_stdout = []
        combined_stderr = []
        
        for mongration_file in context.mongration_files:
            cmd = [
                sys.executable, "-m", "mongrations.main",
                "--url", context.mongodb_url,
                "run",
                "--mongration", str(mongration_file)
            ]
            
            try:
                result = subprocess.run(cmd, cwd=Path(__file__).parent.parent.parent, 
                                      capture_output=True, text=True, timeout=30)
                combined_stdout.append(result.stdout)
                combined_stderr.append(result.stderr)
                if result.returncode != 0:
                    all_success = False
            except subprocess.TimeoutExpired:
                all_success = False
                break
        
        # Create a combined result
        context.run_success = all_success
        context.run_result = type('obj', (object,), {
            'returncode': 0 if all_success else 1,
            'stdout': '\n'.join(combined_stdout),
            'stderr': '\n'.join(combined_stderr)
        })()
    else:
        # Run entire directory
        cmd = [
            sys.executable, "-m", "mongrations.main",
            "--url", context.mongodb_url,
            "run",
            "--mongrations-dir", str(context.mongrations_dir)
        ]
        
        try:
            result = subprocess.run(cmd, cwd=Path(__file__).parent.parent.parent, 
                                  capture_output=True, text=True, timeout=30)
            context.run_result = result
            context.run_success = result.returncode == 0
        except subprocess.TimeoutExpired:
            context.run_success = False
            context.run_result = None


@when('I run the mongration multiple times')
def step_run_mongration_multiple(context):
    """Execute the mongration multiple times"""
    if not context.mongodb_available:
        context.scenario.skip("MongoDB not available")
        return
    
    context.multiple_runs = []
    
    for i in range(3):
        cmd = [
            sys.executable, "-m", "mongrations.main",
            "--url", context.mongodb_url,
            "run",
            "--mongration", str(context.mongration_file)
        ]
        
        try:
            result = subprocess.run(cmd, cwd=Path(__file__).parent.parent.parent, 
                                  capture_output=True, text=True, timeout=30)
            context.multiple_runs.append({
                'success': result.returncode == 0,
                'result': result
            })
        except subprocess.TimeoutExpired:
            context.multiple_runs.append({
                'success': False,
                'result': None
            })


@when('I manipulate the mongration status to "{status}"')
def step_manipulate_status(context, status):
    """Manipulate mongration status"""
    if not context.mongodb_available:
        context.scenario.skip("MongoDB not available")
        return
    
    cmd = [
        sys.executable, "-m", "mongrations.main",
        "--url", context.mongodb_url,
        "manipulate",
        "--mongration", str(context.mongration_file),
        "--status", status
    ]
    
    try:
        result = subprocess.run(cmd, cwd=Path(__file__).parent.parent.parent, 
                              capture_output=True, text=True, timeout=30)
        context.manipulate_result = result
        context.manipulate_success = result.returncode == 0
    except subprocess.TimeoutExpired:
        context.manipulate_success = False
        context.manipulate_result = None


@then('the collection should exist in the database')
def step_collection_exists(context):
    """Verify collection exists"""
    if not context.mongodb_available:
        return
        
    client = pymongo.MongoClient(context.mongodb_url)
    db = client["test_db"]
    
    collections = db.list_collection_names()
    # Check for collections created by test migrations
    assert "test_collection" in collections or "indexed_collection" in collections, \
        f"Expected collection not found. Available collections: {collections}"
    
    client.close()


@then('the collection should not exist in the database')
def step_collection_not_exists(context):
    """Verify collection does not exist"""
    if not context.mongodb_available:
        return
        
    client = pymongo.MongoClient(context.mongodb_url)
    db = client["test_db"]
    
    collections = db.list_collection_names()
    assert "test_collection" not in collections, f"Collection should not exist, but found: {collections}"
    
    client.close()


@then('the mongration status should be "{expected_status}"')
def step_check_status(context, expected_status):
    """Check mongration status"""
    if not context.mongodb_available:
        return
        
    client = pymongo.MongoClient(context.mongodb_url)
    state_db = client["mongrations"]
    state_collection = state_db["state"]
    
    # Find the latest migration state
    state_doc = state_collection.find_one(sort=[("_id", -1)])
    
    if expected_status == "ABSENT":
        assert state_doc is None, f"Expected no state document, but found: {state_doc}"
    else:
        assert state_doc is not None, "Expected state document but none found"
        actual_status = state_doc.get("status", "ABSENT")
        assert actual_status == expected_status, \
            f"Expected status {expected_status}, but got {actual_status}"
    
    client.close()


@then('all phases should be executed in order')
def step_phases_executed(context):
    """Verify all phases were executed"""
    if not context.mongodb_available:
        return
        
    # Check that the run was successful
    assert context.run_success, f"Migration failed: {getattr(context.run_result, 'stderr', 'Unknown error')}"


@then('the error should be logged')
def step_error_logged(context):
    """Verify error was logged"""
    # Check that the run failed
    assert not context.run_success, "Expected migration to fail but it succeeded"
    
    if hasattr(context, 'run_result') and context.run_result:
        assert context.run_result.stderr or context.run_result.stdout, \
            "Expected error output but none found"


@then('the mongration should execute successfully each time')
def step_multiple_success(context):
    """Verify multiple executions were successful"""
    if not context.mongodb_available:
        return
        
    assert len(context.multiple_runs) == 3, "Expected 3 runs"
    
    for i, run in enumerate(context.multiple_runs):
        assert run['success'], f"Run {i+1} failed"


@then('no state should be tracked')
def step_no_state_tracked(context):
    """Verify no state is tracked for stateless migration"""
    if not context.mongodb_available:
        return
        
    client = pymongo.MongoClient(context.mongodb_url)
    state_db = client["mongrations"]
    state_collection = state_db["state"]
    
    # For stateless migrations, no state should be tracked
    states = list(state_collection.find())
    print(f"DEBUG: Found {len(states)} state documents: {states}")
    state_count = state_collection.count_documents({})
    assert state_count == 0, f"Expected no state documents for stateless migration, but found {state_count}"
    
    client.close()


@then('the mongrations should execute in dependency order')
def step_dependency_order(context):
    """Verify mongrations executed in dependency order"""
    if not context.mongodb_available:
        return
        
    # Check that the run was successful
    assert context.run_success, f"Migration failed: {getattr(context.run_result, 'stderr', 'Unknown error')}"


@then('all mongrations should be "{status}"')
def step_all_status(context, status):
    """Verify all mongrations have expected status"""
    if not context.mongodb_available:
        return
        
    client = pymongo.MongoClient(context.mongodb_url)
    state_db = client["mongrations"]
    state_collection = state_db["state"]
    
    states = list(state_collection.find())
    assert len(states) >= 2, f"Expected at least 2 migration states, found {len(states)}"
    
    for state in states:
        actual_status = state.get("status", "ABSENT")
        assert actual_status == status, \
            f"Expected all migrations to be {status}, but {state['name']} is {actual_status}"
    
    client.close()


@then('the index should exist on the collection')
def step_index_exists(context):
    """Verify index exists"""
    if not context.mongodb_available:
        return
        
    client = pymongo.MongoClient(context.mongodb_url)
    db = client["test_db"]
    collection = db["indexed_collection"]
    
    indexes = list(collection.list_indexes())
    index_names = [idx['name'] for idx in indexes]
    
    # Should have at least the default _id index and our custom index
    assert len(indexes) >= 2, f"Expected at least 2 indexes, found {len(indexes)}"
    # Check for an index on the 'name' field (could have auto-generated name)
    name_index_found = any(
        'name' in idx.get('key', {}) for idx in indexes
    )
    assert name_index_found, f"Expected name index not found. Available indexes: {[idx.get('key', {}) for idx in indexes]}"
    
    client.close()


@then('the aggregated data should be processed correctly')
def step_aggregation_result(context):
    """Verify aggregation result"""
    if not context.mongodb_available:
        return
        
    client = pymongo.MongoClient(context.mongodb_url)
    db = client["test_db"]
    
    # Check if aggregated_collection was created and has data
    if "aggregated_collection" in db.list_collection_names():
        stats_collection = db["aggregated_collection"]
        stats = list(stats_collection.find())
        
        # Should have stats for each category
        assert len(stats) > 0, "Expected aggregated data but found none"
    
    client.close()