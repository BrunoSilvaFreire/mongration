import os
import tempfile
import shutil
from pathlib import Path
import pymongo
import logging
import json
from datetime import datetime
from tests.fixtures.mongodb_fixture import MongoDBFixture


def before_all(context):
    """Setup test environment before all tests"""
    # Configure logging to show all levels
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        force=True
    )
    # Ensure mongrations loggers are set to DEBUG
    logging.getLogger('mongrations').setLevel(logging.DEBUG)
    
    # Create a temporary directory for test mongrations
    context.temp_dir = tempfile.mkdtemp(prefix="mongrations_test_")
    context.mongrations_dir = Path(context.temp_dir) / "mongrations"
    context.mongrations_dir.mkdir(exist_ok=True)
    
    # Initialize MongoDB fixture
    context.mongodb_fixture = MongoDBFixture()
    
    # Start MongoDB container
    context.mongodb_started = context.mongodb_fixture.start()
    
    if context.mongodb_started:
        # Use MongoDB connection settings from fixture
        context.mongodb_url = context.mongodb_fixture.connection_string
        context.test_db_name = context.mongodb_fixture.test_db_name
        print(f"Using MongoDB: {context.mongodb_url}")
    else:
        # Fallback to local MongoDB without auth
        context.mongodb_url = os.environ.get('MONGODB_TEST_URL', 'mongodb://localhost:27017')
        context.test_db_name = 'mongrations_test'
        print(f"Fallback to local MongoDB: {context.mongodb_url}")
    
    # Database and collection names for testing
    # IMPORTANT: test_db_name must match the hardcoded database in test migrations
    # All test migrations use 'test_db' as the database name, so we force it here
    # to avoid database mismatch issues
    context.state_db_name = 'mongrations'  # Database for migration state tracking
    
    # Common collection names used in tests
    context.test_collection_name = 'test_collection'
    context.indexed_collection_name = 'indexed_collection'
    context.source_collection_name = 'source_collection'
    context.aggregated_collection_name = 'aggregated_collection'
    context.new_schema_collection_name = 'new_schema_collection'
    
    # Field names commonly used in tests
    context.default_index_field = 'name'
    
    # Engine will be initialized by steps when needed
    context.engine = None
    
    # Log all context configuration values
    print("\n" + "="*70)
    print("Test Environment Configuration")
    print("="*70)
    print(f"MongoDB Configuration:")
    print(f"  - mongodb_url: {context.mongodb_url}")
    print(f"  - mongodb_started: {context.mongodb_started}")
    print(f"\nDatabase Names:")
    print(f"  - test_db_name: {context.test_db_name}")
    print(f"  - state_db_name: {context.state_db_name}")
    print(f"\nCollection Names:")
    print(f"  - test_collection_name: {context.test_collection_name}")
    print(f"  - indexed_collection_name: {context.indexed_collection_name}")
    print(f"  - source_collection_name: {context.source_collection_name}")
    print(f"  - aggregated_collection_name: {context.aggregated_collection_name}")
    print(f"  - new_schema_collection_name: {context.new_schema_collection_name}")
    print(f"\nField Names:")
    print(f"  - default_index_field: {context.default_index_field}")
    print(f"\nDirectories:")
    print(f"  - temp_dir: {context.temp_dir}")
    print(f"  - mongrations_dir: {context.mongrations_dir}")
    print("="*70 + "\n")


def after_all(context):
    """Cleanup after all tests"""
    # Stop MongoDB container if we started it
    if getattr(context, 'mongodb_started', False):
        context.mongodb_fixture.stop()
    
    # Clean up temporary directory
    if hasattr(context, 'temp_dir') and os.path.exists(context.temp_dir):
        shutil.rmtree(context.temp_dir)


def before_scenario(context, scenario):
    """Setup before each scenario"""
    # Always clean the test database directly since we use a fixed database name
    try:
        client = pymongo.MongoClient(context.mongodb_url, serverSelectionTimeoutMS=5000)
        # Drop the test database (which matches migrations' hardcoded 'test_db')
        client.drop_database(context.test_db_name)
        # Drop the mongrations state database
        client.drop_database(context.state_db_name)
        client.close()
    except Exception as e:
        print(f"Warning: Could not clean databases: {e}")
    
    # Clean up any leftover export files from previous test runs
    _cleanup_export_files()


def after_scenario(context, scenario):
    """Cleanup after each scenario"""
    # Take a snapshot of the MongoDB database before cleanup
    if hasattr(context, 'mongodb_url'):
        _take_mongodb_snapshot(context, scenario)
    
    # Clean up any test files created during the scenario
    # Only clean the temporary mongrations directory, not the actual test_migrations directory
    if hasattr(context, 'mongrations_dir') and hasattr(context, 'temp_dir'):
        temp_mongrations_path = Path(context.temp_dir) / "mongrations"
        # Only delete files if we're cleaning the temp directory, not test_migrations
        if context.mongrations_dir == temp_mongrations_path:
            for file in context.mongrations_dir.glob("*.py"):
                if file.name != "__init__.py":
                    file.unlink()


def _take_mongodb_snapshot(context, scenario):
    """
    Take a snapshot of all MongoDB databases and save to reports directory.
    Each collection is saved as a separate file.
    
    Args:
        context: Behave context object
        scenario: Behave scenario object
    """
    try:
        # Create reports directory if it doesn't exist
        tests_dir = Path(__file__).parent.parent
        reports_dir = tests_dir / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        # Create a sanitized scenario name for the directory
        scenario_name = scenario.name.replace(' ', '_').replace('/', '_')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        snapshot_dir_name = f"{scenario_name}_{timestamp}"
        snapshot_dir = reports_dir / "snapshots" / snapshot_dir_name
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        
        # Connect to MongoDB
        client = pymongo.MongoClient(context.mongodb_url, serverSelectionTimeoutMS=5000)
        
        # Create a manifest file with metadata
        manifest = {
            "scenario": scenario.name,
            "timestamp": timestamp,
            "status": scenario.status.name,
            "databases": {}
        }
        
        # Get all databases (excluding system databases)
        db_names = [db for db in client.list_database_names() 
                    if db not in ['admin', 'local', 'config']]
        
        collection_count = 0
        
        # Snapshot each database
        for db_name in db_names:
            db = client[db_name]
            
            # Create database directory
            db_dir = snapshot_dir / db_name
            db_dir.mkdir(parents=True, exist_ok=True)
            
            # Get all collections in the database
            collection_names = db.list_collection_names()
            manifest["databases"][db_name] = {
                "collections": collection_names,
                "collection_count": len(collection_names)
            }
            
            for coll_name in collection_names:
                collection = db[coll_name]
                
                # Get all documents from the collection
                documents = list(collection.find({}))
                
                # Convert ObjectId and other BSON types to JSON-serializable format
                for doc in documents:
                    _convert_to_json_serializable(doc)
                
                # Save collection to its own file
                collection_file = db_dir / f"{coll_name}.json"
                collection_data = {
                    "database": db_name,
                    "collection": coll_name,
                    "count": len(documents),
                    "documents": documents
                }
                
                with open(collection_file, 'w') as f:
                    json.dump(collection_data, f, indent=2, default=str)
                
                collection_count += 1
        
        client.close()
        
        # Write manifest file
        manifest_path = snapshot_dir / "_manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2, default=str)
        
        print(f"✓ MongoDB snapshot saved to: {snapshot_dir}")
        print(f"  - {len(db_names)} database(s), {collection_count} collection(s)")
        
    except Exception as e:
        print(f"Warning: Could not take MongoDB snapshot: {e}")


def _convert_to_json_serializable(obj):
    """
    Recursively convert BSON types to JSON-serializable types.
    
    Args:
        obj: Object to convert (dict, list, or primitive)
    """
    if isinstance(obj, dict):
        for key, value in obj.items():
            if hasattr(value, '__dict__') and hasattr(value, '__class__'):
                # Convert BSON types like ObjectId, DateTime, etc.
                obj[key] = str(value)
            elif isinstance(value, (dict, list)):
                _convert_to_json_serializable(value)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            if hasattr(item, '__dict__') and hasattr(item, '__class__'):
                obj[i] = str(item)
            elif isinstance(item, (dict, list)):
                _convert_to_json_serializable(item)


def _cleanup_export_files():
    """
    Clean up export files from /tmp directory before each scenario.
    This prevents tests from finding files from previous runs.
    """
    import glob
    
    # Patterns for export files created by test migrations
    patterns = [
        '/tmp/mongrations_export*.json',
        '/tmp/mongrations_export*.ejson',
        '/tmp/mongrations_export*.csv',
        '/tmp/mongrations_file_dest*.jsonl',
        '/tmp/mongrations_import*.json',
        '/tmp/mongrations_import*.ejson',
    ]
    
    for pattern in patterns:
        for file_path in glob.glob(pattern):
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"Warning: Could not delete {file_path}: {e}")
