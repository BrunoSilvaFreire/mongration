import os
import tempfile
import shutil
from pathlib import Path
import pymongo
import logging
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
    
    # Engine will be initialized by steps when needed
    context.engine = None


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
    # Clean the test database using the fixture if available
    if hasattr(context, 'mongodb_fixture') and context.mongodb_started:
        context.mongodb_fixture.clean_test_databases()
    else:
        # Fallback to manual cleanup
        try:
            client = pymongo.MongoClient(context.mongodb_url, serverSelectionTimeoutMS=5000)
            client.drop_database(context.test_db_name)
            client.close()
        except Exception as e:
            print(f"Warning: Could not clean test database: {e}")
        
        # Clean mongrations state database
        try:
            client = pymongo.MongoClient(context.mongodb_url, serverSelectionTimeoutMS=5000)
            client.drop_database('mongrations')
            client.close()
        except Exception as e:
            print(f"Warning: Could not clean mongrations state database: {e}")


def after_scenario(context, scenario):
    """Cleanup after each scenario"""
    # Clean up any test files created during the scenario
    # Only clean the temporary mongrations directory, not the actual test_migrations directory
    if hasattr(context, 'mongrations_dir') and hasattr(context, 'temp_dir'):
        temp_mongrations_path = Path(context.temp_dir) / "mongrations"
        # Only delete files if we're cleaning the temp directory, not test_migrations
        if context.mongrations_dir == temp_mongrations_path:
            for file in context.mongrations_dir.glob("*.py"):
                if file.name != "__init__.py":
                    file.unlink()