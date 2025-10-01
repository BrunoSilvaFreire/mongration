"""
Pytest configuration and fixtures for mongrations tests.

This module provides reusable fixtures for testing, including MongoDB container management.
"""
import pytest
import tempfile
import shutil
from pathlib import Path
from tests.fixtures.mongodb_fixture import MongoDBFixture


@pytest.fixture(scope="session")
def mongodb_container():
    """
    Session-scoped fixture that starts a MongoDB container for testing.
    
    The container is started once at the beginning of the test session
    and stopped at the end.
    
    Yields:
        MongoDBFixture: A fixture object with MongoDB connection details
    """
    fixture = MongoDBFixture()
    
    # Start the container
    if not fixture.start():
        pytest.skip("MongoDB container could not be started")
    
    yield fixture
    
    # Cleanup
    fixture.stop()


@pytest.fixture(scope="function")
def clean_mongodb(mongodb_container):
    """
    Function-scoped fixture that provides a clean MongoDB instance for each test.
    
    This fixture cleans up any test databases before each test runs.
    
    Args:
        mongodb_container: The session-scoped MongoDB container fixture
        
    Yields:
        MongoDBFixture: A fixture object with a clean database
    """
    # Clean up before test
    mongodb_container.clean_test_databases()
    
    yield mongodb_container
    
    # Clean up after test
    mongodb_container.clean_test_databases()


@pytest.fixture(scope="function")
def temp_mongrations_dir():
    """
    Function-scoped fixture that provides a temporary directory for mongration scripts.
    
    The directory is created before each test and cleaned up afterwards.
    
    Yields:
        Path: Path to the temporary mongrations directory
    """
    temp_dir = tempfile.mkdtemp(prefix="mongrations_test_")
    mongrations_dir = Path(temp_dir) / "mongrations"
    mongrations_dir.mkdir(exist_ok=True)
    
    yield mongrations_dir
    
    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="function")
def mongodb_client(clean_mongodb):
    """
    Function-scoped fixture that provides a MongoDB client for testing.
    
    Args:
        clean_mongodb: The clean MongoDB fixture
        
    Yields:
        pymongo.MongoClient: A MongoDB client connected to the test database
    """
    client = clean_mongodb.get_client()
    yield client
    client.close()


@pytest.fixture(scope="function")
def test_database(mongodb_client, clean_mongodb):
    """
    Function-scoped fixture that provides access to the test database.
    
    Args:
        mongodb_client: The MongoDB client fixture
        clean_mongodb: The clean MongoDB fixture
        
    Returns:
        pymongo.database.Database: The test database
    """
    return mongodb_client[clean_mongodb.test_db_name]
