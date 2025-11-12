"""
Helper utilities for test steps to reduce duplication.
"""
import pymongo
from contextlib import contextmanager
from typing import Optional, Dict, Any, List


def skip_if_mongodb_unavailable(context):
    """Skip scenario if MongoDB is not available."""
    if not getattr(context, 'mongodb_available', False):
        context.scenario.skip("MongoDB not available")
        return True
    return False


@contextmanager
def mongodb_client(context, database_name: Optional[str] = None):
    """
    Context manager for MongoDB client connections.
    
    Usage:
        with mongodb_client(context) as db:
            collection = db["my_collection"]
            collection.insert_one({"test": "data"})
    
    Note: Uses context.test_db_name by default.
    """
    client = None
    try:
        client = pymongo.MongoClient(context.mongodb_url, serverSelectionTimeoutMS=5000)
        # Use test_db_name from context
        db_name = database_name or getattr(context, 'test_db_name', 'test_db')
        yield client[db_name]
    finally:
        if client:
            client.close()


def get_mongrations_state_db(context):
    """Get the mongrations state database."""
    client = pymongo.MongoClient(context.mongodb_url, serverSelectionTimeoutMS=5000)
    state_db_name = getattr(context, 'state_db_name', 'mongrations')
    return client[state_db_name]


def collection_exists(context, collection_name: str, database: Optional[str] = None) -> bool:
    """
    Check if a collection exists in the database.
    """
    db_name = database or getattr(context, 'test_db_name', 'test_db')
    with mongodb_client(context, db_name) as db:
        return collection_name in db.list_collection_names()


def get_collection_count(context, collection_name: str, database: Optional[str] = None) -> int:
    """
    Get the document count for a collection.
    """
    db_name = database or getattr(context, 'test_db_name', 'test_db')
    with mongodb_client(context, db_name) as db:
        return db[collection_name].count_documents({})


def insert_documents(context, collection_name: str, documents: List[Dict[str, Any]], 
                     database: Optional[str] = None):
    """
    Insert documents into a collection.
    """
    db_name = database or getattr(context, 'test_db_name', 'test_db')
    with mongodb_client(context, db_name) as db:
        if documents:
            db[collection_name].insert_many(documents)


def get_migration_status(context, migration_name: Optional[str] = None) -> Optional[str]:
    """
    Get the status of a migration from the state collection.
    
    Args:
        context: Behave context
        migration_name: Name of the migration. If None, gets the latest.
    
    Returns:
        Status string or None if not found
    """
    client = None
    try:
        client = pymongo.MongoClient(context.mongodb_url, serverSelectionTimeoutMS=5000)
        state_db_name = getattr(context, 'state_db_name', 'mongrations')
        state_db = client[state_db_name]
        state_collection = state_db["state"]
        
        if migration_name:
            state_doc = state_collection.find_one({"name": migration_name})
        else:
            state_doc = state_collection.find_one(sort=[("_id", -1)])
        
        return state_doc.get("status") if state_doc else None
    finally:
        if client:
            client.close()


def verify_indexes_exist(context, collection_name: str, 
                        expected_fields: Optional[List[str]] = None,
                        min_count: int = 2, database: Optional[str] = None) -> bool:
    """
    Verify indexes exist on a collection.
    
    Args:
        context: Behave context
        collection_name: Name of the collection
        expected_fields: List of field names that should be indexed
        min_count: Minimum number of indexes expected (including _id)
        database: Database name (uses context.test_db_name if not specified)
        
    Returns:
        True if verification passes
    """
    db_name = database or getattr(context, 'test_db_name', 'test_db')
    with mongodb_client(context, db_name) as db:
        collection = db[collection_name]
        indexes = list(collection.list_indexes())
        
        # Check minimum count
        if len(indexes) < min_count:
            return False
        
        # If specific fields are expected, verify them
        if expected_fields:
            index_fields = set()
            for idx in indexes:
                index_fields.update(idx.get('key', {}).keys())
            
            return all(field in index_fields for field in expected_fields)
        
        return True


def table_to_documents(table) -> List[Dict[str, Any]]:
    """
    Convert a Behave table to a list of document dictionaries.
    Attempts to convert numeric strings to integers.
    
    Args:
        table: Behave table object or None
        
    Returns:
        List of document dictionaries
    """
    if table is None:
        return []
    
    documents = []
    for row in table:
        doc = {}
        for heading in table.headings:
            value = row[heading]
            # Try to convert to int
            try:
                doc[heading] = int(value)
            except (ValueError, AttributeError):
                doc[heading] = value
        documents.append(doc)
    return documents


def verify_field_in_documents(context, collection_name: str, field_name: str,
                              database: Optional[str] = None) -> bool:
    """
    Verify that documents in a collection contain a specific field.
    
    Args:
        context: Behave context
        collection_name: Name of the collection
        field_name: Field name to check for
        database: Database name (optional)
        
    Returns:
        True if all sampled documents have the field
    """
    db_name = database or getattr(context, 'test_db_name', 'test_db')
    with mongodb_client(context, db_name) as db:
        collection = db[collection_name]
        docs = list(collection.find({}).limit(5))
        
        if docs:
            return all(field_name in doc for doc in docs)
    
    return False


def run_mongration_cli(context, mongration_file: Optional[str] = None, 
                      mongrations_dir: Optional[str] = None,
                      dry_run: bool = False,
                      command: str = 'run',
                      status: Optional[str] = None,
                      index: Optional[int] = None) -> Dict[str, Any]:
    """
    Run mongration using the CLI API (not subprocess).
    
    Args:
        context: Behave context
        mongration_file: Path to single mongration file (absolute path or relative to test_migrations)
        mongrations_dir: Path to directory of mongrations  
        dry_run: Whether to run in dry-run mode
        command: Command to execute ('run' or 'manipulate')
        status: Status to set (for 'manipulate' command)
        index: Index to set (for 'manipulate' command)
    
    Returns:
        Dict with keys: 'exit_code', 'stdout', 'stderr'
    """
    from mongrations.main import run_mongration_from_args
    from pathlib import Path
    import io
    import logging
    
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
            # If it's not an absolute path, assume it's in test_migrations
            file_path = Path(mongration_file)
            if not file_path.is_absolute():
                from pathlib import Path as P
                test_migrations_dir = P(__file__).parent.parent.parent / "test_migrations"
                file_path = test_migrations_dir / mongration_file
            kwargs['mongration'] = str(file_path)
        elif mongrations_dir:
            kwargs['mongrations_dir'] = str(mongrations_dir)
        
        if command == 'manipulate':
            kwargs['command'] = command
            if status:
                kwargs['status'] = status
            if index is not None:
                kwargs['index'] = index
        
        exit_code = run_mongration_from_args(**kwargs)
        stderr_output = stderr_capture.getvalue()
        
        return {
            'exit_code': exit_code,
            'stdout': '',
            'stderr': stderr_output
        }
    except Exception as e:
        import traceback
        formatted_exc = traceback.format_exc()
        migration_logger.error(formatted_exc)
        stderr_output = stderr_capture.getvalue() + formatted_exc
        
        return {
            'exit_code': 1,
            'stdout': '',
            'stderr': stderr_output
        }
    finally:
        migration_logger.removeHandler(stderr_handler)
        stderr_capture.close()


def verify_cli_success(context) -> None:
    """
    Verify that a CLI command succeeded.
    Raises AssertionError if it failed.
    """
    assert hasattr(context, 'cli_result'), "No CLI result found in context"
    assert context.cli_result['exit_code'] == 0, \
        f"Command failed with exit code {context.cli_result['exit_code']}\nStderr: {context.cli_result['stderr']}"


def verify_cli_failure(context) -> None:
    """
    Verify that a CLI command failed.
    Raises AssertionError if it succeeded.
    """
    assert hasattr(context, 'cli_result'), "No CLI result found in context"
    assert context.cli_result['exit_code'] != 0, \
        f"Command should have failed but succeeded with exit code 0"


def verify_error_message_contains(context, *keywords) -> None:
    """
    Verify that error output contains specific keywords.
    
    Args:
        context: Behave context
        keywords: One or more keywords to check for (case-insensitive)
    """
    stderr = context.cli_result.get('stderr', '')
    stdout = context.cli_result.get('stdout', '')
    output = (stderr + stdout).lower()
    
    assert any(keyword.lower() in output for keyword in keywords), \
        f"Error message doesn't contain any of {keywords}: {output}"


def get_state_document(context, migration_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Get the state document for a migration.
    
    Args:
        context: Behave context
        migration_name: Name of the migration. If None, gets the latest.
    
    Returns:
        State document or None if not found
    """
    with mongodb_client(context, context.state_db_name) as state_db:
        state_collection = state_db["state"]
        
        if migration_name:
            return state_collection.find_one({"name": migration_name})
        else:
            return state_collection.find_one(sort=[("_id", -1)])


def count_completed_migrations(context) -> int:
    """
    Count the number of completed migrations in the state database.
    
    Args:
        context: Behave context
    
    Returns:
        Number of completed migrations
    """
    with mongodb_client(context, context.state_db_name) as state_db:
        state_collection = state_db["state"]
        return state_collection.count_documents({"status": "COMPLETED"})
