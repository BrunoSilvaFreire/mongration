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
    
    Note: Uses context.test_db_name by default, or context.fallback_db_name if not set.
    However, test migrations hardcode the fallback DB, so we check both databases.
    """
    client = None
    try:
        client = pymongo.MongoClient(context.mongodb_url, serverSelectionTimeoutMS=5000)
        # Use test_db_name from context, fallback to context.fallback_db_name
        db_name = database_name or getattr(context, 'test_db_name', getattr(context, 'fallback_db_name', 'test_db'))
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
    
    Checks both the configured test database and fallback DB since
    test migrations hardcode the fallback DB as the database name.
    """
    # First check the specified/default database
    fallback_db = getattr(context, 'fallback_db_name', 'test_db')
    db_name = database or getattr(context, 'test_db_name', fallback_db)
    with mongodb_client(context, db_name) as db:
        if collection_name in db.list_collection_names():
            return True
    
    # Also check fallback DB if it's different (migrations hardcode this)
    if db_name != fallback_db:
        with mongodb_client(context, fallback_db) as db:
            return collection_name in db.list_collection_names()
    
    return False


def get_collection_count(context, collection_name: str, database: Optional[str] = None) -> int:
    """
    Get the document count for a collection.
    
    Checks both the configured test database and fallback DB since
    test migrations hardcode the fallback DB as the database name.
    """
    # First try the specified/default database
    fallback_db = getattr(context, 'fallback_db_name', 'test_db')
    db_name = database or getattr(context, 'test_db_name', fallback_db)
    with mongodb_client(context, db_name) as db:
        count = db[collection_name].count_documents({})
        if count > 0:
            return count
    
    # Also try fallback DB if it's different
    if db_name != fallback_db:
        with mongodb_client(context, fallback_db) as db:
            return db[collection_name].count_documents({})
    
    return 0


def insert_documents(context, collection_name: str, documents: List[Dict[str, Any]], 
                     database: Optional[str] = None):
    """
    Insert documents into a collection.
    
    Uses fallback DB by default since test migrations hardcode this database.
    """
    # Use fallback DB by default to match where migrations write
    fallback_db = getattr(context, 'fallback_db_name', 'test_db')
    db_name = database or fallback_db
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
        database: Database name (uses context.fallback_db_name if not specified)
        
    Returns:
        True if verification passes
    """
    db_name = database or getattr(context, 'fallback_db_name', 'test_db')
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
        table: Behave table object
        
    Returns:
        List of document dictionaries
    """
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
    
    Checks both the configured test database and 'test_db' since
    test migrations hardcode 'test_db' as the database name.
    
    Args:
        context: Behave context
        collection_name: Name of the collection
        field_name: Field name to check for
        database: Database name (optional)
        
    Returns:
        True if all sampled documents have the field
    """
    # First try the specified/default database
    db_name = database or getattr(context, 'test_db_name', 'test_db')
    with mongodb_client(context, db_name) as db:
        collection = db[collection_name]
        docs = list(collection.find({}).limit(5))
        
        if docs:
            return all(field_name in doc for doc in docs)
    
    # Also try 'test_db' if different
    if db_name != 'test_db':
        with mongodb_client(context, 'test_db') as db:
            collection = db[collection_name]
            docs = list(collection.find({}).limit(5))
            
            if docs:
                return all(field_name in doc for doc in docs)
    
    return False
