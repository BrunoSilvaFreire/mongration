"""
Test fixtures and utilities for mongrations functional tests.
"""
import shutil
import tempfile
import textwrap
from pathlib import Path
from typing import Dict, Any, List


# Path to the test mongrations directory
MONGRATIONS_DIR = Path(__file__).parent.parent / "mongrations"


class MongrationFixture:
    """Helper class to create mongration test scripts."""
    
    def __init__(self, temp_dir: Path):
        self.temp_dir = temp_dir
        self.counter = 1
    
    def _copy_migration_template(self, template_name: str, name: str = None) -> Path:
        """Copy a migration template from the mongrations directory."""
        source_file = MONGRATIONS_DIR / f"{template_name}.py"
        
        if name is None:
            name = f"{self.counter:03d}_{template_name}"
            self.counter += 1
        
        dest_file = self.temp_dir / f"{name}.py"
        shutil.copy(source_file, dest_file)
        return dest_file
    
    def create_simple_migration(self, name: str = None) -> Path:
        """Create a simple migration that inserts a document."""
        return self._copy_migration_template("simple_migration", name)
    
    def create_aggregation_migration(self, name: str = None) -> Path:
        """Create a migration that performs aggregation."""
        return self._copy_migration_template("aggregation_migration", name)
    
    def create_index_migration(self, name: str = None) -> Path:
        """Create a migration that creates an index."""
        return self._copy_migration_template("index_migration", name)
    
    def create_stateless_migration(self, name: str = None) -> Path:
        """Create a stateless migration."""
        return self._copy_migration_template("stateless_migration", name)
    
    def create_failing_migration(self, name: str = None) -> Path:
        """Create a migration that will fail."""
        return self._copy_migration_template("failing_migration", name)
    
    def create_custom_migration(self, content: str, name: str = None) -> Path:
        """Create a migration with custom content."""
        if name is None:
            name = f"{self.counter:03d}_custom"
            self.counter += 1
        
        file_path = self.temp_dir / f"{name}.py"
        file_path.write_text(content)
        return file_path


def create_test_documents() -> List[Dict[str, Any]]:
    """Create a list of test documents for seeding databases."""
    return [
        {"name": "Alice", "age": 30, "department": "Engineering", "active": True},
        {"name": "Bob", "age": 25, "department": "Marketing", "active": True},
        {"name": "Charlie", "age": 35, "department": "Engineering", "active": False},
        {"name": "Diana", "age": 28, "department": "Sales", "active": True},
        {"name": "Eve", "age": 32, "department": "Marketing", "active": True},
    ]


def create_test_categories() -> List[Dict[str, Any]]:
    """Create test category documents."""
    return [
        {"category": "electronics", "name": "Electronics"},
        {"category": "books", "name": "Books"},
        {"category": "clothing", "name": "Clothing"},
        {"category": "home", "name": "Home & Garden"},
    ]


def create_test_products() -> List[Dict[str, Any]]:
    """Create test product documents."""
    return [
        {"name": "Laptop", "category": "electronics", "price": 999.99},
        {"name": "Book", "category": "books", "price": 19.99},
        {"name": "T-Shirt", "category": "clothing", "price": 29.99},
        {"name": "Chair", "category": "home", "price": 149.99},
        {"name": "Phone", "category": "electronics", "price": 699.99},
    ]