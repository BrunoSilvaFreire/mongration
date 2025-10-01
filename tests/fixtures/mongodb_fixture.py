"""
MongoDB fixture module for managing test database containers.

This module provides a reusable MongoDBFixture class that manages
a MongoDB Docker container for testing purposes.
"""
import os
import subprocess
import time
from pathlib import Path
from typing import Optional, Dict, Any
import pymongo


class MongoDBFixture:
    """
    Manages a MongoDB Docker container for testing.
    
    This class handles starting, stopping, and cleaning up a MongoDB
    container using Docker Compose. It also provides helper methods
    for connecting to the database and cleaning test data.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 27017,
        username: str = "testuser",
        password: str = "testpass",
        auth_db: str = "admin",
        test_db_name: str = "mongrations_test"
    ):
        """
        Initialize the MongoDB fixture.
        
        Args:
            host: MongoDB host (default: localhost)
            port: MongoDB port (default: 27017)
            username: MongoDB username for authentication
            password: MongoDB password for authentication
            auth_db: Authentication database (default: admin)
            test_db_name: Name of the test database
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.auth_db = auth_db
        self.test_db_name = test_db_name
        
        # Determine the docker-compose file location
        tests_dir = Path(__file__).parent.parent
        self.docker_compose_file = tests_dir / "docker-compose.yml"
        self.tests_dir = tests_dir
        
        self._started = False
        self._compose_cmd = None
    
    @property
    def connection_string(self) -> str:
        """
        Get the MongoDB connection string.
        
        Returns:
            str: MongoDB connection URI
        """
        return f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.auth_db}"
    
    @property
    def connection_string_no_auth(self) -> str:
        """
        Get the MongoDB connection string without authentication.
        
        Returns:
            str: MongoDB connection URI without credentials
        """
        return f"mongodb://{self.host}:{self.port}"
    
    def _get_compose_command(self) -> Optional[list]:
        """
        Determine which docker compose command is available.
        
        Returns:
            list: Command to use for docker compose, or None if unavailable
        """
        if self._compose_cmd is not None:
            return self._compose_cmd
        
        # Try docker compose (newer)
        try:
            subprocess.run(
                ["docker", "compose", "--version"],
                capture_output=True,
                check=True
            )
            self._compose_cmd = ["docker", "compose"]
            return self._compose_cmd
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass
        
        # Try docker-compose (older)
        try:
            subprocess.run(
                ["docker-compose", "--version"],
                capture_output=True,
                check=True
            )
            self._compose_cmd = ["docker-compose"]
            return self._compose_cmd
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass
        
        return None
    
    def start(self, timeout: int = 30) -> bool:
        """
        Start the MongoDB container using Docker Compose.
        
        Args:
            timeout: Maximum seconds to wait for MongoDB to be ready
            
        Returns:
            bool: True if started successfully, False otherwise
        """
        compose_cmd = self._get_compose_command()
        if compose_cmd is None:
            print("Warning: Docker Compose not available. Please install Docker and Docker Compose.")
            return False
        
        try:
            print("Starting MongoDB test container...")
            
            # Stop any existing container
            subprocess.run(
                compose_cmd + ["-f", str(self.docker_compose_file), "down"],
                cwd=self.tests_dir,
                capture_output=True
            )
            
            # Start the container
            result = subprocess.run(
                compose_cmd + ["-f", str(self.docker_compose_file), "up", "-d"],
                cwd=self.tests_dir,
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                print(f"Failed to start MongoDB container: {result.stderr}")
                return False
            
            # Wait for MongoDB to be healthy
            print("Waiting for MongoDB to be ready...")
            for i in range(timeout):
                if self._check_connection():
                    print(f"✓ MongoDB container is ready at {self.connection_string}")
                    self._started = True
                    return True
                time.sleep(1)
            
            print("✗ MongoDB container failed to become ready")
            return False
            
        except Exception as e:
            print(f"Error starting MongoDB container: {e}")
            return False
    
    def stop(self) -> None:
        """Stop the MongoDB Docker container."""
        if not self._started:
            return
        
        compose_cmd = self._get_compose_command()
        if compose_cmd is None:
            return
        
        try:
            print("Stopping MongoDB test container...")
            subprocess.run(
                compose_cmd + ["-f", str(self.docker_compose_file), "down"],
                cwd=self.tests_dir,
                capture_output=True
            )
            print("✓ MongoDB container stopped")
            self._started = False
        except Exception as e:
            print(f"Error stopping MongoDB container: {e}")
    
    def _check_connection(self) -> bool:
        """
        Check if MongoDB is accepting connections.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            client = pymongo.MongoClient(
                self.connection_string,
                serverSelectionTimeoutMS=2000
            )
            client.server_info()
            client.close()
            return True
        except Exception:
            return False
    
    def get_client(self, **kwargs) -> pymongo.MongoClient:
        """
        Get a MongoDB client connected to the test database.
        
        Args:
            **kwargs: Additional arguments to pass to MongoClient
            
        Returns:
            pymongo.MongoClient: Connected MongoDB client
        """
        return pymongo.MongoClient(self.connection_string, **kwargs)
    
    def clean_test_databases(self) -> None:
        """
        Clean all test databases.
        
        This drops the test database and the mongrations state database.
        """
        try:
            client = self.get_client(serverSelectionTimeoutMS=5000)
            
            # Drop test database
            client.drop_database(self.test_db_name)
            
            # Drop mongrations state database
            client.drop_database('mongrations')
            
            client.close()
        except Exception as e:
            print(f"Warning: Could not clean test databases: {e}")
    
    def is_available(self) -> bool:
        """
        Check if MongoDB is available and responding.
        
        Returns:
            bool: True if MongoDB is available, False otherwise
        """
        return self._started and self._check_connection()
    
    def get_database(self, db_name: Optional[str] = None) -> Any:
        """
        Get a specific database.
        
        Args:
            db_name: Name of the database. If None, uses test_db_name
            
        Returns:
            pymongo.database.Database: The database object
        """
        client = self.get_client()
        return client[db_name or self.test_db_name]
