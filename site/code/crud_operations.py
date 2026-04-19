"""
CRUD Operations Module - Enhanced Version
CS 499 Capstone Project - Software Design and Engineering Enhancement

This module provides a generic CRUD class for MongoDB operations with
improved error handling, logging, and security features.

Author: Steven Foltz
Course: CS 499 Capstone
Date: March 22, 2026
"""

from pymongo import MongoClient
from pymongo.errors import PyMongoError, ConnectionFailure, ServerSelectionTimeoutError
import logging
import os
from typing import Optional, Dict, List, Any, Union
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CRUD:
    """
    A class to perform CRUD operations on MongoDB database.
    
    This class provides generic CRUD operations that can be used with any
    MongoDB database and collection. It includes comprehensive error handling,
    logging, and secure connection management.
    
    Attributes:
        client (MongoClient): MongoDB client connection
        db (Database): MongoDB database reference
        collection (Collection): MongoDB collection reference
    """
    
    def __init__(self, db_name: str, collection_name: str, 
                 username: Optional[str] = None, password: Optional[str] = None,
                 host: str = 'localhost', port: int = 27017):
        """
        Initialize the CRUD object with database connection details.
        
        Args:
            db_name (str): Name of the database
            collection_name (str): Name of the collection
            username (str, optional): MongoDB username. Defaults to None.
            password (str, optional): MongoDB password. Defaults to None.
            host (str, optional): MongoDB host. Defaults to 'localhost'.
            port (int, optional): MongoDB port. Defaults to 27017.
            
        Raises:
            ConnectionFailure: If unable to connect to MongoDB
            ServerSelectionTimeoutError: If server selection times out
        """
        try:
            if username and password:
                logger.info(f"Connecting to MongoDB at {host}:{port} with authentication")
                self.client = MongoClient(
                    host=host,
                    port=port,
                    username=username,
                    password=password,
                    authSource=db_name,
                    authMechanism='SCRAM-SHA-1',
                    serverSelectionTimeoutMS=5000  # 5 second timeout
                )
            else:
                logger.info(f"Connecting to MongoDB at {host}:{port} without authentication")
                self.client = MongoClient(host, port, serverSelectionTimeoutMS=5000)
            
            # Test the connection
            self.client.admin.command('ping')
            logger.info("Successfully connected to MongoDB server")
            
            self.db = self.client[db_name]
            self.collection = self.db[collection_name]
            logger.info(f"Using database: {db_name}, collection: {collection_name}")
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during connection: {e}")
            raise

    def create(self, document: Dict[str, Any]) -> bool:
        """
        Insert a document into the collection.
        
        Args:
            document (dict): Document to insert
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not document:
                logger.warning("Create operation called with empty document")
                return False
            
            result = self.collection.insert_one(document)
            logger.info(f"Document created successfully with ID: {result.inserted_id}")
            return result.acknowledged
            
        except PyMongoError as e:
            logger.error(f"Error creating document: {e}")
            return False

    def read(self, query: Optional[Dict[str, Any]] = None, 
             projection: Optional[Dict[str, int]] = None,
             limit: int = 0) -> List[Dict[str, Any]]:
        """
        Query for documents in the collection.
        
        Args:
            query (dict, optional): Query criteria. Defaults to None (all documents).
            projection (dict, optional): Fields to include/exclude. Defaults to None.
            limit (int, optional): Maximum number of documents to return. Defaults to 0 (no limit).
            
        Returns:
            list: List of matching documents (empty list if none found)
        """
        try:
            if query is None:
                query = {}
                logger.info("Reading all documents from collection")
            else:
                logger.info(f"Reading documents with query: {query}")
            
            cursor = self.collection.find(query, projection)
            
            if limit > 0:
                cursor = cursor.limit(limit)
            
            results = list(cursor)
            logger.info(f"Read operation returned {len(results)} documents")
            return results
            
        except PyMongoError as e:
            logger.error(f"Error reading documents: {e}")
            return []

    def update(self, query: Dict[str, Any], update_data: Dict[str, Any]) -> int:
        """
        Update documents matching the query.
        
        Args:
            query (dict): Query criteria
            update_data (dict): Update operations and values
            
        Returns:
            int: Number of documents modified
        """
        try:
            if not query:
                logger.warning("Update operation called with empty query")
                return 0
            if not update_data:
                logger.warning("Update operation called with empty update_data")
                return 0
            
            result = self.collection.update_many(query, update_data)
            logger.info(f"Updated {result.modified_count} documents")
            return result.modified_count
            
        except PyMongoError as e:
            logger.error(f"Error updating documents: {e}")
            return 0

    def delete(self, query: Dict[str, Any]) -> int:
        """
        Delete documents matching the query.
        
        Args:
            query (dict): Query criteria
            
        Returns:
            int: Number of documents deleted
        """
        try:
            if not query:
                logger.warning("Delete operation called with empty query")
                return 0
            
            result = self.collection.delete_many(query)
            logger.info(f"Deleted {result.deleted_count} documents")
            return result.deleted_count
            
        except PyMongoError as e:
            logger.error(f"Error deleting documents: {e}")
            return 0

    def count(self, query: Optional[Dict[str, Any]] = None) -> int:
        """
        Count documents matching the query.
        
        Args:
            query (dict, optional): Query criteria. Defaults to None (all documents).
            
        Returns:
            int: Number of documents matching the query
        """
        try:
            if query is None:
                query = {}
            
            count = self.collection.count_documents(query)
            logger.info(f"Count operation returned {count} documents")
            return count
            
        except PyMongoError as e:
            logger.error(f"Error counting documents: {e}")
            return 0

    def aggregate(self, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Perform aggregation pipeline on the collection.
        
        Args:
            pipeline (list): Aggregation pipeline stages
            
        Returns:
            list: Aggregation results
        """
        try:
            if not pipeline:
                logger.warning("Aggregation operation called with empty pipeline")
                return []
            
            results = list(self.collection.aggregate(pipeline))
            logger.info(f"Aggregation returned {len(results)} results")
            return results
            
        except PyMongoError as e:
            logger.error(f"Error during aggregation: {e}")
            return []

    def close(self) -> None:
        """Close the MongoDB connection."""
        if hasattr(self, 'client'):
            self.client.close()
            logger.info("MongoDB connection closed")

    def __del__(self) -> None:
        """Destructor to ensure connection is closed."""
        self.close()
