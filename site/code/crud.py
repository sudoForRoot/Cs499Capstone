"""
CRUD Operations Module for Grazioso Salvare Dashboard
CS 499 Capstone Project - Software Design and Engineering Enhancement

This module provides database CRUD operations for the Austin Animal Center data.
It handles connections to MongoDB and provides specialized query methods for
different rescue types.

Author: Steven Foltz
Course: CS 499 Capstone
Date: March 22, 2026
"""

from pymongo import MongoClient
from pymongo.errors import PyMongoError
import pandas as pd
import logging
from typing import Optional, Dict, List, Any

# Configure logging
logger = logging.getLogger(__name__)

class AnimalShelter:
    """
    AnimalShelter class for MongoDB CRUD operations.
    
    This class provides methods to interact with the MongoDB database
    containing animal shelter data for Grazioso Salvare.
    
    Attributes:
        client (MongoClient): MongoDB client connection
        database (Database): MongoDB database reference
        collection (Collection): MongoDB collection reference
    """
    
    def __init__(self, username: str, password: str, host: str = 'localhost', 
                port: int = 27017, db_name: str = 'AAC'):
        try:
            self.client = MongoClient(
                host=host,
                port=port,
                username=username,
                password=password,
                authSource=db_name,
                authMechanism='SCRAM-SHA-1',
                serverSelectionTimeoutMS=5000
            )
            self.client.admin.command('ping')
            self.database = self.client[db_name]
            self.collection = self.database['animals']
            logger.info(f"Successfully connected to MongoDB database: {db_name}")
        except PyMongoError as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    def create(self, data: Dict[str, Any]) -> bool:
        try:
            if not data:
                raise ValueError("Cannot create document: data parameter is empty")
            result = self.collection.insert_one(data)
            logger.info(f"Document created successfully with ID: {result.inserted_id}")
            return result.acknowledged
        except PyMongoError as e:
            logger.error(f"Error creating document: {e}")
            return False
        except ValueError as e:
            logger.error(str(e))
            return False
    
    def read(self, query: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        try:
            if query is None:
                query = {}
            cursor = self.collection.find(query, {"_id": 0})
            data = pd.DataFrame(list(cursor))
            logger.info(f"Read operation returned {len(data)} records")
            return data
        except PyMongoError as e:
            logger.error(f"Error reading documents: {e}")
            return pd.DataFrame()
    
    def update(self, query: Dict[str, Any], update_data: Dict[str, Any]) -> int:
        try:
            if not query:
                raise ValueError("Update operation: query parameter cannot be empty")
            if not update_data:
                raise ValueError("Update operation: update_data parameter cannot be empty")
            result = self.collection.update_many(query, {"$set": update_data})
            logger.info(f"Updated {result.modified_count} documents")
            return result.modified_count
        except PyMongoError as e:
            logger.error(f"Error updating documents: {e}")
            return 0
        except ValueError as e:
            logger.error(str(e))
            return 0
    
    def delete(self, query: Dict[str, Any]) -> int:
        try:
            if not query:
                raise ValueError("Delete operation: query parameter cannot be empty")
            result = self.collection.delete_many(query)
            logger.info(f"Deleted {result.deleted_count} documents")
            return result.deleted_count
        except PyMongoError as e:
            logger.error(f"Error deleting documents: {e}")
            return 0
        except ValueError as e:
            logger.error(str(e))
            return 0
    
    def get_water_rescue_dogs(self) -> pd.DataFrame:
        query = {
            "animal_type": "Dog",
            "breed": {"$in": ["Labrador Retriever Mix", "Newfoundland Mix", "Portuguese Water Dog Mix"]},
            "age_upon_outcome_in_weeks": {"$gte": 26, "$lte": 156}
        }
        logger.info("Executing water rescue dogs query")
        return self.read(query)
    
    def get_mountain_rescue_dogs(self) -> pd.DataFrame:
        query = {
            "animal_type": "Dog",
            "breed": {"$in": ["German Shepherd Mix", "Alaskan Malamute Mix", 
                             "Old English Sheepdog Mix", "Siberian Husky Mix", "Rottweiler Mix"]},
            "age_upon_outcome_in_weeks": {"$gte": 26, "$lte": 156}
        }
        logger.info("Executing mountain rescue dogs query")
        return self.read(query)
    
    def get_disaster_rescue_dogs(self) -> pd.DataFrame:
        query = {
            "animal_type": "Dog",
            "breed": {"$in": ["Doberman Pinscher Mix", "German Shorthaired Pointer Mix",
                             "Bloodhound Mix", "Golden Retriever Mix"]},
            "age_upon_outcome_in_weeks": {"$gte": 20, "$lte": 300}
        }
        logger.info("Executing disaster rescue dogs query")
        return self.read(query)
    
    def get_rescue_counts(self) -> Dict[str, int]:
        counts = {
            'water': len(self.get_water_rescue_dogs()),
            'mountain': len(self.get_mountain_rescue_dogs()),
            'disaster': len(self.get_disaster_rescue_dogs()),
            'total': len(self.read({}))
        }
        logger.info(f"Rescue counts: {counts}")
        return counts
    
    def close(self) -> None:
        if hasattr(self, 'client'):
            self.client.close()
            logger.info("MongoDB connection closed")
    
    def __del__(self) -> None:
        self.close()
