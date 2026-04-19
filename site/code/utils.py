"""
Utility Functions Module
CS 499 Capstone Project - Software Design and Engineering Enhancement

This module provides utility functions for the Grazioso Salvare Dashboard,
including data validation, formatting, and helper functions.

Author: Steven Foltz
Course: CS 499 Capstone
Date: March 22, 2026
"""

import logging
import re
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)


def validate_animal_data(data: Dict[str, Any]) -> tuple:
    try:
        required_fields = ['name', 'animal_type', 'breed']
        for field in required_fields:
            if field not in data or not data[field]:
                return False, f"Missing required field: {field}"
        if not re.match(r'^[A-Za-z\s\-]+$', data['name']):
            return False, "Name must contain only letters, spaces, and hyphens"
        valid_types = ['Dog', 'Cat', 'Bird', 'Rabbit', 'Other']
        if data['animal_type'] not in valid_types:
            return False, f"Invalid animal type. Must be one of: {valid_types}"
        if 'age_upon_outcome_in_weeks' in data:
            age = data['age_upon_outcome_in_weeks']
            if not isinstance(age, (int, float)) or age < 0 or age > 1000:
                return False, "Age must be a positive number between 0 and 1000 weeks"
        if 'location_lat' in data:
            lat = data['location_lat']
            if not isinstance(lat, (int, float)) or lat < -90 or lat > 90:
                return False, "Latitude must be between -90 and 90"
        if 'location_long' in data:
            lon = data['location_long']
            if not isinstance(lon, (int, float)) or lon < -180 or lon > 180:
                return False, "Longitude must be between -180 and 180"
        return True, None
    except Exception as e:
        logger.error(f"Error validating animal data: {e}")
        return False, f"Validation error: {str(e)}"


def format_animal_record(record: Dict[str, Any]) -> Dict[str, Any]:
    formatted = record.copy()
    if 'age_upon_outcome_in_weeks' in formatted:
        weeks = formatted['age_upon_outcome_in_weeks']
        if weeks < 52:
            formatted['age_display'] = f"{weeks} weeks"
        elif weeks < 208:
            formatted['age_display'] = f"{weeks / 52:.1f} years"
        else:
            formatted['age_display'] = f"{weeks / 52:.0f} years"
    if 'name' in formatted and formatted['name']:
        formatted['name_display'] = formatted['name'].title()
    if 'breed' in formatted:
        formatted['rescue_types'] = classify_rescue_type(formatted)
    return formatted


def classify_rescue_type(record: Dict[str, Any]) -> List[str]:
    rescue_types = []
    if record.get('animal_type') != 'Dog':
        return []
    breed = record.get('breed', '').lower()
    age = record.get('age_upon_outcome_in_weeks', 0)
    water_breeds = ['labrador', 'newfoundland', 'portuguese water dog']
    if any(b in breed for b in water_breeds) and 26 <= age <= 156:
        rescue_types.append('Water Rescue')
    mountain_breeds = ['german shepherd', 'alaskan malamute', 'old english sheepdog',
                       'siberian husky', 'rottweiler']
    if any(b in breed for b in mountain_breeds) and 26 <= age <= 156:
        rescue_types.append('Mountain Rescue')
    disaster_breeds = ['doberman', 'german shorthaired pointer', 'bloodhound', 'golden retriever']
    if any(b in breed for b in disaster_breeds) and 20 <= age <= 300:
        rescue_types.append('Disaster Rescue')
    return rescue_types


def get_rescue_summary(data: List[Dict[str, Any]]) -> Dict[str, int]:
    summary = {'Water Rescue': 0, 'Mountain Rescue': 0, 'Disaster Rescue': 0, 'Total Animals': len(data)}
    for record in data:
        for t in classify_rescue_type(record):
            summary[t] += 1
    return summary


def sanitize_input(input_string: str) -> str:
    if not input_string:
        return ""
    sanitized = re.sub(r'[<>$;]', '', input_string)
    return sanitized.strip()


def log_operation(operation: str, details: Dict[str, Any]) -> None:
    logger.info(f"OPERATION: {operation} | DETAILS: {details}")


def format_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_data_statistics(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    stats = {
        'total': len(data),
        'by_type': {},
        'by_outcome': {},
        'age_range': {'min': None, 'max': None, 'avg': None},
        'top_breeds': {}
    }
    if not data:
        return stats
    for record in data:
        animal_type = record.get('animal_type', 'Unknown')
        stats['by_type'][animal_type] = stats['by_type'].get(animal_type, 0) + 1
        outcome = record.get('outcome_type', 'Unknown')
        stats['by_outcome'][outcome] = stats['by_outcome'].get(outcome, 0) + 1
    ages = [record.get('age_upon_outcome_in_weeks') for record in data
            if record.get('age_upon_outcome_in_weeks') is not None
            and isinstance(record.get('age_upon_outcome_in_weeks'), (int, float))]
    if ages:
        stats['age_range']['min'] = min(ages)
        stats['age_range']['max'] = max(ages)
        stats['age_range']['avg'] = sum(ages) / len(ages)
    breed_counts = {}
    for record in data:
        breed = record.get('breed', 'Unknown')
        breed_counts[breed] = breed_counts.get(breed, 0) + 1
    stats['top_breeds'] = dict(sorted(breed_counts.items(), key=lambda x: x[1], reverse=True)[:10])
    return stats
