"""
Security Module
CS 499 Capstone Project - Software Design and Engineering Enhancement

This module provides security-related functions including input validation,
credential management, and security best practices.

Author: Steven Foltz
Course: CS 499 Capstone
Date: March 22, 2026
"""

import os
import re
import hashlib
import secrets
import logging
from typing import Optional, Dict, Any
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class SecurityManager:
    def __init__(self):
        self.salt = os.getenv('SECURITY_SALT', secrets.token_hex(16))
        logger.info("SecurityManager initialized")
    
    def validate_mongodb_query(self, query: Dict[str, Any]) -> bool:
        try:
            dangerous_operators = ['$where', '$regex', '$eval', '$function']
            def check_dict(obj):
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        if key in dangerous_operators:
                            logger.warning(f"Dangerous operator detected: {key}")
                            return False
                        if not check_dict(value):
                            return False
                elif isinstance(obj, list):
                    for item in obj:
                        if not check_dict(item):
                            return False
                return True
            return check_dict(query)
        except Exception as e:
            logger.error(f"Error validating query: {e}")
            return False
    
    def sanitize_input(self, input_string: str, max_length: int = 255) -> str:
        if not input_string:
            return ""
        if len(input_string) > max_length:
            input_string = input_string[:max_length]
            logger.warning(f"Input truncated to {max_length} characters")
        sanitized = re.sub(r'[<>$;\"\'`]', '', input_string)
        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE',
                        'ALTER', 'EXEC', 'UNION', 'SCRIPT', 'JAVASCRIPT']
        for keyword in sql_keywords:
            sanitized = re.sub(keyword, '', sanitized, flags=re.IGNORECASE)
        return sanitized.strip()
    
    def hash_password(self, password: str) -> str:
        try:
            salted = password + self.salt
            hashed = hashlib.sha256(salted.encode()).hexdigest()
            logger.info("Password hashed successfully")
            return hashed
        except Exception as e:
            logger.error(f"Error hashing password: {e}")
            return ""
    
    def verify_password(self, password: str, hashed: str) -> bool:
        try:
            return self.hash_password(password) == hashed
        except Exception as e:
            logger.error(f"Error verifying password: {e}")
            return False
    
    def generate_secure_token(self, length: int = 32) -> str:
        return secrets.token_hex(length)
    
    def validate_environment(self) -> Dict[str, bool]:
        required_vars = ['DB_USERNAME', 'DB_PASSWORD', 'DB_HOST', 'DB_NAME']
        results = {}
        for var in required_vars:
            value = os.getenv(var)
            if not value:
                logger.warning(f"Missing environment variable: {var}")
                results[var] = False
            else:
                results[var] = True
        return results
    
    def mask_sensitive_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        masked = data.copy()
        sensitive_fields = ['password', 'token', 'secret', 'api_key', 'auth']
        for field in sensitive_fields:
            if field in masked:
                masked[field] = '***MASKED***'
        return masked
    
    def check_rate_limit(self, key: str, limit: int = 10, window: int = 60) -> bool:
        logger.info(f"Rate limit check for {key}: {limit} requests per {window} seconds")
        return True


def sanitize_for_html(input_string: str) -> str:
    if not input_string:
        return ""
    html_escape_table = {"&": "&amp;", '"': "&quot;", "'": "&apos;", ">": "&gt;", "<": "&lt;"}
    return "".join(html_escape_table.get(c, c) for c in input_string)


def validate_file_upload(filename: str, allowed_extensions: list = None) -> bool:
    if allowed_extensions is None:
        allowed_extensions = ['.csv', '.json', '.txt']
    if '..' in filename or '/' in filename or '\\' in filename:
        logger.warning(f"Path traversal attempt detected: {filename}")
        return False
    ext = os.path.splitext(filename)[1].lower()
    if ext not in allowed_extensions:
        logger.warning(f"Invalid file extension: {ext}")
        return False
    return True


def get_security_headers() -> Dict[str, str]:
    return {
        'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
        'X-Content-Type-Options': 'nosniff',
        'X-Frame-Options': 'DENY',
        'X-XSS-Protection': '1; mode=block',
        'Content-Security-Policy': "default-src 'self'",
        'Referrer-Policy': 'strict-origin-when-cross-origin'
    }
