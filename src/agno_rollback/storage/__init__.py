"""Storage module for the workflow resumption system.

This module provides:
- Abstract storage interface
- Multiple storage backend implementations
- Session management utilities
"""

from .base import StorageBackend
from .sqlite import SQLiteStorage

__all__ = [
    "StorageBackend",
    "SQLiteStorage",
]