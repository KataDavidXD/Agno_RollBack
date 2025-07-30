"""Storage module for the workflow resumption system.

This module provides:
- Abstract storage interface
- Multiple storage backend implementations
- Session management utilities
- Composite storage for multi-backend operations
"""

import os
from typing import Optional

from .base import StorageBackend
from .sqlite import SQLiteStorage

# Import new backends with graceful fallbacks
try:
    from .postgres import PostgresStorage
except ImportError:
    PostgresStorage = None

try:
    from .clickhouse import ClickHouseStorage
except ImportError:
    ClickHouseStorage = None

try:
    from .s3 import S3Storage
except ImportError:
    S3Storage = None

try:
    from .composite import CompositeStorage
except ImportError:
    CompositeStorage = None


def create_storage_backend(backend_type: Optional[str] = None) -> StorageBackend:
    """Create a storage backend based on configuration.
    
    Args:
        backend_type: Type of backend ("sqlite", "postgres", "composite", or None for auto-detect)
        
    Returns:
        Configured storage backend
        
    Environment Variables:
        STORAGE_BACKEND: Backend type (sqlite, postgres, composite)
        POSTGRES_DSN: PostgreSQL connection string (for postgres/composite backends)
        ENABLE_CLICKHOUSE: Enable ClickHouse analytics (for composite backend)
        ENABLE_S3: Enable S3 blob storage (for composite backend)
    """
    # Auto-detect backend type from environment if not specified
    if backend_type is None:
        backend_type = os.getenv("STORAGE_BACKEND", "sqlite").lower()
    
    if backend_type == "sqlite":
        # Default SQLite backend for development
        db_path = os.getenv("SQLITE_DB_PATH", "data/workflow_storage.db")
        return SQLiteStorage(db_path=db_path)
    
    elif backend_type == "postgres":
        # PostgreSQL backend for production
        if PostgresStorage is None:
            raise ImportError(
                "PostgreSQL backend requires asyncpg. "
                "Install it with: pip install asyncpg"
            )
        
        postgres_dsn = os.getenv("POSTGRES_DSN")
        if not postgres_dsn:
            raise ValueError(
                "POSTGRES_DSN environment variable is required for PostgreSQL backend"
            )
        
        return PostgresStorage(dsn=postgres_dsn)
    
    elif backend_type == "composite":
        # Composite backend for full-scale production
        if CompositeStorage is None:
            raise ImportError(
                "Composite backend requires additional dependencies. "
                "Install them with: pip install asyncpg clickhouse-connect aioboto3"
            )
        
        return CompositeStorage.from_environment()
    
    else:
        raise ValueError(
            f"Unknown backend type: {backend_type}. "
            f"Supported types: sqlite, postgres, composite"
        )


# Legacy function for backward compatibility
def get_default_storage() -> StorageBackend:
    """Get the default storage backend.
    
    This function is deprecated. Use create_storage_backend() instead.
    """
    return create_storage_backend()


__all__ = [
    "StorageBackend",
    "SQLiteStorage",
    "PostgresStorage",
    "ClickHouseStorage", 
    "S3Storage",
    "CompositeStorage",
    "create_storage_backend",
    "get_default_storage",
]