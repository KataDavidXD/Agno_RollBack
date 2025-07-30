"""Storage configuration examples for the Agno Rollback system.

This example demonstrates how to configure and use different storage backends:
1. SQLite (development)
2. PostgreSQL (production OLTP)
3. Composite (production with ClickHouse + S3)
"""

import asyncio
import os
from pathlib import Path
import sys

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent))

from agno_rollback.storage import (
    create_storage_backend,
    SQLiteStorage,
    PostgresStorage,
    CompositeStorage
)


async def example_sqlite_storage():
    """Example: SQLite storage for development."""
    print("\n🗃️  SQLite Storage Example")
    print("-" * 50)
    
    # Method 1: Direct instantiation
    storage = SQLiteStorage(db_path="data/example.db")
    
    # Method 2: Using factory with environment
    os.environ["STORAGE_BACKEND"] = "sqlite"
    os.environ["SQLITE_DB_PATH"] = "data/example_env.db"
    storage_env = create_storage_backend()
    
    print("✅ SQLite storage configured")
    print(f"   Database path: {storage.db_path}")
    print(f"   Environment-based path: {storage_env.db_path}")
    
    # Test basic operations
    await storage.initialize()
    print("✅ SQLite storage initialized")
    await storage.close()


async def example_postgres_storage():
    """Example: PostgreSQL storage for production OLTP."""
    print("\n🐘 PostgreSQL Storage Example")
    print("-" * 50)
    
    # Check if PostgreSQL is available
    try:
        # Example configuration (adjust for your setup)
        postgres_dsn = "postgresql://user:password@localhost:5432/agno_rollback"
        
        # Method 1: Direct instantiation
        storage = PostgresStorage(dsn=postgres_dsn)
        
        # Method 2: Using factory with environment
        os.environ["STORAGE_BACKEND"] = "postgres"
        os.environ["POSTGRES_DSN"] = postgres_dsn
        storage_env = create_storage_backend("postgres")
        
        print("✅ PostgreSQL storage configured")
        print(f"   Connection: {postgres_dsn}")
        print("   Features: ACID transactions, JSONB, full-text search")
        
        # Note: Don't actually connect in this example
        print("ℹ️  (Connection test skipped - adjust DSN for your setup)")
        
    except ImportError:
        print("❌ PostgreSQL storage requires asyncpg")
        print("   Install with: pip install asyncpg")


async def example_composite_storage():
    """Example: Composite storage for full production setup."""
    print("\n🏗️  Composite Storage Example")
    print("-" * 50)
    
    try:
        # Example environment configuration
        os.environ.update({
            "STORAGE_BACKEND": "composite",
            
            # PostgreSQL (required)
            "POSTGRES_DSN": "postgresql://user:password@localhost:5432/agno_rollback",
            
            # ClickHouse (optional)
            "ENABLE_CLICKHOUSE": "true",
            "CLICKHOUSE_HOST": "localhost",
            "CLICKHOUSE_PORT": "8123",
            "CLICKHOUSE_DATABASE": "agno_rollback",
            "CLICKHOUSE_USERNAME": "default",
            "CLICKHOUSE_PASSWORD": "",
            
            # S3 (optional)
            "ENABLE_S3": "true",
            "S3_BUCKET_NAME": "agno-rollback-blobs",
            "AWS_ACCESS_KEY_ID": "your-access-key",
            "AWS_SECRET_ACCESS_KEY": "your-secret-key",
            "AWS_DEFAULT_REGION": "us-east-1",
            # "S3_ENDPOINT_URL": "http://localhost:9000",  # For MinIO
        })
        
        # Create composite storage from environment
        storage = CompositeStorage.from_environment()
        
        print("✅ Composite storage configured")
        print("   Primary: PostgreSQL (OLTP)")
        print("   Analytics: ClickHouse (OLAP)")
        print("   Blobs: S3 (Object Storage)")
        
        # Show backend information
        backend_info = storage.get_backend_info()
        print(f"   Capabilities: {list(backend_info['capabilities'].keys())}")
        
        # Note: Don't actually connect in this example
        print("ℹ️  (Connection test skipped - adjust configuration for your setup)")
        
    except ImportError as e:
        print(f"❌ Composite storage requires additional dependencies: {e}")
        print("   Install with: pip install asyncpg clickhouse-connect aioboto3")


async def example_configuration_patterns():
    """Example: Different configuration patterns."""
    print("\n⚙️  Configuration Patterns")
    print("-" * 50)
    
    # Pattern 1: Development (SQLite only)
    print("1. Development Setup:")
    print("   STORAGE_BACKEND=sqlite")
    print("   SQLITE_DB_PATH=data/dev.db")
    
    # Pattern 2: Production with PostgreSQL only
    print("\n2. Production Setup (PostgreSQL only):")
    print("   STORAGE_BACKEND=postgres")
    print("   POSTGRES_DSN=postgresql://user:pass@host:5432/db")
    
    # Pattern 3: Full production with all backends
    print("\n3. Full Production Setup (All backends):")
    print("   STORAGE_BACKEND=composite")
    print("   POSTGRES_DSN=postgresql://user:pass@pg-host:5432/db")
    print("   ENABLE_CLICKHOUSE=true")
    print("   CLICKHOUSE_HOST=ch-host")
    print("   ENABLE_S3=true")
    print("   S3_BUCKET_NAME=my-bucket")
    
    # Pattern 4: Gradual migration
    print("\n4. Gradual Migration (PostgreSQL + ClickHouse):")
    print("   STORAGE_BACKEND=composite")
    print("   POSTGRES_DSN=postgresql://user:pass@host:5432/db")
    print("   ENABLE_CLICKHOUSE=true")
    print("   ENABLE_S3=false")


async def example_usage_patterns():
    """Example: Different usage patterns for the storage backends."""
    print("\n📋 Usage Patterns")
    print("-" * 50)
    
    print("🔹 When to use SQLite:")
    print("   - Local development")
    print("   - Small deployments (<1000 tasks/day)")
    print("   - Prototyping and testing")
    
    print("\n🔹 When to use PostgreSQL:")
    print("   - Production deployments")
    print("   - Need ACID transactions")
    print("   - Team collaboration")
    print("   - Moderate scale (1000-10000 tasks/day)")
    
    print("\n🔹 When to use Composite:")
    print("   - Large scale production (10000+ tasks/day)")
    print("   - Need analytics and monitoring")
    print("   - Large file/blob storage")
    print("   - Multiple data access patterns")
    
    print("\n🔹 Cost Optimization:")
    print("   - PostgreSQL: OLTP operations ($$$)")
    print("   - ClickHouse: Analytics queries ($$)")
    print("   - S3: Blob storage ($)")


async def example_health_monitoring():
    """Example: Health monitoring for composite storage."""
    print("\n🏥 Health Monitoring Example")
    print("-" * 50)
    
    # Example health check for composite storage
    print("Health check example for composite storage:")
    print("""
async def monitor_storage_health():
    storage = create_storage_backend("composite")
    await storage.initialize()
    
    # Check health of all backends
    health = await storage.health_check()
    
    for backend, status in health.items():
        if status["status"] == "healthy":
            print(f"✅ {backend}: Healthy")
        elif status["status"] == "disabled":
            print(f"⚪ {backend}: Disabled")
        else:
            print(f"❌ {backend}: {status['error']}")
    
    await storage.close()
""")


async def main():
    """Main example function."""
    print("🚀 Agno Rollback Storage Configuration Examples")
    print("=" * 60)
    
    await example_sqlite_storage()
    await example_postgres_storage()
    await example_composite_storage()
    await example_configuration_patterns()
    await example_usage_patterns()
    await example_health_monitoring()
    
    print("\n✅ Examples completed!")
    print("\nNext steps:")
    print("1. Choose your storage backend based on your needs")
    print("2. Set up the required environment variables")
    print("3. Install the necessary dependencies")
    print("4. Test the configuration with your application")


if __name__ == "__main__":
    asyncio.run(main()) 