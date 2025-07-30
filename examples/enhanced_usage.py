"""Enhanced usage example for the Agno Rollback system with new storage backends.

This example demonstrates:
1. Using different storage backends
2. Analytics and performance monitoring
3. Health checks and system information
4. Enterprise features with composite storage
"""

import asyncio
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent))

from agno_rollback import WorkflowManager


async def example_development_setup():
    """Example: Development setup with SQLite."""
    print("\n🔧 Development Setup Example")
    print("-" * 50)
    
    # Method 1: Explicit development setup
    manager = WorkflowManager.create_development()
    
    await manager.initialize()
    print("✅ Development manager initialized with SQLite")
    
    # Show storage info
    storage_info = manager.get_storage_info()
    print(f"   Storage type: {storage_info['storage_type']}")
    print(f"   Capabilities: {list(storage_info['capabilities'].keys())}")
    
    await manager.close()


async def example_production_setup():
    """Example: Production setup with PostgreSQL."""
    print("\n🏭 Production Setup Example")
    print("-" * 50)
    
    # Set environment for production
    os.environ.update({
        "STORAGE_BACKEND": "postgres",
        "POSTGRES_DSN": "postgresql://user:password@localhost:5432/agno_rollback"
    })
    
    try:
        manager = WorkflowManager.create_production()
        print("✅ Production manager configured with PostgreSQL")
        
        storage_info = manager.get_storage_info()
        print(f"   Storage type: {storage_info['storage_type']}")
        print(f"   High availability: {storage_info['capabilities']['high_availability']}")
        
        # Note: Don't actually initialize - requires real PostgreSQL
        print("ℹ️  (Initialization skipped - requires real PostgreSQL setup)")
        
    except ImportError:
        print("❌ Production setup requires PostgreSQL dependencies")
        print("   Install with: pip install agno-rollback[postgres]")


async def example_enterprise_setup():
    """Example: Enterprise setup with all storage backends."""
    print("\n🏢 Enterprise Setup Example")
    print("-" * 50)
    
    # Set environment for enterprise
    os.environ.update({
        "STORAGE_BACKEND": "composite",
        "POSTGRES_DSN": "postgresql://user:password@localhost:5432/agno_rollback",
        "ENABLE_CLICKHOUSE": "true",
        "CLICKHOUSE_HOST": "localhost",
        "CLICKHOUSE_DATABASE": "agno_rollback",
        "ENABLE_S3": "true",
        "S3_BUCKET_NAME": "agno-rollback-blobs",
        "AWS_ACCESS_KEY_ID": "your-key",
        "AWS_SECRET_ACCESS_KEY": "your-secret"
    })
    
    try:
        manager = WorkflowManager.create_enterprise()
        print("✅ Enterprise manager configured with composite storage")
        
        storage_info = manager.get_storage_info()
        print(f"   Primary backend: {storage_info.get('primary_backend', 'Unknown')}")
        print(f"   Secondary backends: {storage_info.get('secondary_backends', {})}")
        print(f"   Analytics enabled: {storage_info['capabilities']['analytics']}")
        print(f"   Blob storage enabled: {storage_info['capabilities']['blob_storage']}")
        
        # Note: Don't actually initialize - requires real infrastructure
        print("ℹ️  (Initialization skipped - requires real infrastructure)")
        
    except ImportError as e:
        print(f"❌ Enterprise setup requires additional dependencies: {e}")
        print("   Install with: pip install agno-rollback[enterprise]")


async def example_workflow_with_analytics():
    """Example: Running a workflow and viewing analytics."""
    print("\n📊 Workflow with Analytics Example")
    print("-" * 50)
    
    # Use SQLite for this example
    manager = WorkflowManager.create_development()
    
    try:
        await manager.initialize()
        print("✅ Manager initialized")
        
        # Start a workflow
        user_id = "analytics_user"
        query = "Latest developments in quantum computing for enterprise applications"
        
        print(f"🚀 Starting workflow: {query[:50]}...")
        task_id = await manager.start_workflow(user_id=user_id, query=query)
        print(f"   Task ID: {task_id}")
        
        # Monitor for a few seconds
        print("⏳ Monitoring workflow...")
        for i in range(10):
            await asyncio.sleep(1)
            task = await manager.get_task_status(task_id)
            if task:
                print(f"   Progress: {task.progress:.1f}% | Status: {task.status.value}")
                if task.status.value in ['completed', 'failed']:
                    break
            else:
                print("   Task not found")
                break
        
        # Try to get analytics (will be limited for SQLite)
        print("\n📈 Attempting to get analytics...")
        analytics = await manager.get_analytics()
        if analytics:
            print("✅ Analytics retrieved:")
            print(f"   Daily events: {len(analytics.get('daily_events', []))}")
            print(f"   Hourly patterns: {len(analytics.get('hourly_patterns', []))}")
        else:
            print("ℹ️  Analytics not available (requires ClickHouse backend)")
        
        # Get performance metrics
        print("\n⚡ Attempting to get performance metrics...")
        metrics = await manager.get_performance_metrics()
        if metrics:
            print("✅ Performance metrics retrieved")
        else:
            print("ℹ️  Performance metrics not available (requires ClickHouse backend)")
        
        # Health check
        print("\n🏥 Performing health check...")
        health = await manager.health_check()
        print(f"   Health status: {health}")
        
    finally:
        await manager.close()


async def example_resumption_workflow():
    """Example: Demonstrating workflow resumption with enhanced storage."""
    print("\n🔄 Enhanced Resumption Example")
    print("-" * 50)
    
    manager = WorkflowManager.create_development()
    
    try:
        await manager.initialize()
        print("✅ Manager initialized")
        
        # List resumable tasks
        resumable = await manager.list_resumable_tasks()
        print(f"📋 Found {resumable['total_resumable_tasks']} resumable tasks")
        
        if resumable['tasks']:
            # Analyze the first resumable task
            task = resumable['tasks'][0]
            task_id = task['task_id']
            
            print(f"\n🔍 Analyzing task: {task_id}")
            analysis = await manager.analyze_task(task_id)
            
            print(f"   Original query: {analysis['original_info']['query'][:50]}...")
            print(f"   Status: {analysis['original_info']['status']}")
            print(f"   Progress: {analysis['original_info']['progress']:.1f}%")
            
            # Show completion analysis
            completion = analysis['resumption_analysis']['completion_analysis']
            print("\n   Completion status:")
            for step, completed in completion.items():
                if step.startswith('_'):  # Skip internal fields
                    continue
                status = "✅" if completed else "❌"
                print(f"     {step}: {status}")
            
            # Show available resumption points
            points = analysis['resumption_analysis']['available_points']
            print(f"\n   Available resumption points: {len(points)}")
            for point in points:
                print(f"     - {point}")
            
            recommended = analysis['resumption_analysis']['recommended_point']
            if recommended:
                print(f"   Recommended: {recommended}")
                
                # Resume the workflow
                print(f"\n🚀 Resuming workflow from {recommended}...")
                try:
                    result = await manager.resume_workflow(task_id)
                    if result['success']:
                        print(f"✅ Workflow resumed successfully")
                        print(f"   New task ID: {result['new_task_id']}")
                    else:
                        print(f"❌ Resume failed: {result['error']}")
                except Exception as e:
                    print(f"❌ Resume error: {e}")
        else:
            print("ℹ️  No resumable tasks found")
            print("   Start a workflow and let it fail to see resumption in action")
    
    finally:
        await manager.close()


async def example_configuration_comparison():
    """Example: Comparing different configurations."""
    print("\n⚖️  Configuration Comparison")
    print("-" * 50)
    
    configurations = [
        ("Development", WorkflowManager.create_development),
        ("Production", WorkflowManager.create_production), 
        ("Enterprise", WorkflowManager.create_enterprise)
    ]
    
    for name, factory in configurations:
        try:
            manager = factory()
            storage_info = manager.get_storage_info()
            
            print(f"\n{name} Configuration:")
            print(f"   Storage: {storage_info['storage_type']}")
            print(f"   Analytics: {storage_info['capabilities']['analytics']}")
            print(f"   Blob Storage: {storage_info['capabilities']['blob_storage']}")
            print(f"   High Availability: {storage_info['capabilities']['high_availability']}")
            
        except Exception as e:
            print(f"\n{name} Configuration:")
            print(f"   ❌ Not available: {e}")


async def example_migration_strategy():
    """Example: Migration strategy from development to production."""
    print("\n🔄 Migration Strategy Example")
    print("-" * 50)
    
    print("Migration path from development to production:")
    print()
    print("1. Development (SQLite):")
    print("   - Local development")
    print("   - Testing and prototyping")
    print("   - Small datasets")
    print()
    print("2. Staging (PostgreSQL):")
    print("   - Team collaboration")
    print("   - Integration testing")
    print("   - ACID transactions")
    print("   - Migration: Export SQLite → Import PostgreSQL")
    print()
    print("3. Production (PostgreSQL + ClickHouse):")
    print("   - Add ClickHouse for analytics")
    print("   - Keep PostgreSQL for transactional data")
    print("   - Enable real-time monitoring")
    print()
    print("4. Enterprise (PostgreSQL + ClickHouse + S3):")
    print("   - Add S3 for blob storage")
    print("   - Large file handling")
    print("   - Cost optimization")
    print("   - Multi-region deployment")
    print()
    print("Environment variables for each stage:")
    print()
    print("Development:")
    print("   STORAGE_BACKEND=sqlite")
    print()
    print("Staging:")
    print("   STORAGE_BACKEND=postgres")
    print("   POSTGRES_DSN=postgresql://...")
    print()
    print("Production:")
    print("   STORAGE_BACKEND=composite")
    print("   POSTGRES_DSN=postgresql://...")
    print("   ENABLE_CLICKHOUSE=true")
    print("   CLICKHOUSE_HOST=...")
    print()
    print("Enterprise:")
    print("   STORAGE_BACKEND=composite")
    print("   POSTGRES_DSN=postgresql://...")
    print("   ENABLE_CLICKHOUSE=true")
    print("   ENABLE_S3=true")
    print("   S3_BUCKET_NAME=...")


async def main():
    """Main example function."""
    print("🚀 Agno Rollback Enhanced Usage Examples")
    print("=" * 60)
    
    await example_development_setup()
    await example_production_setup()
    await example_enterprise_setup()
    await example_workflow_with_analytics()
    await example_resumption_workflow()
    await example_configuration_comparison()
    await example_migration_strategy()
    
    print("\n✅ Enhanced examples completed!")
    print("\n📚 Key Takeaways:")
    print("1. Choose storage backend based on your scale and requirements")
    print("2. SQLite for development, PostgreSQL for production, Composite for enterprise")
    print("3. Analytics require ClickHouse, blob storage requires S3")
    print("4. Use factory methods for easy configuration")
    print("5. Health checks and monitoring are built-in")
    print("\n🔗 Next Steps:")
    print("1. Install appropriate dependencies for your chosen backend")
    print("2. Set up environment variables for your infrastructure")
    print("3. Test the configuration in a development environment")
    print("4. Plan your migration strategy for production deployment")


if __name__ == "__main__":
    asyncio.run(main()) 