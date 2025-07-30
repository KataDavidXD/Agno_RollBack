"""Migration helper for Agno Rollback storage backends.

This script helps migrate data from SQLite to PostgreSQL or composite storage.
It provides tools for:
1. Exporting data from SQLite
2. Importing data to PostgreSQL
3. Validating data integrity
4. Testing new storage configuration
"""

import asyncio
import json
import sys
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
import argparse

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent))

from agno_rollback.storage import (
    SQLiteStorage,
    PostgresStorage,
    CompositeStorage,
    create_storage_backend
)
from agno_rollback.core.models import WorkflowTask, TaskStatus


class MigrationHelper:
    """Helper class for migrating between storage backends."""
    
    def __init__(self):
        self.source_storage = None
        self.target_storage = None
    
    async def export_from_sqlite(
        self, 
        sqlite_path: str,
        export_file: str = "agno_rollback_export.json"
    ) -> Dict[str, Any]:
        """Export all data from SQLite storage.
        
        Args:
            sqlite_path: Path to SQLite database
            export_file: File to export data to
            
        Returns:
            Export summary
        """
        print(f"📤 Exporting data from SQLite: {sqlite_path}")
        
        self.source_storage = SQLiteStorage(db_path=sqlite_path)
        await self.source_storage.initialize()
        
        try:
            # Export all tasks
            all_tasks = await self.source_storage.list_tasks(limit=10000)
            print(f"   Found {len(all_tasks)} tasks")
            
            # Export workflow states
            workflow_states = []
            for task in all_tasks:
                state = await self.source_storage.get_workflow_state(task.task_id)
                if state:
                    workflow_states.append({
                        "task_id": str(state.task_id),
                        "last_checkpoint": state.last_checkpoint,
                        "checkpoint_data": state.checkpoint_data,
                        "available_resumption_points": [p.value for p in state.available_resumption_points],
                        "recommended_resumption_point": state.recommended_resumption_point.value if state.recommended_resumption_point else None,
                        "web_retrieval_completed": state.web_retrieval_completed,
                        "news_retrieval_completed": state.news_retrieval_completed,
                        "summarization_started": state.summarization_started,
                        "summarization_completed": state.summarization_completed
                    })
            
            print(f"   Found {len(workflow_states)} workflow states")
            
            # Prepare export data
            export_data = {
                "export_metadata": {
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": "sqlite",
                    "source_path": sqlite_path,
                    "agno_rollback_version": "2.2.1"
                },
                "tasks": [
                    {
                        "task_id": str(task.task_id),
                        "user_id": task.user_id,
                        "session_id": task.session_id,
                        "query": task.query,
                        "status": task.status.value,
                        "created_at": task.created_at.isoformat() if task.created_at else None,
                        "started_at": task.started_at.isoformat() if task.started_at else None,
                        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
                        "progress": task.progress,
                        "result": task.result,
                        "error": task.error,
                        "current_step": task.current_step,
                        "workflow_events": [e.to_dict() for e in task.workflow_events],
                        "agent_outputs": [o.to_dict() for o in task.agent_outputs],
                        "tool_results": [t.to_dict() for t in task.tool_results],
                        "step_results": [s.to_dict() for s in task.step_results],
                        "web_retrieval_results": task.web_retrieval_results,
                        "news_retrieval_results": task.news_retrieval_results
                    }
                    for task in all_tasks
                ],
                "workflow_states": workflow_states
            }
            
            # Write to file
            with open(export_file, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Export completed: {export_file}")
            print(f"   Exported {len(all_tasks)} tasks")
            print(f"   Exported {len(workflow_states)} workflow states")
            
            return {
                "success": True,
                "export_file": export_file,
                "tasks_count": len(all_tasks),
                "states_count": len(workflow_states)
            }
            
        finally:
            await self.source_storage.close()
    
    async def import_to_postgres(
        self,
        export_file: str,
        postgres_dsn: str,
        dry_run: bool = True
    ) -> Dict[str, Any]:
        """Import data to PostgreSQL storage.
        
        Args:
            export_file: File containing exported data
            postgres_dsn: PostgreSQL connection string
            dry_run: If True, don't actually import data
            
        Returns:
            Import summary
        """
        print(f"📥 Importing data to PostgreSQL (dry_run={dry_run})")
        
        # Load export data
        with open(export_file, 'r', encoding='utf-8') as f:
            export_data = json.load(f)
        
        tasks_data = export_data['tasks']
        states_data = export_data['workflow_states']
        
        print(f"   Loaded {len(tasks_data)} tasks and {len(states_data)} states")
        
        if dry_run:
            print("🔍 DRY RUN - No data will be imported")
            return {
                "success": True,
                "dry_run": True,
                "tasks_count": len(tasks_data),
                "states_count": len(states_data)
            }
        
        # Initialize PostgreSQL storage
        self.target_storage = PostgresStorage(dsn=postgres_dsn)
        await self.target_storage.initialize()
        
        try:
            imported_tasks = 0
            imported_states = 0
            errors = []
            
            # Import tasks
            for task_data in tasks_data:
                try:
                    # Reconstruct task object
                    from agno_rollback.core.models import (
                        WorkflowEvent, AgentOutput, ToolResult, StepResult
                    )
                    
                    # Parse datetime fields
                    created_at = datetime.fromisoformat(task_data['created_at']) if task_data['created_at'] else None
                    started_at = datetime.fromisoformat(task_data['started_at']) if task_data['started_at'] else None
                    completed_at = datetime.fromisoformat(task_data['completed_at']) if task_data['completed_at'] else None
                    
                    # Reconstruct complex fields
                    workflow_events = [WorkflowEvent.from_dict(e) for e in task_data['workflow_events']]
                    agent_outputs = [AgentOutput.from_dict(o) for o in task_data['agent_outputs']]
                    tool_results = [ToolResult.from_dict(t) for t in task_data['tool_results']]
                    step_results = [StepResult.from_dict(s) for s in task_data['step_results']]
                    
                    task = WorkflowTask(
                        task_id=task_data['task_id'],
                        user_id=task_data['user_id'],
                        session_id=task_data['session_id'],
                        query=task_data['query'],
                        status=TaskStatus(task_data['status']),
                        created_at=created_at,
                        started_at=started_at,
                        completed_at=completed_at,
                        progress=task_data['progress'],
                        result=task_data['result'],
                        error=task_data['error'],
                        current_step=task_data['current_step'],
                        workflow_events=workflow_events,
                        agent_outputs=agent_outputs,
                        tool_results=tool_results,
                        step_results=step_results,
                        web_retrieval_results=task_data['web_retrieval_results'],
                        news_retrieval_results=task_data['news_retrieval_results']
                    )
                    
                    await self.target_storage.create_task(task)
                    imported_tasks += 1
                    
                except Exception as e:
                    errors.append(f"Task {task_data['task_id']}: {e}")
            
            # Import workflow states
            for state_data in states_data:
                try:
                    from agno_rollback.core.models import WorkflowState, ResumptionPoint
                    from uuid import UUID
                    
                    state = WorkflowState(
                        task_id=UUID(state_data['task_id']),
                        last_checkpoint=state_data['last_checkpoint']
                    )
                    state.checkpoint_data = state_data['checkpoint_data']
                    state.available_resumption_points = [
                        ResumptionPoint(p) for p in state_data['available_resumption_points']
                    ]
                    state.recommended_resumption_point = (
                        ResumptionPoint(state_data['recommended_resumption_point'])
                        if state_data['recommended_resumption_point'] else None
                    )
                    state.web_retrieval_completed = state_data['web_retrieval_completed']
                    state.news_retrieval_completed = state_data['news_retrieval_completed']
                    state.summarization_started = state_data['summarization_started']
                    state.summarization_completed = state_data['summarization_completed']
                    
                    await self.target_storage.save_workflow_state(state)
                    imported_states += 1
                    
                except Exception as e:
                    errors.append(f"State {state_data['task_id']}: {e}")
            
            print(f"✅ Import completed")
            print(f"   Imported {imported_tasks}/{len(tasks_data)} tasks")
            print(f"   Imported {imported_states}/{len(states_data)} states")
            
            if errors:
                print(f"⚠️  {len(errors)} errors occurred:")
                for error in errors[:5]:  # Show first 5 errors
                    print(f"   - {error}")
                if len(errors) > 5:
                    print(f"   ... and {len(errors) - 5} more")
            
            return {
                "success": True,
                "dry_run": False,
                "imported_tasks": imported_tasks,
                "imported_states": imported_states,
                "total_tasks": len(tasks_data),
                "total_states": len(states_data),
                "errors": errors
            }
            
        finally:
            await self.target_storage.close()
    
    async def validate_migration(
        self,
        export_file: str,
        target_backend_type: str = "postgres"
    ) -> Dict[str, Any]:
        """Validate that migration was successful.
        
        Args:
            export_file: File containing original exported data
            target_backend_type: Type of target storage backend
            
        Returns:
            Validation results
        """
        print(f"🔍 Validating migration to {target_backend_type}")
        
        # Load original data
        with open(export_file, 'r', encoding='utf-8') as f:
            export_data = json.load(f)
        
        original_tasks = export_data['tasks']
        original_states = export_data['workflow_states']
        
        # Initialize target storage
        self.target_storage = create_storage_backend(target_backend_type)
        await self.target_storage.initialize()
        
        try:
            # Check tasks
            found_tasks = 0
            missing_tasks = []
            
            for original_task in original_tasks:
                from uuid import UUID
                task_id = UUID(original_task['task_id'])
                task = await self.target_storage.get_task(task_id)
                
                if task:
                    found_tasks += 1
                else:
                    missing_tasks.append(str(task_id))
            
            # Check workflow states
            found_states = 0
            missing_states = []
            
            for original_state in original_states:
                from uuid import UUID
                task_id = UUID(original_state['task_id'])
                state = await self.target_storage.get_workflow_state(task_id)
                
                if state:
                    found_states += 1
                else:
                    missing_states.append(str(task_id))
            
            tasks_match = found_tasks == len(original_tasks)
            states_match = found_states == len(original_states)
            migration_successful = tasks_match and states_match
            
            print(f"📊 Validation Results:")
            print(f"   Tasks: {found_tasks}/{len(original_tasks)} {'✅' if tasks_match else '❌'}")
            print(f"   States: {found_states}/{len(original_states)} {'✅' if states_match else '❌'}")
            print(f"   Overall: {'✅ Migration successful' if migration_successful else '❌ Migration incomplete'}")
            
            if missing_tasks:
                print(f"   Missing tasks: {len(missing_tasks)}")
            if missing_states:
                print(f"   Missing states: {len(missing_states)}")
            
            return {
                "success": migration_successful,
                "tasks_found": found_tasks,
                "tasks_total": len(original_tasks),
                "states_found": found_states,
                "states_total": len(original_states),
                "missing_tasks": missing_tasks,
                "missing_states": missing_states
            }
            
        finally:
            await self.target_storage.close()
    
    async def test_new_configuration(
        self,
        backend_type: str = "postgres"
    ) -> Dict[str, Any]:
        """Test the new storage configuration.
        
        Args:
            backend_type: Type of storage backend to test
            
        Returns:
            Test results
        """
        print(f"🧪 Testing {backend_type} storage configuration")
        
        try:
            # Create and initialize storage
            storage = create_storage_backend(backend_type)
            await storage.initialize()
            
            # Test basic operations
            test_results = {
                "initialization": True,
                "task_listing": False,
                "health_check": False,
                "capabilities": {}
            }
            
            # Test task listing
            try:
                tasks = await storage.list_tasks(limit=1)
                test_results["task_listing"] = True
            except Exception as e:
                print(f"   ❌ Task listing failed: {e}")
            
            # Test health check (if available)
            if hasattr(storage, 'health_check'):
                try:
                    health = await storage.health_check()
                    test_results["health_check"] = True
                    test_results["health_status"] = health
                except Exception as e:
                    print(f"   ❌ Health check failed: {e}")
            
            # Get capabilities (if available)
            if hasattr(storage, 'get_backend_info'):
                try:
                    info = storage.get_backend_info()
                    test_results["capabilities"] = info.get("capabilities", {})
                except Exception as e:
                    print(f"   ❌ Capabilities check failed: {e}")
            
            await storage.close()
            
            # Summary
            all_tests_passed = all([
                test_results["initialization"],
                test_results["task_listing"]
            ])
            
            print(f"   Initialization: {'✅' if test_results['initialization'] else '❌'}")
            print(f"   Task listing: {'✅' if test_results['task_listing'] else '❌'}")
            print(f"   Health check: {'✅' if test_results['health_check'] else '⚪'}")
            print(f"   Overall: {'✅ Configuration working' if all_tests_passed else '❌ Configuration issues'}")
            
            test_results["success"] = all_tests_passed
            return test_results
            
        except Exception as e:
            print(f"   ❌ Configuration test failed: {e}")
            return {
                "success": False,
                "error": str(e)
            }


async def main():
    """Main CLI interface for migration helper."""
    parser = argparse.ArgumentParser(
        description="Agno Rollback Storage Migration Helper"
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export data from SQLite')
    export_parser.add_argument('sqlite_path', help='Path to SQLite database')
    export_parser.add_argument('--output', '-o', default='agno_rollback_export.json',
                             help='Output file for export')
    
    # Import command
    import_parser = subparsers.add_parser('import', help='Import data to PostgreSQL')
    import_parser.add_argument('export_file', help='Export file to import')
    import_parser.add_argument('postgres_dsn', help='PostgreSQL connection string')
    import_parser.add_argument('--dry-run', action='store_true',
                             help='Perform dry run without importing')
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate migration')
    validate_parser.add_argument('export_file', help='Original export file')
    validate_parser.add_argument('--backend', default='postgres',
                               choices=['postgres', 'composite'],
                               help='Target backend type')
    
    # Test command
    test_parser = subparsers.add_parser('test', help='Test storage configuration')
    test_parser.add_argument('--backend', default='postgres',
                           choices=['sqlite', 'postgres', 'composite'],
                           help='Backend type to test')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    helper = MigrationHelper()
    
    try:
        if args.command == 'export':
            result = await helper.export_from_sqlite(args.sqlite_path, args.output)
            
        elif args.command == 'import':
            result = await helper.import_to_postgres(
                args.export_file, args.postgres_dsn, args.dry_run
            )
            
        elif args.command == 'validate':
            result = await helper.validate_migration(args.export_file, args.backend)
            
        elif args.command == 'test':
            result = await helper.test_new_configuration(args.backend)
        
        print(f"\n📋 Operation completed: {result.get('success', False)}")
        
    except KeyboardInterrupt:
        print("\n⏹️  Operation cancelled by user")
    except Exception as e:
        print(f"\n❌ Operation failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main()) 