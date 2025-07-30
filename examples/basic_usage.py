"""Basic usage example for the Agno Rollback system.

This example demonstrates:
1. Starting a new workflow
2. Monitoring progress
3. Handling failures
4. Resuming workflows
"""

import asyncio
import sys
from pathlib import Path

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent))

from agno_rollback import WorkflowManager, TaskStatus


async def main():
    """Main example function."""
    # Create workflow manager
    manager = WorkflowManager()
    
    try:
        # Initialize manager
        await manager.initialize()
        print("✅ Workflow manager initialized")
        
        # Example 1: Start a new workflow
        print("\n📋 Example 1: Starting a new workflow")
        print("-" * 50)
        
        user_id = "demo_user"
        query = "What are the latest developments in AI and machine learning?"
        
        task_id = await manager.start_workflow(
            user_id=user_id,
            query=query
        )
        
        print(f"✨ Started workflow with task ID: {task_id}")
        
        # Monitor progress
        print("\n⏳ Monitoring progress...")
        for i in range(30):  # Monitor for up to 30 seconds
            await asyncio.sleep(1)
            
            task = await manager.get_task_status(task_id)
            if task:
                print(f"   Status: {task.status.value} | Progress: {task.progress:.1f}%", end="\r")
                
                if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                    print()  # New line
                    break
        
        # Check final status
        final_task = await manager.get_task_status(task_id)
        if final_task:
            if final_task.status == TaskStatus.COMPLETED:
                print(f"✅ Workflow completed successfully!")
                print(f"\n📄 Full Summary:")
                print("-" * 50)
                print(final_task.result if final_task.result else "No result")
                print("-" * 50)
            elif final_task.status == TaskStatus.FAILED:
                print(f"❌ Workflow failed: {final_task.error}")
        
        # Example 2: List resumable workflows
        print("\n\n📋 Example 2: Listing resumable workflows")
        print("-" * 50)
        
        resumable = await manager.list_resumable_tasks(user_id=user_id)
        print(f"Found {resumable['total_resumable_tasks']} resumable tasks")
        
        if resumable['tasks']:
            for task in resumable['tasks'][:3]:  # Show first 3
                print(f"\n  Task ID: {task['task_id']}")
                print(f"  Query: {task['query'][:50]}...")
                print(f"  Status: {task['status']} ({task['progress']:.1f}% complete)")
                print(f"  Recommended resumption: {task['recommended_resumption']}")
        
        # Example 3: Analyze a task for resumption
        if resumable['tasks']:
            print("\n\n📋 Example 3: Analyzing task for resumption")
            print("-" * 50)
            
            task_to_analyze = resumable['tasks'][-1]['task_id']
            analysis = await manager.analyze_task(task_to_analyze)
            
            print(f"Task ID: {task_to_analyze}")
            print(f"Original status: {analysis['original_info']['status']}")
            print(f"Progress: {analysis['original_info']['progress']:.1f}%")
            
            print("\nCompletion analysis:")
            completion = analysis['resumption_analysis']['completion_analysis']
            errors = completion.pop('_errors', {})  # Extract errors separately
            
            for key, value in completion.items():
                status_icon = '✅' if value else '❌'
                error_info = ""
                
                # Add error information for failed components
                component_name = key.replace('_completed', '').replace('_started', '')
                if component_name in errors:
                    error_info = f" (Error: {errors[component_name][:50]}...)" if len(errors[component_name]) > 50 else f" (Error: {errors[component_name]})"
                
                print(f"  {key}: {status_icon}{error_info}")
            
            # Show error summary if any
            if errors:
                print(f"\n❌ Component Errors:")
                for component, error in errors.items():
                    print(f"  {component}: {error}")
            
            print(f"\nRecommended resumption: {analysis['resumption_analysis']['recommended_point']}")
            print(f"Available points: {len(analysis['resumption_analysis']['available_points'])}")
        
        # Example 4: Resume a workflow
        if resumable['tasks'] and resumable['tasks'][0]['status'] in ['failed', 'cancelled']:
            print("\n\n📋 Example 4: Resuming a failed workflow")
            print("-" * 50)
            
            failed_task_id = resumable['tasks'][0]['task_id']
            
            resume_result = await manager.resume_workflow(failed_task_id)
            
            if resume_result['success']:
                print(f"✅ Successfully resumed workflow!")
                print(f"  Original task: {resume_result['original_task_id']}")
                print(f"  New task: {resume_result['new_task_id']}")
                print(f"  Resumption point: {resume_result['resumption_point']}")
                
                # Monitor the resumed workflow
                print("\n⏳ Monitoring resumed workflow...")
                new_task_id = resume_result['new_task_id']
                
                for i in range(20):
                    await asyncio.sleep(1)
                    task = await manager.get_task_status(new_task_id)
                    if task:
                        print(f"   Status: {task.status.value} | Progress: {task.progress:.1f}%", end="\r")
                        if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                            print()
                            break
            else:
                print(f"❌ Failed to resume: {resume_result.get('error')}")
        
        # Example 5: Active workflows monitoring
        print("\n\n📋 Example 5: Active workflows")
        print("-" * 50)
        
        active = manager.monitor.get_active_workflows()
        if active:
            print(f"Found {len(active)} active workflows:")
            for workflow in active:
                print(f"\n  Task ID: {workflow['task_id']}")
                print(f"  Duration: {workflow['duration_seconds']:.1f}s")
                print(f"  Metrics: {workflow['metrics']}")
        else:
            print("No active workflows")
        
    finally:
        # Cleanup
        await manager.close()
        print("\n\n✅ Workflow manager closed")


if __name__ == "__main__":
    # Run the example
    print("🚀 Agno Rollback System - Basic Usage Example")
    print("=" * 60)
    
    asyncio.run(main())