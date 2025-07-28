"""Advanced resumption demonstration.

This example shows:
1. Simulating failures at different points
2. Resuming from specific checkpoints
3. Handling partial completions
"""

import asyncio
import sys
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).parent.parent))

from agno_rollback import (
    WorkflowManager,
    ResumptionPoint,
    TaskStatus,
    SQLiteStorage,
    WorkflowMonitor,
)


class SimulatedFailureWorkflow:
    """Workflow that can simulate failures for testing resumption."""
    
    def __init__(self, manager: WorkflowManager, fail_at: str = None):
        self.manager = manager
        self.fail_at = fail_at
    
    async def run_with_failure(self, user_id: str, query: str) -> str:
        """Run workflow with simulated failure."""
        # Temporarily modify the workflow to inject failures
        # In real scenarios, failures would occur naturally
        
        task_id = await self.manager.start_workflow(
            user_id=user_id,
            query=query
        )
        
        if self.fail_at:
            # Wait a bit then simulate failure by updating task status
            await asyncio.sleep(2)
            
            # This is for demo purposes - normally failures happen naturally
            task = await self.manager.get_task_status(task_id)
            if task and self.fail_at == "web_retrieval":
                # Simulate failure during web retrieval
                task.status = TaskStatus.FAILED
                task.error = "Simulated web retrieval failure"
                task.progress = 30.0
                await self.manager.storage.update_task(task)
            elif task and self.fail_at == "summarization":
                # Simulate failure during summarization
                task.status = TaskStatus.FAILED
                task.error = "Simulated summarization failure"
                task.progress = 70.0
                # Mark retrievals as complete
                if task.workflow_state:
                    task.workflow_state.web_retrieval_completed = True
                    task.workflow_state.news_retrieval_completed = True
                await self.manager.storage.update_task(task)
        
        return task_id


async def demonstrate_resumption_scenarios():
    """Demonstrate various resumption scenarios."""
    
    # Initialize manager
    storage = SQLiteStorage("resumption_demo.db")
    monitor = WorkflowMonitor(storage)
    manager = WorkflowManager(storage=storage, monitor=monitor)
    
    await manager.initialize()
    
    user_id = "resumption_demo_user"
    
    try:
        # Scenario 1: Complete workflow (baseline)
        print("\n🔹 Scenario 1: Running complete workflow (baseline)")
        print("-" * 60)
        
        query1 = "Latest breakthroughs in quantum computing"
        task1 = await manager.start_workflow(user_id, query1)
        print(f"Started task: {task1}")
        
        # Wait for completion
        await wait_for_completion(manager, task1, timeout=30)
        
        # Scenario 2: Failure during web retrieval
        print("\n\n🔹 Scenario 2: Simulating failure during web retrieval")
        print("-" * 60)
        
        query2 = "Impact of AI on healthcare industry"
        failure_workflow = SimulatedFailureWorkflow(manager, fail_at="web_retrieval")
        task2 = await failure_workflow.run_with_failure(user_id, query2)
        
        await asyncio.sleep(3)  # Let it fail
        
        # Analyze the failed task
        print("\nAnalyzing failed task...")
        analysis = await manager.analyze_task(task2)
        print_analysis(analysis)
        
        # Resume from recommended point
        print("\nResuming from recommended point...")
        resume_result = await manager.resume_workflow(task2)
        if resume_result['success']:
            print(f"✅ Resumed as task: {resume_result['new_task_id']}")
            await wait_for_completion(manager, resume_result['new_task_id'])
        
        # Scenario 3: Failure during summarization
        print("\n\n🔹 Scenario 3: Simulating failure during summarization")
        print("-" * 60)
        
        query3 = "Future of renewable energy technologies"
        failure_workflow2 = SimulatedFailureWorkflow(manager, fail_at="summarization")
        task3 = await failure_workflow2.run_with_failure(user_id, query3)
        
        await asyncio.sleep(3)  # Let it fail
        
        # Analyze and show we can skip retrieval
        print("\nAnalyzing failed task...")
        analysis = await manager.analyze_task(task3)
        print_analysis(analysis)
        
        # Resume from summarization (skip retrieval)
        print("\nResuming from summarization point (skipping retrieval)...")
        resume_result = await manager.resume_workflow(
            task3,
            ResumptionPoint.RESUME_BEFORE_SUMMARIZATION.value
        )
        if resume_result['success']:
            print(f"✅ Resumed from summarization: {resume_result['new_task_id']}")
            await wait_for_completion(manager, resume_result['new_task_id'])
        
        # Scenario 4: Manual resumption point selection
        print("\n\n🔹 Scenario 4: Manual resumption point selection")
        print("-" * 60)
        
        # List all resumable tasks
        resumable = await manager.list_resumable_tasks(user_id)
        print(f"\nFound {resumable['total_resumable_tasks']} resumable tasks:")
        
        for task in resumable['tasks'][:5]:
            print(f"\n  Task: {task['task_id'][:8]}...")
            print(f"  Query: {task['query'][:40]}...")
            print(f"  Status: {task['status']} at {task['progress']:.0f}%")
            print(f"  Available resumptions: {', '.join(task['available_resumptions'][:3])}...")
        
        # Demonstrate different resumption strategies
        if resumable['tasks']:
            task_id = resumable['tasks'][0]['task_id']
            
            print(f"\n\nDemonstrating resumption strategies for task {task_id[:8]}...")
            
            # Option 1: Restart completely
            print("\n  Option 1: Complete restart")
            result1 = await manager.resume_workflow(
                task_id,
                ResumptionPoint.RESTART_FROM_BEGINNING.value
            )
            print(f"  Result: {'✅ Success' if result1['success'] else '❌ Failed'}")
            
            # Option 2: Smart continuation
            print("\n  Option 2: Smart continuation")
            result2 = await manager.resume_workflow(
                task_id,
                ResumptionPoint.CONTINUE_FROM_WHERE_LEFT_OFF.value
            )
            print(f"  Result: {'✅ Success' if result2['success'] else '❌ Failed'}")
        
        # Scenario 5: Metrics and monitoring
        print("\n\n🔹 Scenario 5: Workflow metrics and monitoring")
        print("-" * 60)
        
        active = monitor.get_active_workflows()
        print(f"\nActive workflows: {len(active)}")
        
        for workflow in active:
            print(f"\n  Task: {workflow['task_id'][:8]}...")
            print(f"  Duration: {workflow['duration_seconds']:.1f}s")
            print(f"  Metrics: {workflow['metrics']}")
        
    finally:
        await manager.close()
        print("\n\n✅ Demo completed")


async def wait_for_completion(manager: WorkflowManager, task_id: str, timeout: int = 30):
    """Wait for task completion with progress updates."""
    print(f"\n⏳ Waiting for task completion...")
    
    for i in range(timeout):
        task = await manager.get_task_status(task_id)
        if task:
            print(f"   Progress: {task.progress:.0f}% - Status: {task.status.value}", end="\r")
            
            if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                print()  # New line
                if task.status == TaskStatus.COMPLETED:
                    print(f"✅ Task completed successfully")
                else:
                    print(f"❌ Task failed: {task.error}")
                return task
        
        await asyncio.sleep(1)
    
    print("\n⏱️  Timeout waiting for completion")
    return None


def print_analysis(analysis: dict):
    """Print task analysis in a readable format."""
    print("\n📊 Task Analysis:")
    print(f"  Status: {analysis['original_info']['status']}")
    print(f"  Progress: {analysis['original_info']['progress']:.0f}%")
    
    if analysis['original_info'].get('error'):
        print(f"  Error: {analysis['original_info']['error']}")
    
    print("\n  Completion status:")
    completion = analysis['resumption_analysis']['completion_analysis']
    for key, value in completion.items():
        if key.endswith('_completed'):
            print(f"    {key}: {'✅' if value else '❌'}")
    
    print(f"\n  Recommended resumption: {analysis['resumption_analysis']['recommended_point']}")
    print(f"  Remaining work: {analysis['resumption_analysis']['remaining_work']['remaining_steps']}")


if __name__ == "__main__":
    print("🚀 Agno Rollback System - Advanced Resumption Demo")
    print("=" * 70)
    print("This demo shows various failure and resumption scenarios")
    
    asyncio.run(demonstrate_resumption_scenarios())