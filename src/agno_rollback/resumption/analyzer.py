"""Workflow state analyzer for resumption decisions.

This module analyzes workflow states to determine:
- What has been completed
- What can be resumed
- The best resumption strategy
"""

from typing import Dict, List, Optional, Tuple
from uuid import UUID

from ..core.models import (
    ResumptionPoint,
    TaskStatus,
    WorkflowState,
    WorkflowTask,
)
from ..storage.base import StorageBackend


class WorkflowStateAnalyzer:
    """Analyzes workflow states for resumption capabilities."""
    
    def __init__(self, storage: StorageBackend):
        """Initialize the analyzer.
        
        Args:
            storage: Storage backend for accessing workflow data
        """
        self.storage = storage
    
    async def analyze_task_for_resumption(
        self,
        task_id: UUID
    ) -> Dict[str, any]:
        """Analyze a task to determine resumption options.
        
        Args:
            task_id: Task ID to analyze
            
        Returns:
            Analysis results including available resumption points
        """
        # Get task and state
        task = await self.storage.get_task(task_id)
        if not task:
            return {
                "error": f"Task {task_id} not found",
                "resumable": False
            }
        
        state = await self.storage.get_workflow_state(task_id)
        if not state:
            # Create state from task data if missing
            state = self._create_state_from_task(task)
        
        # Analyze completion status
        completion_analysis = self._analyze_completion_status(task, state)
        
        # Determine available resumption points
        state.analyze_resumption_points()
        recommended = state.get_recommended_resumption_point()
        
        # Calculate estimated remaining work
        remaining_work = self._estimate_remaining_work(task, state)
        
        return {
            "task_id": str(task_id),
            "original_info": {
                "user_id": task.user_id,
                "session_id": task.session_id,
                "query": task.query,
                "status": task.status.value,
                "progress": task.progress,
                "created_at": task.created_at.isoformat(),
                "last_step": task.current_step,
                "error": task.error
            },
            "resumption_analysis": {
                "resumable": task.status in [TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.RUNNING],
                "recommended_point": recommended.value if recommended else None,
                "available_points": [p.value for p in state.available_resumption_points],
                "completion_analysis": completion_analysis,
                "remaining_work": remaining_work
            },
            "stored_data_summary": {
                "workflow_events": len(task.workflow_events),
                "agent_outputs": len(task.agent_outputs),
                "tool_results": len(task.tool_results),
                "step_results": len(task.step_results),
                "has_web_results": task.web_retrieval_results is not None,
                "has_news_results": task.news_retrieval_results is not None
            }
        }
    
    async def find_best_resumption_point(
        self,
        task_id: UUID
    ) -> Tuple[ResumptionPoint, Dict[str, any]]:
        """Find the best resumption point for a task.
        
        Args:
            task_id: Task ID to analyze
            
        Returns:
            Tuple of (best resumption point, analysis data)
        """
        analysis = await self.analyze_task_for_resumption(task_id)
        
        if not analysis.get("resumption_analysis", {}).get("resumable"):
            return ResumptionPoint.RESTART_FROM_BEGINNING, analysis
        
        recommended = analysis["resumption_analysis"].get("recommended_point")
        if recommended:
            return ResumptionPoint(recommended), analysis
        
        # Fallback to smart continuation
        return ResumptionPoint.CONTINUE_FROM_WHERE_LEFT_OFF, analysis
    
    async def get_resumable_tasks_summary(
        self,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> Dict[str, any]:
        """Get summary of all resumable tasks.
        
        Args:
            user_id: Optional user ID filter
            session_id: Optional session ID filter
            
        Returns:
            Summary of resumable tasks
        """
        resumable_tasks = await self.storage.get_resumable_tasks(user_id, session_id)
        
        # Group by status
        by_status = {
            "failed": [],
            "cancelled": [],
            "running": []
        }
        
        for task in resumable_tasks:
            status = task.get("status", "unknown")
            if status in by_status:
                by_status[status].append(task)
        
        # Calculate statistics
        total_resumable = len(resumable_tasks)
        avg_progress = sum(t.get("progress", 0) for t in resumable_tasks) / max(total_resumable, 1)
        
        return {
            "total_resumable_tasks": total_resumable,
            "by_status": {
                "failed": len(by_status["failed"]),
                "cancelled": len(by_status["cancelled"]),
                "running": len(by_status["running"])
            },
            "average_progress": round(avg_progress, 2),
            "tasks": resumable_tasks
        }
    
    def _create_state_from_task(self, task: WorkflowTask) -> WorkflowState:
        """Create workflow state from task data.
        
        Args:
            task: Workflow task
            
        Returns:
            Reconstructed workflow state
        """
        state = WorkflowState(
            task_id=task.task_id,
            last_checkpoint=task.current_step or "unknown"
        )
        
        # Infer completion status from task data
        state.web_retrieval_completed = task.web_retrieval_results is not None
        state.news_retrieval_completed = task.news_retrieval_results is not None
        
        # Check if summarization was started
        for event in task.workflow_events:
            if event.event_type == "summarization_started":
                state.summarization_started = True
            elif event.event_type == "summarization_completed":
                state.summarization_completed = True
        
        return state
    
    def _analyze_completion_status(
        self,
        task: WorkflowTask,
        state: WorkflowState
    ) -> Dict[str, bool]:
        """Analyze what parts of the workflow are completed.
        
        Args:
            task: Workflow task
            state: Workflow state
            
        Returns:
            Completion status for each component
        """
        return {
            "initialization_completed": len(task.workflow_events) > 0,
            "web_retrieval_started": any(
                e.event_type == "web_retrieval_started" 
                for e in task.workflow_events
            ),
            "web_retrieval_completed": state.web_retrieval_completed,
            "news_retrieval_started": any(
                e.event_type == "news_retrieval_started" 
                for e in task.workflow_events
            ),
            "news_retrieval_completed": state.news_retrieval_completed,
            "parallel_retrieval_completed": (
                state.web_retrieval_completed and 
                state.news_retrieval_completed
            ),
            "summarization_started": state.summarization_started,
            "summarization_completed": state.summarization_completed,
            "workflow_completed": task.status == TaskStatus.COMPLETED
        }
    
    def _estimate_remaining_work(
        self,
        task: WorkflowTask,
        state: WorkflowState
    ) -> Dict[str, any]:
        """Estimate remaining work for the workflow.
        
        Args:
            task: Workflow task
            state: Workflow state
            
        Returns:
            Estimation of remaining work
        """
        total_steps = 4  # init, web, news, summary
        completed_steps = 0
        
        if len(task.workflow_events) > 0:
            completed_steps += 1  # initialization
        if state.web_retrieval_completed:
            completed_steps += 1
        if state.news_retrieval_completed:
            completed_steps += 1
        if state.summarization_completed:
            completed_steps += 1
        
        remaining_steps = []
        if not state.web_retrieval_completed:
            remaining_steps.append("web_retrieval")
        if not state.news_retrieval_completed:
            remaining_steps.append("news_retrieval")
        if not state.summarization_completed:
            remaining_steps.append("summarization")
        
        return {
            "total_steps": total_steps,
            "completed_steps": completed_steps,
            "remaining_steps": remaining_steps,
            "estimated_completion_percentage": round(
                (completed_steps / total_steps) * 100, 2
            ),
            "estimated_time_minutes": len(remaining_steps) * 2  # Rough estimate
        }