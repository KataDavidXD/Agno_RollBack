"""Base workflow classes for the resumption system.

This module provides the foundation for all workflows with:
- Built-in state management
- Progress tracking
- Error handling
- Resumption support
"""

import asyncio
from abc import abstractmethod
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

import logging
logger = logging.getLogger(__name__)

from ..storage.base import StorageBackend
from .models import (
    ResumptionPoint,
    TaskStatus,
    WorkflowState,
    WorkflowTask,
)


class ResumableWorkflow:
    """Base class for resumable workflows.
    
    This extends Agno's Workflow class with:
    - Automatic state persistence
    - Progress tracking
    - Resumption capabilities
    - Error recovery
    """
    
    def __init__(
        self,
        task_id: UUID,
        user_id: str,
        session_id: str,
        query: str,
        storage: StorageBackend,
        resume_from: Optional[ResumptionPoint] = None,
        **kwargs
    ):
        """Initialize resumable workflow.
        
        Args:
            task_id: Unique task identifier
            user_id: User ID for the workflow
            session_id: Session ID for context
            query: User's query/request
            storage: Storage backend for persistence
            resume_from: Optional resumption point
            **kwargs: Additional workflow configuration
        """
        
        self.task_id = task_id
        self.user_id = user_id
        self.session_id = session_id
        self.query = query
        self.storage = storage
        self.resume_from = resume_from
        
        # Initialize workflow task
        self.task: Optional[WorkflowTask] = None
        self.workflow_state: Optional[WorkflowState] = None
        
        # Session state for storing intermediate results
        self.session_state: Dict[str, Any] = {}
        
        # Progress tracking
        self._current_step = ""
        self._progress = 0.0
    
    async def initialize_task(self) -> None:
        """Initialize or load the workflow task."""
        # Try to load existing task if resuming
        if self.resume_from:
            self.task = await self.storage.get_task(self.task_id)
            if not self.task:
                raise ValueError(f"Task {self.task_id} not found for resumption")
            
            # Load workflow state
            self.workflow_state = await self.storage.get_workflow_state(self.task_id)
            if not self.workflow_state:
                # Create new state if missing
                self.workflow_state = WorkflowState(
                    task_id=self.task_id,
                    last_checkpoint="initialized"
                )
            
            # Update task status
            self.task.status = TaskStatus.RUNNING
            self.task.started_at = datetime.utcnow()
            await self.storage.update_task(self.task)
            
            # Load session state from checkpoint
            if self.workflow_state.checkpoint_data:
                self.session_state.update(self.workflow_state.checkpoint_data)
        else:
            # Create new task
            self.task = WorkflowTask(
                task_id=self.task_id,
                user_id=self.user_id,
                session_id=self.session_id,
                query=self.query,
                status=TaskStatus.RUNNING,
                started_at=datetime.utcnow()
            )
            await self.storage.create_task(self.task)
            
            # Create workflow state
            self.workflow_state = WorkflowState(
                task_id=self.task_id,
                last_checkpoint="initialized"
            )
            await self.storage.save_workflow_state(self.workflow_state)
    
    async def update_progress(self, step_name: str, progress: float) -> None:
        """Update workflow progress.
        
        Args:
            step_name: Current step name
            progress: Progress percentage (0-100)
        """
        self._current_step = step_name
        self._progress = progress
        
        if self.task:
            self.task.update_progress(step_name, progress)
            self.task.add_event("progress_update", {
                "step": step_name,
                "progress": progress,
                "timestamp": datetime.utcnow().isoformat()
            })
            await self.storage.update_task(self.task)
    
    async def save_checkpoint(self, checkpoint_name: str, data: Optional[Dict[str, Any]] = None) -> None:
        """Save a checkpoint for resumption.
        
        Args:
            checkpoint_name: Name of the checkpoint
            data: Optional data to save with checkpoint
        """
        if self.workflow_state:
            self.workflow_state.last_checkpoint = checkpoint_name
            self.workflow_state.checkpoint_data = {
                **self.session_state,
                **(data or {})
            }
            await self.storage.update_workflow_state(self.workflow_state)
        
        logger.info(f"Checkpoint saved: {checkpoint_name} for task {self.task_id}")
    
    async def add_agent_output(self, agent_name: str, output: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Record agent output.
        
        Args:
            agent_name: Name of the agent
            output: Agent's output
            metadata: Optional metadata
        """
        if self.task:
            self.task.add_agent_output(agent_name, output, metadata)
            await self.storage.update_task(self.task)
    
    async def add_tool_result(
        self,
        tool_name: str,
        tool_args: Dict[str, Any],
        result: Any,
        success: bool,
        execution_time_ms: Optional[int] = None
    ) -> None:
        """Record tool execution result.
        
        Args:
            tool_name: Name of the tool
            tool_args: Tool arguments
            result: Tool result
            success: Whether execution was successful
            execution_time_ms: Execution time in milliseconds
        """
        if self.task:
            self.task.add_tool_result(tool_name, tool_args, result, success, execution_time_ms)
            await self.storage.update_task(self.task)
    
    async def complete_workflow(self, result: str) -> None:
        """Mark workflow as completed.
        
        Args:
            result: Final workflow result
        """
        if self.task:
            self.task.status = TaskStatus.COMPLETED
            self.task.completed_at = datetime.utcnow()
            self.task.progress = 100.0
            self.task.result = result
            await self.storage.update_task(self.task)
        
        logger.info(f"Workflow {self.task_id} completed successfully")
    
    async def fail_workflow(self, error: str) -> None:
        """Mark workflow as failed.
        
        Args:
            error: Error message
        """
        if self.task:
            self.task.status = TaskStatus.FAILED
            self.task.completed_at = datetime.utcnow()
            self.task.error = error
            await self.storage.update_task(self.task)
        
        logger.error(f"Workflow {self.task_id} failed: {error}")
    
    @abstractmethod
    async def execute(self) -> str:
        """Execute the workflow logic.
        
        This method should be implemented by concrete workflow classes.
        It should handle resumption based on self.resume_from.
        
        Returns:
            The workflow result
        """
        pass
    
    async def run(self) -> str:
        """Run the workflow with error handling and state management.
        
        Returns:
            The workflow result
        """
        try:
            # Initialize task and state
            await self.initialize_task()
            
            # Execute workflow logic
            result = await self.execute()
            
            # Mark as completed
            await self.complete_workflow(result)
            
            return result
            
        except Exception as e:
            # Handle errors
            error_msg = str(e)
            logger.error(f"Workflow {self.task_id} error: {error_msg}", exc_info=True)
            await self.fail_workflow(error_msg)
            raise
    
    def should_resume_from(self, point: ResumptionPoint) -> bool:
        """Check if workflow should resume from a specific point.
        
        Args:
            point: Resumption point to check
            
        Returns:
            True if should resume from this point
        """
        if not self.resume_from:
            return False
        
        # Map resumption points to priority order
        priority_order = [
            ResumptionPoint.RESTART_FROM_BEGINNING,
            ResumptionPoint.RESUME_BEFORE_PARALLEL_RETRIEVAL,
            ResumptionPoint.RESUME_AFTER_WEB_RETRIEVAL,
            ResumptionPoint.RESUME_AFTER_NEWS_RETRIEVAL,
            ResumptionPoint.RESUME_AFTER_PARALLEL_RETRIEVAL,
            ResumptionPoint.RESUME_BEFORE_SUMMARIZATION,
            ResumptionPoint.RESUME_FROM_PARTIAL_SUMMARIZATION,
            ResumptionPoint.CONTINUE_FROM_WHERE_LEFT_OFF,
        ]
        
        try:
            resume_index = priority_order.index(self.resume_from)
            point_index = priority_order.index(point)
            return point_index >= resume_index
        except ValueError:
            return False


class ParallelRetrievalMixin:
    """Mixin for workflows that perform parallel retrieval operations.
    
    This provides common functionality for web and news retrieval.
    """
    
    async def parallel_retrieval(
        self,
        web_retrieval_func,
        news_retrieval_func,
        skip_web: bool = False,
        skip_news: bool = False
    ) -> Dict[str, Any]:
        """Execute parallel retrieval operations.
        
        Args:
            web_retrieval_func: Async function for web retrieval
            news_retrieval_func: Async function for news retrieval
            skip_web: Skip web retrieval
            skip_news: Skip news retrieval
            
        Returns:
            Dict with web_results and news_results
        """
        results = {"web_results": None, "news_results": None}
        
        # Create tasks for parallel execution
        tasks = []
        
        if not skip_web:
            tasks.append(("web", web_retrieval_func()))
        
        if not skip_news:
            tasks.append(("news", news_retrieval_func()))
        
        # Execute in parallel
        if tasks:
            task_results = await asyncio.gather(
                *[task[1] for task in tasks],
                return_exceptions=True
            )
            
            # Process results
            for i, (task_type, _) in enumerate(tasks):
                result = task_results[i]
                
                if isinstance(result, Exception):
                    logger.error(f"{task_type} retrieval failed: {result}")
                    results[f"{task_type}_results"] = {
                        "error": str(result),
                        "success": False
                    }
                else:
                    results[f"{task_type}_results"] = result
        
        return results