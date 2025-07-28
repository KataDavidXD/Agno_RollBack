"""Workflow resumption manager.

This module handles the actual resumption of workflows:
- Creating new tasks from failed ones
- Restoring state and context
- Managing resumption strategies
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Type
from uuid import UUID, uuid4

import logging
logger = logging.getLogger(__name__)

from ..core.models import ResumptionPoint, TaskStatus, WorkflowTask
from ..core.workflow import ResumableWorkflow
from ..storage.base import StorageBackend
from .analyzer import WorkflowStateAnalyzer


class ResumptionManager:
    """Manages workflow resumption operations."""
    
    def __init__(self, storage: StorageBackend):
        """Initialize the resumption manager.
        
        Args:
            storage: Storage backend
        """
        self.storage = storage
        self.analyzer = WorkflowStateAnalyzer(storage)
        self._workflow_registry: Dict[str, Type[ResumableWorkflow]] = {}
    
    def register_workflow(
        self,
        workflow_type: str,
        workflow_class: Type[ResumableWorkflow]
    ) -> None:
        """Register a workflow class for resumption.
        
        Args:
            workflow_type: Unique workflow type identifier
            workflow_class: Workflow class
        """
        self._workflow_registry[workflow_type] = workflow_class
        logger.info(f"Registered workflow type: {workflow_type}")
    
    async def resume_workflow(
        self,
        original_task_id: UUID,
        resumption_point: Optional[ResumptionPoint] = None,
        workflow_type: Optional[str] = None,
        **workflow_kwargs
    ) -> Dict[str, Any]:
        """Resume a workflow from a specific point.
        
        Args:
            original_task_id: Original task ID to resume
            resumption_point: Specific resumption point (auto-detect if None)
            workflow_type: Workflow type (required if not registered)
            **workflow_kwargs: Additional kwargs for workflow initialization
            
        Returns:
            Resumption result with new task ID
        """
        # Analyze the original task
        original_task = await self.storage.get_task(original_task_id)
        if not original_task:
            return {
                "success": False,
                "error": f"Original task {original_task_id} not found"
            }
        
        # Determine resumption point if not specified
        if not resumption_point:
            point, analysis = await self.analyzer.find_best_resumption_point(original_task_id)
            resumption_point = point
        else:
            analysis = await self.analyzer.analyze_task_for_resumption(original_task_id)
        
        # Validate resumption point
        available_points = analysis["resumption_analysis"]["available_points"]
        if resumption_point.value not in available_points:
            return {
                "success": False,
                "error": f"Resumption point {resumption_point.value} not available",
                "available_points": available_points
            }
        
        # Create new task for resumption
        new_task_id = uuid4()
        
        try:
            # Get workflow class
            if workflow_type and workflow_type in self._workflow_registry:
                workflow_class = self._workflow_registry[workflow_type]
            else:
                return {
                    "success": False,
                    "error": "Workflow type not registered or specified"
                }
            
            # Create workflow instance
            workflow = workflow_class(
                task_id=new_task_id,
                user_id=original_task.user_id,
                session_id=original_task.session_id,
                query=original_task.query,
                storage=self.storage,
                resume_from=resumption_point,
                **workflow_kwargs
            )
            
            # Log resumption
            logger.info(
                f"Resuming workflow {original_task_id} as {new_task_id} "
                f"from {resumption_point.value}"
            )
            
            # Create resumption record
            await self._create_resumption_record(
                original_task_id,
                new_task_id,
                resumption_point
            )
            
            # Start workflow execution asynchronously
            asyncio.create_task(self._execute_workflow(workflow))
            
            return {
                "success": True,
                "new_task_id": str(new_task_id),
                "original_task_id": str(original_task_id),
                "resumption_point": resumption_point.value,
                "message": f"Workflow resumed from {resumption_point.value}",
                "analysis": analysis
            }
            
        except Exception as e:
            logger.error(f"Failed to resume workflow: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    async def retry_failed_workflow(
        self,
        task_id: UUID,
        workflow_type: str,
        **workflow_kwargs
    ) -> Dict[str, Any]:
        """Retry a failed workflow from the best resumption point.
        
        Args:
            task_id: Failed task ID
            workflow_type: Workflow type
            **workflow_kwargs: Additional workflow arguments
            
        Returns:
            Retry result
        """
        # Analyze task
        task = await self.storage.get_task(task_id)
        if not task:
            return {
                "success": False,
                "error": f"Task {task_id} not found"
            }
        
        if task.status != TaskStatus.FAILED:
            return {
                "success": False,
                "error": f"Task is not in failed state (current: {task.status.value})"
            }
        
        # Find best resumption point
        resumption_point, _ = await self.analyzer.find_best_resumption_point(task_id)
        
        # Resume workflow
        return await self.resume_workflow(
            task_id,
            resumption_point,
            workflow_type,
            **workflow_kwargs
        )
    
    async def list_resumable_workflows(
        self,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """List all workflows that can be resumed.
        
        Args:
            user_id: Optional user filter
            session_id: Optional session filter
            
        Returns:
            Summary of resumable workflows
        """
        return await self.analyzer.get_resumable_tasks_summary(user_id, session_id)
    
    async def get_resumption_details(
        self,
        task_id: UUID
    ) -> Dict[str, Any]:
        """Get detailed resumption information for a task.
        
        Args:
            task_id: Task ID
            
        Returns:
            Detailed resumption analysis
        """
        analysis = await self.analyzer.analyze_task_for_resumption(task_id)
        
        # Add execution history
        history = await self.storage.get_task_execution_history(task_id)
        analysis["execution_history"] = history
        
        # Add resumption recommendations
        if analysis["resumption_analysis"]["resumable"]:
            recommendations = self._generate_resumption_recommendations(analysis)
            analysis["recommendations"] = recommendations
        
        return analysis
    
    async def _execute_workflow(self, workflow: ResumableWorkflow) -> None:
        """Execute a workflow with error handling.
        
        Args:
            workflow: Workflow instance to execute
        """
        try:
            await workflow.run()
        except Exception as e:
            logger.error(
                f"Workflow {workflow.task_id} execution failed: {e}", 
                exc_info=True
            )
    
    async def _create_resumption_record(
        self,
        original_task_id: UUID,
        new_task_id: UUID,
        resumption_point: ResumptionPoint
    ) -> None:
        """Create a record linking original and resumed tasks.
        
        Args:
            original_task_id: Original task ID
            new_task_id: New task ID
            resumption_point: Resumption point used
        """
        # Add event to original task
        original_task = await self.storage.get_task(original_task_id)
        if original_task:
            original_task.add_event("workflow_resumed", {
                "new_task_id": str(new_task_id),
                "resumption_point": resumption_point.value,
                "timestamp": datetime.utcnow().isoformat()
            })
            await self.storage.update_task(original_task)
    
    def _generate_resumption_recommendations(
        self,
        analysis: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        """Generate resumption recommendations based on analysis.
        
        Args:
            analysis: Task analysis results
            
        Returns:
            List of recommendations
        """
        recommendations = []
        completion = analysis["resumption_analysis"]["completion_analysis"]
        
        # Check what's completed
        if not completion.get("web_retrieval_completed") and not completion.get("news_retrieval_completed"):
            recommendations.append({
                "type": "efficiency",
                "message": "Both retrievals incomplete - consider restarting for fresh data",
                "suggested_point": ResumptionPoint.RESTART_FROM_BEGINNING.value
            })
        elif completion.get("parallel_retrieval_completed") and not completion.get("summarization_started"):
            recommendations.append({
                "type": "optimal",
                "message": "All data retrieved - resume from summarization to save time",
                "suggested_point": ResumptionPoint.RESUME_BEFORE_SUMMARIZATION.value
            })
        
        # Check error patterns
        if analysis["original_info"].get("error"):
            error = analysis["original_info"]["error"].lower()
            if "timeout" in error:
                recommendations.append({
                    "type": "warning",
                    "message": "Previous failure was timeout - consider increasing timeout settings"
                })
            elif "api" in error or "rate limit" in error:
                recommendations.append({
                    "type": "warning",
                    "message": "Previous failure was API-related - check API quotas and limits"
                })
        
        # Check data freshness
        if completion.get("web_retrieval_completed") or completion.get("news_retrieval_completed"):
            recommendations.append({
                "type": "info",
                "message": "Retrieved data may be stale - consider if fresh data is needed"
            })
        
        return recommendations