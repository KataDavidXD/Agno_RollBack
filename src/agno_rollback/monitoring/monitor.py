"""Workflow monitoring service.

This module provides real-time monitoring of workflow execution:
- Event tracking
- Progress monitoring
- Performance metrics
"""

import asyncio
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
from uuid import UUID

import logging
logger = logging.getLogger(__name__)

from ..storage.base import StorageBackend
from .events import EventType, MonitoringEvent


class WorkflowMonitor:
    """Monitors workflow execution and collects metrics."""
    
    def __init__(self, storage: Optional[StorageBackend] = None):
        """Initialize the workflow monitor.
        
        Args:
            storage: Optional storage backend for persisting events
        """
        self.storage = storage
        self._event_handlers: Dict[EventType, List[Callable]] = defaultdict(list)
        self._active_workflows: Dict[UUID, Dict[str, Any]] = {}
        self._event_queue: asyncio.Queue = asyncio.Queue()
        self._running = False
        self._processor_task: Optional[asyncio.Task] = None
    
    async def start(self) -> None:
        """Start the monitoring service."""
        if self._running:
            return
        
        self._running = True
        self._processor_task = asyncio.create_task(self._process_events())
        logger.info("Workflow monitor started")
    
    async def stop(self) -> None:
        """Stop the monitoring service."""
        self._running = False
        
        if self._processor_task:
            await self._event_queue.put(None)  # Sentinel to stop processor
            await self._processor_task
            self._processor_task = None
        
        logger.info("Workflow monitor stopped")
    
    def register_handler(
        self,
        event_type: EventType,
        handler: Callable[[MonitoringEvent], None]
    ) -> None:
        """Register an event handler.
        
        Args:
            event_type: Type of event to handle
            handler: Handler function
        """
        self._event_handlers[event_type].append(handler)
    
    async def emit_event(self, event: MonitoringEvent) -> None:
        """Emit a monitoring event.
        
        Args:
            event: Event to emit
        """
        await self._event_queue.put(event)
    
    async def track_workflow_start(
        self,
        task_id: UUID,
        user_id: str,
        query: str
    ) -> None:
        """Track workflow start.
        
        Args:
            task_id: Task ID
            user_id: User ID
            query: User query
        """
        self._active_workflows[task_id] = {
            "user_id": user_id,
            "query": query,
            "start_time": datetime.utcnow(),
            "events": [],
            "metrics": {
                "agent_calls": 0,
                "tool_calls": 0,
                "errors": 0,
                "checkpoints": 0
            }
        }
        
        from .events import EventFactory
        event = EventFactory.workflow_started(task_id, query)
        await self.emit_event(event)
    
    async def track_workflow_end(
        self,
        task_id: UUID,
        success: bool,
        result_or_error: str
    ) -> None:
        """Track workflow end.
        
        Args:
            task_id: Task ID
            success: Whether workflow succeeded
            result_or_error: Result or error message
        """
        if task_id not in self._active_workflows:
            return
        
        workflow_info = self._active_workflows[task_id]
        workflow_info["end_time"] = datetime.utcnow()
        workflow_info["success"] = success
        workflow_info["duration_seconds"] = (
            workflow_info["end_time"] - workflow_info["start_time"]
        ).total_seconds()
        
        from .events import EventFactory
        if success:
            event = EventFactory.workflow_completed(task_id, result_or_error)
        else:
            event = EventFactory.workflow_failed(task_id, result_or_error)
        
        await self.emit_event(event)
        
        # Log summary
        logger.info(
            f"Workflow {task_id} {'completed' if success else 'failed'} "
            f"in {workflow_info['duration_seconds']:.2f}s - "
            f"Metrics: {workflow_info['metrics']}"
        )
        
        # Cleanup
        del self._active_workflows[task_id]
    
    async def update_progress(
        self,
        task_id: UUID,
        step_name: str,
        progress: float,
        message: Optional[str] = None
    ) -> None:
        """Update workflow progress.
        
        Args:
            task_id: Task ID
            step_name: Current step name
            progress: Progress percentage
            message: Optional progress message
        """
        from .events import EventFactory
        event = EventFactory.progress_update(task_id, step_name, progress, message)
        await self.emit_event(event)
    
    def get_active_workflows(self) -> List[Dict[str, Any]]:
        """Get information about active workflows.
        
        Returns:
            List of active workflow information
        """
        active = []
        now = datetime.utcnow()
        
        for task_id, info in self._active_workflows.items():
            duration = (now - info["start_time"]).total_seconds()
            active.append({
                "task_id": str(task_id),
                "user_id": info["user_id"],
                "query": info["query"],
                "duration_seconds": duration,
                "metrics": info["metrics"]
            })
        
        return active
    
    def get_workflow_metrics(self, task_id: UUID) -> Optional[Dict[str, Any]]:
        """Get metrics for a specific workflow.
        
        Args:
            task_id: Task ID
            
        Returns:
            Workflow metrics or None if not found
        """
        if task_id in self._active_workflows:
            return self._active_workflows[task_id]["metrics"]
        return None
    
    async def _process_events(self) -> None:
        """Process events from the queue."""
        while self._running:
            try:
                # Get event with timeout to allow checking _running
                event = await asyncio.wait_for(
                    self._event_queue.get(),
                    timeout=1.0
                )
                
                if event is None:  # Sentinel value
                    break
                
                # Process event
                await self._handle_event(event)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing event: {e}", exc_info=True)
    
    async def _handle_event(self, event: MonitoringEvent) -> None:
        """Handle a monitoring event.
        
        Args:
            event: Event to handle
        """
        # Update metrics
        if event.task_id in self._active_workflows:
            workflow = self._active_workflows[event.task_id]
            workflow["events"].append(event)
            
            # Update specific metrics
            if event.event_type in [EventType.AGENT_STARTED, EventType.AGENT_COMPLETED]:
                workflow["metrics"]["agent_calls"] += 1
            elif event.event_type == EventType.TOOL_CALLED:
                workflow["metrics"]["tool_calls"] += 1
            elif event.event_type == EventType.ERROR_OCCURRED:
                workflow["metrics"]["errors"] += 1
            elif event.event_type == EventType.CHECKPOINT_SAVED:
                workflow["metrics"]["checkpoints"] += 1
        
        # Call registered handlers
        handlers = self._event_handlers.get(event.event_type, [])
        for handler in handlers:
            try:
                handler(event)
            except Exception as e:
                logger.error(f"Error in event handler: {e}", exc_info=True)
        
        # Persist event if storage is available
        if self.storage and hasattr(event, "task_id"):
            try:
                task = await self.storage.get_task(event.task_id)
                if task:
                    task.add_event(event.event_type.value, event.data)
                    await self.storage.update_task(task)
            except Exception as e:
                logger.error(f"Failed to persist event: {e}", exc_info=True)


class MonitoringContext:
    """Context manager for monitoring a specific operation."""
    
    def __init__(
        self,
        monitor: WorkflowMonitor,
        task_id: UUID,
        operation_name: str,
        event_type: EventType
    ):
        self.monitor = monitor
        self.task_id = task_id
        self.operation_name = operation_name
        self.event_type = event_type
        self.start_time = None
    
    async def __aenter__(self):
        """Enter the context."""
        self.start_time = datetime.utcnow()
        event = MonitoringEvent(
            event_type=self.event_type,
            task_id=self.task_id,
            data={"operation": self.operation_name, "status": "started"}
        )
        await self.monitor.emit_event(event)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the context."""
        duration_ms = int((datetime.utcnow() - self.start_time).total_seconds() * 1000)
        
        if exc_type is None:
            # Success
            event = MonitoringEvent(
                event_type=self.event_type,
                task_id=self.task_id,
                data={
                    "operation": self.operation_name,
                    "status": "completed",
                    "duration_ms": duration_ms
                }
            )
        else:
            # Failure
            event = MonitoringEvent(
                event_type=EventType.ERROR_OCCURRED,
                task_id=self.task_id,
                data={
                    "operation": self.operation_name,
                    "status": "failed",
                    "error": str(exc_val),
                    "duration_ms": duration_ms
                }
            )
        
        await self.monitor.emit_event(event)