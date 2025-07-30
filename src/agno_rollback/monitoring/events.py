"""Event definitions for the monitoring system.

This module defines the events that can occur during workflow execution.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
from uuid import UUID


class EventType(str, Enum):
    """Types of events that can occur in workflows."""
    
    # Workflow lifecycle events
    WORKFLOW_STARTED = "workflow_started"
    WORKFLOW_COMPLETED = "workflow_completed"
    WORKFLOW_FAILED = "workflow_failed"
    WORKFLOW_CANCELLED = "workflow_cancelled"
    WORKFLOW_RESUMED = "workflow_resumed"
    
    # Progress events
    PROGRESS_UPDATE = "progress_update"
    CHECKPOINT_SAVED = "checkpoint_saved"
    
    # Agent events
    AGENT_STARTED = "agent_started"
    AGENT_SUCCESS = "agent_success"
    AGENT_COMPLETED = "agent_completed"
    AGENT_FAILED = "agent_failed"
    
    # Tool events
    TOOL_CALLED = "tool_called"
    TOOL_COMPLETED = "tool_completed"
    TOOL_FAILED = "tool_failed"
    
    # Retrieval events
    WEB_RETRIEVAL_STARTED = "web_retrieval_started"
    WEB_RETRIEVAL_COMPLETED = "web_retrieval_completed"
    NEWS_RETRIEVAL_STARTED = "news_retrieval_started"
    NEWS_RETRIEVAL_COMPLETED = "news_retrieval_completed"
    
    # Summarization events
    SUMMARIZATION_STARTED = "summarization_started"
    SUMMARIZATION_COMPLETED = "summarization_completed"
    
    # Error events
    ERROR_OCCURRED = "error_occurred"
    WARNING_RAISED = "warning_raised"


@dataclass
class MonitoringEvent:
    """Base monitoring event."""
    
    task_id: UUID
    event_type: Optional[EventType] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    data: Dict[str, Any] = field(default_factory=dict)
    metadata: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary."""
        return {
            "event_type": self.event_type.value,
            "task_id": str(self.task_id),
            "timestamp": self.timestamp.isoformat(),
            "data": self.data,
            "metadata": self.metadata
        }


@dataclass
class ProgressEvent(MonitoringEvent):
    """Progress update event."""
    
    step_name: str = ""
    progress: float = 0.0
    message: Optional[str] = None
    
    def __post_init__(self):
        super().__post_init__()
        self.event_type = EventType.PROGRESS_UPDATE
        self.data.update({
            "step_name": self.step_name,
            "progress": self.progress,
            "message": self.message
        })


@dataclass
class AgentEvent(MonitoringEvent):
    """Agent-related event."""
    
    agent_name: str = ""
    agent_action: str = ""
    agent_result: Optional[Any] = None
    
    def __post_init__(self):
        super().__post_init__()
        self.data.update({
            "agent_name": self.agent_name,
            "agent_action": self.agent_action,
            "agent_result": str(self.agent_result) if self.agent_result else None
        })


@dataclass
class ToolEvent(MonitoringEvent):
    """Tool execution event."""
    
    tool_name: str = ""
    tool_args: Dict[str, Any] = field(default_factory=dict)
    tool_result: Optional[Any] = None
    execution_time_ms: Optional[int] = None
    success: bool = True
    
    def __post_init__(self):
        super().__post_init__()
        self.data.update({
            "tool_name": self.tool_name,
            "tool_args": self.tool_args,
            "tool_result": str(self.tool_result) if self.tool_result else None,
            "execution_time_ms": self.execution_time_ms,
            "success": self.success
        })


@dataclass
class ErrorEvent(MonitoringEvent):
    """Error event."""
    
    error_type: str = ""
    error_message: str = ""
    error_details: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        super().__post_init__()
        self.event_type = EventType.ERROR_OCCURRED
        self.data.update({
            "error_type": self.error_type,
            "error_message": self.error_message,
            "error_details": self.error_details
        })


class EventFactory:
    """Factory for creating monitoring events."""
    
    @staticmethod
    def workflow_started(task_id: UUID, query: str) -> MonitoringEvent:
        """Create workflow started event."""
        return MonitoringEvent(
            event_type=EventType.WORKFLOW_STARTED,
            task_id=task_id,
            data={"query": query}
        )
    
    @staticmethod
    def workflow_completed(task_id: UUID, result: str) -> MonitoringEvent:
        """Create workflow completed event."""
        return MonitoringEvent(
            event_type=EventType.WORKFLOW_COMPLETED,
            task_id=task_id,
            data={"result": result[:500]}  # Truncate large results
        )
    
    @staticmethod
    def workflow_failed(task_id: UUID, error: str) -> MonitoringEvent:
        """Create workflow failed event."""
        return MonitoringEvent(
            event_type=EventType.WORKFLOW_FAILED,
            task_id=task_id,
            data={"error": error}
        )
    
    @staticmethod
    def checkpoint_saved(task_id: UUID, checkpoint_name: str) -> MonitoringEvent:
        """Create checkpoint saved event."""
        return MonitoringEvent(
            event_type=EventType.CHECKPOINT_SAVED,
            task_id=task_id,
            data={"checkpoint_name": checkpoint_name}
        )
    
    @staticmethod
    def progress_update(
        task_id: UUID,
        step_name: str,
        progress: float,
        message: Optional[str] = None
    ) -> ProgressEvent:
        """Create progress update event."""
        return ProgressEvent(
            task_id=task_id,
            step_name=step_name,
            progress=progress,
            message=message
        )
    
    @staticmethod
    def agent_success_status(
        task_id: UUID,
        success_statuses: Dict[str, bool]
    ) -> MonitoringEvent:
        """Create agent success status event from checkpoint analysis."""
        overall_success = all(success_statuses.values()) if success_statuses else True
        failed_components = [
            component for component, status in success_statuses.items() 
            if not status
        ]
        
        return MonitoringEvent(
            event_type=EventType.AGENT_SUCCESS,
            task_id=task_id,
            data={
                "overall_success": overall_success,
                "component_statuses": success_statuses,
                "failed_components": failed_components,
                "total_components": len(success_statuses),
                "successful_components": len([s for s in success_statuses.values() if s])
            }
        )