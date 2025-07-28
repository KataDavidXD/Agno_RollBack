"""Base storage interface for the workflow resumption system.

This abstract base class defines the contract that all storage implementations
must follow. This allows us to swap storage backends without changing the
business logic.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from uuid import UUID

from ..core.models import SessionContext, WorkflowState, WorkflowTask


class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    
    @abstractmethod
    async def initialize(self) -> None:
        """Initialize the storage backend (create tables, connections, etc.)."""
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """Close storage connections and cleanup resources."""
        pass
    
    # Task Management Methods
    
    @abstractmethod
    async def create_task(self, task: WorkflowTask) -> WorkflowTask:
        """Create a new workflow task."""
        pass
    
    @abstractmethod
    async def get_task(self, task_id: UUID) -> Optional[WorkflowTask]:
        """Get a task by ID."""
        pass
    
    @abstractmethod
    async def update_task(self, task: WorkflowTask) -> WorkflowTask:
        """Update an existing task."""
        pass
    
    @abstractmethod
    async def list_tasks(
        self,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[WorkflowTask]:
        """List tasks with optional filters."""
        pass
    
    @abstractmethod
    async def delete_task(self, task_id: UUID) -> bool:
        """Delete a task by ID."""
        pass
    
    # Workflow State Methods
    
    @abstractmethod
    async def save_workflow_state(self, state: WorkflowState) -> WorkflowState:
        """Save workflow execution state."""
        pass
    
    @abstractmethod
    async def get_workflow_state(self, task_id: UUID) -> Optional[WorkflowState]:
        """Get workflow state by task ID."""
        pass
    
    @abstractmethod
    async def update_workflow_state(self, state: WorkflowState) -> WorkflowState:
        """Update workflow state."""
        pass
    
    # Session Management Methods
    
    @abstractmethod
    async def get_session(self, user_id: str, session_id: str) -> Optional[SessionContext]:
        """Get session context."""
        pass
    
    @abstractmethod
    async def save_session(self, session: SessionContext) -> SessionContext:
        """Save or update session context."""
        pass
    
    @abstractmethod
    async def delete_session(self, user_id: str, session_id: str) -> bool:
        """Delete a session."""
        pass
    
    # Utility Methods
    
    @abstractmethod
    async def get_resumable_tasks(
        self,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get all tasks that can be resumed."""
        pass
    
    @abstractmethod
    async def get_task_execution_history(self, task_id: UUID) -> Dict[str, Any]:
        """Get complete execution history for a task."""
        pass
    
    @abstractmethod
    async def cleanup_old_tasks(self, days: int = 30) -> int:
        """Clean up tasks older than specified days. Returns number of deleted tasks."""
        pass