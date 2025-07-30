"""Agno Rollback - Modern Workflow Resumption System.

A production-ready workflow resumption system built with Agno 1.7.6 that enables:
- Workflow pause and resume from any point
- Parallel information retrieval
- Smart summarization
- Comprehensive monitoring
"""

# Load environment variables from .env file if it exists
from dotenv import load_dotenv
load_dotenv()

import asyncio
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

from .core import (
    AgentConfig,
    AgentFactory,
    ResumableWorkflow,
    ResumptionPoint,
    TaskStatus,
    WorkflowTask,
)
from .monitoring import WorkflowMonitor
from .resumption import ResumptionManager, WorkflowStateAnalyzer
from .storage import (
    StorageBackend,
    SQLiteStorage,
    PostgresStorage,
    ClickHouseStorage,
    S3Storage,
    CompositeStorage,
    create_storage_backend,
)
from .workflows import RetrievalSummarizeWorkflow

__version__ = "2.2.0"

__all__ = [
    # Core
    "ResumableWorkflow",
    "WorkflowTask",
    "TaskStatus",
    "ResumptionPoint",
    "AgentFactory",
    "AgentConfig",
    # Storage
    "StorageBackend",
    "SQLiteStorage",
    "PostgresStorage",
    "ClickHouseStorage",
    "S3Storage",
    "CompositeStorage",
    "create_storage_backend",
    # Resumption
    "ResumptionManager",
    "WorkflowStateAnalyzer",
    # Monitoring
    "WorkflowMonitor",
    # Workflows
    "RetrievalSummarizeWorkflow",
    # High-level interface
    "WorkflowManager",
]


# Convenience class for easy usage
class WorkflowManager:
    """High-level interface for managing resumable workflows."""
    
    def __init__(
        self,
        storage: Optional[StorageBackend] = None,
        monitor: Optional[WorkflowMonitor] = None,
        storage_backend_type: Optional[str] = None
    ):
        """Initialize workflow manager.
        
        Args:
            storage: Storage backend (if not provided, creates one based on configuration)
            monitor: Workflow monitor (creates one if not provided)
            storage_backend_type: Type of storage backend ("sqlite", "postgres", "composite")
        """
        from uuid import uuid4
        
        # Create storage backend using factory if not provided
        if storage is None:
            try:
                self.storage = create_storage_backend(storage_backend_type)
            except Exception as e:
                logger.warning(f"Failed to create configured storage backend: {e}")
                logger.warning("Falling back to SQLite storage")
                self.storage = SQLiteStorage()
        else:
            self.storage = storage
        
        self.monitor = monitor or WorkflowMonitor(self.storage)
        self.resumption_manager = ResumptionManager(self.storage)
        
        # Register default workflows
        self.resumption_manager.register_workflow(
            "retrieval_summarize",
            RetrievalSummarizeWorkflow
        )
        
        self._initialized = False
        self._running_workflows = set()  # Track running workflow tasks
    
    async def initialize(self) -> None:
        """Initialize the workflow manager."""
        if self._initialized:
            return
        
        await self.storage.initialize()
        await self.monitor.start()
        self._initialized = True
    
    async def _run_workflow(self, workflow) -> None:
        """Run a workflow and handle errors."""
        try:
            await workflow.run()
        except Exception as e:
            logger.error(f"Workflow error: {e}", exc_info=True)
    
    async def close(self) -> None:
        """Close the workflow manager."""
        # Wait for all running workflows to complete
        if self._running_workflows:
            await asyncio.gather(*self._running_workflows, return_exceptions=True)
        
        await self.monitor.stop()
        await self.storage.close()
        self._initialized = False
    
    async def start_workflow(
        self,
        user_id: str,
        query: str,
        session_id: Optional[str] = None,
        workflow_type: str = "retrieval_summarize"
    ) -> str:
        """Start a new workflow.
        
        Args:
            user_id: User ID
            query: User query
            session_id: Optional session ID
            workflow_type: Type of workflow to start
            
        Returns:
            Task ID for the workflow
        """
        from uuid import uuid4
        
        if not self._initialized:
            await self.initialize()
        
        task_id = uuid4()
        session_id = session_id or str(uuid4())
        
        # Get workflow class
        workflow_class = self.resumption_manager._workflow_registry.get(workflow_type)
        if not workflow_class:
            raise ValueError(f"Unknown workflow type: {workflow_type}")
        
        # Create and run workflow
        workflow = workflow_class(
            task_id=task_id,
            user_id=user_id,
            session_id=session_id,
            query=query,
            storage=self.storage,
            monitor=self.monitor
        )
        
        # Start workflow asynchronously and track it
        task = asyncio.create_task(self._run_workflow(workflow))
        self._running_workflows.add(task)
        task.add_done_callback(self._running_workflows.discard)
        
        return str(task_id)
    
    async def resume_workflow(
        self,
        task_id: str,
        resumption_point: Optional[str] = None
    ) -> Dict[str, Any]:
        """Resume a workflow.
        
        Args:
            task_id: Task ID to resume
            resumption_point: Optional specific resumption point
            
        Returns:
            Resumption result
        """
        from uuid import UUID
        
        if not self._initialized:
            await self.initialize()
        
        point = ResumptionPoint(resumption_point) if resumption_point else None
        
        return await self.resumption_manager.resume_workflow(
            UUID(task_id),
            point,
            workflow_type="retrieval_summarize",
            monitor=self.monitor
        )
    
    async def get_task_status(self, task_id: str) -> Optional[WorkflowTask]:
        """Get task status.
        
        Args:
            task_id: Task ID
            
        Returns:
            Task information or None
        """
        from uuid import UUID
        
        if not self._initialized:
            await self.initialize()
        
        return await self.storage.get_task(UUID(task_id))
    
    async def list_resumable_tasks(
        self,
        user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """List resumable tasks.
        
        Args:
            user_id: Optional user filter
            
        Returns:
            Summary of resumable tasks
        """
        if not self._initialized:
            await self.initialize()
        
        return await self.resumption_manager.list_resumable_workflows(user_id)
    
    async def analyze_task(self, task_id: str) -> Dict[str, Any]:
        """Analyze a task for resumption.
        
        Args:
            task_id: Task ID
            
        Returns:
            Detailed analysis
        """
        from uuid import UUID
        
        if not self._initialized:
            await self.initialize()
        
        return await self.resumption_manager.get_resumption_details(UUID(task_id))
    
    # Enhanced methods for composite storage
    
    async def get_analytics(
        self,
        start_date=None,
        end_date=None,
        event_types: Optional[list] = None
    ) -> Dict[str, Any]:
        """Get workflow analytics (requires ClickHouse backend).
        
        Args:
            start_date: Start date for analytics
            end_date: End date for analytics
            event_types: Optional list of event types to filter
            
        Returns:
            Analytics data or empty dict if not supported
        """
        if not self._initialized:
            await self.initialize()
        
        # Check if storage supports analytics
        if hasattr(self.storage, 'get_event_analytics'):
            try:
                from datetime import datetime, timedelta
                
                # Default to last 7 days if no dates provided
                if not end_date:
                    end_date = datetime.utcnow()
                if not start_date:
                    start_date = end_date - timedelta(days=7)
                
                return await self.storage.get_event_analytics(
                    start_date, end_date, event_types
                )
            except Exception as e:
                logger.error(f"Failed to get analytics: {e}")
                return {}
        
        logger.warning("Analytics not supported by current storage backend")
        return {}
    
    async def get_performance_metrics(
        self,
        start_date=None,
        end_date=None
    ) -> Dict[str, Any]:
        """Get performance metrics (requires ClickHouse backend).
        
        Args:
            start_date: Start date for metrics
            end_date: End date for metrics
            
        Returns:
            Performance metrics or empty dict if not supported
        """
        if not self._initialized:
            await self.initialize()
        
        if hasattr(self.storage, 'get_performance_analytics'):
            try:
                from datetime import datetime, timedelta
                
                # Default to last 24 hours if no dates provided
                if not end_date:
                    end_date = datetime.utcnow()
                if not start_date:
                    start_date = end_date - timedelta(hours=24)
                
                return await self.storage.get_performance_analytics(start_date, end_date)
            except Exception as e:
                logger.error(f"Failed to get performance metrics: {e}")
                return {}
        
        logger.warning("Performance metrics not supported by current storage backend")
        return {}
    
    async def health_check(self) -> Dict[str, Any]:
        """Check health of all storage backends.
        
        Returns:
            Health status of all storage systems
        """
        if not self._initialized:
            await self.initialize()
        
        if hasattr(self.storage, 'health_check'):
            try:
                return await self.storage.health_check()
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                return {"error": str(e)}
        
        # Basic health check for simple storage backends
        try:
            await self.storage.get_resumable_tasks(limit=1)
            return {"storage": {"status": "healthy"}}
        except Exception as e:
            return {"storage": {"status": "unhealthy", "error": str(e)}}
    
    def get_storage_info(self) -> Dict[str, Any]:
        """Get information about the configured storage backend.
        
        Returns:
            Storage backend information and capabilities
        """
        storage_type = type(self.storage).__name__
        
        # Enhanced info for composite storage
        if hasattr(self.storage, 'get_backend_info'):
            return {
                "storage_type": storage_type,
                **self.storage.get_backend_info()
            }
        
        # Basic info for simple storage backends
        return {
            "storage_type": storage_type,
            "capabilities": {
                "transactional_operations": True,
                "analytics": False,
                "blob_storage": False,
                "high_availability": storage_type != "SQLiteStorage"
            }
        }
    
    @classmethod
    def create_development(cls) -> "WorkflowManager":
        """Create a WorkflowManager optimized for development.
        
        Returns:
            WorkflowManager with SQLite storage
        """
        return cls(storage_backend_type="sqlite")
    
    @classmethod
    def create_production(cls) -> "WorkflowManager":
        """Create a WorkflowManager optimized for production.
        
        Returns:
            WorkflowManager with PostgreSQL or Composite storage
        """
        return cls(storage_backend_type="postgres")
    
    @classmethod
    def create_enterprise(cls) -> "WorkflowManager":
        """Create a WorkflowManager with full enterprise features.
        
        Returns:
            WorkflowManager with Composite storage (PostgreSQL + ClickHouse + S3)
        """
        return cls(storage_backend_type="composite")