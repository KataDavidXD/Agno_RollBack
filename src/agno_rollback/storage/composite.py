"""Composite storage backend implementation.

This implementation orchestrates between multiple storage backends:
- PostgreSQL for OLTP operations (tasks, workflow states, sessions)
- ClickHouse for OLAP operations (events, monitoring, analytics)
- S3 for blob storage (large attachments, raw data, binary content)
"""

import os
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from ..core.models import SessionContext, WorkflowState, WorkflowTask
from .base import StorageBackend
from .postgres import PostgresStorage
from .clickhouse import ClickHouseStorage
from .s3 import S3Storage


class CompositeStorage(StorageBackend):
    """Composite storage backend that orchestrates multiple storage systems.
    
    This class provides a unified interface while routing different types of
    operations to their optimal storage backends:
    - Transactional data → PostgreSQL
    - Analytics data → ClickHouse
    - Blob data → S3
    """
    
    def __init__(
        self,
        postgres_dsn: Optional[str] = None,
        clickhouse_config: Optional[Dict[str, Any]] = None,
        s3_config: Optional[Dict[str, Any]] = None,
        enable_clickhouse: bool = True,
        enable_s3: bool = True
    ):
        """Initialize composite storage with multiple backends.
        
        Args:
            postgres_dsn: PostgreSQL connection string
            clickhouse_config: ClickHouse configuration dict
            s3_config: S3 configuration dict
            enable_clickhouse: Whether to enable ClickHouse backend
            enable_s3: Whether to enable S3 backend
        """
        # Primary backend (required) - PostgreSQL for OLTP
        self.postgres = PostgresStorage(dsn=postgres_dsn)
        
        # Secondary backends (optional)
        self.clickhouse = None
        self.s3 = None
        
        if enable_clickhouse:
            ch_config = clickhouse_config or {}
            self.clickhouse = ClickHouseStorage(
                primary_backend=self.postgres,
                **ch_config
            )
        
        if enable_s3:
            s3_conf = s3_config or {}
            self.s3 = S3Storage(
                primary_backend=self.postgres,
                **s3_conf
            )
    
    @classmethod
    def from_environment(cls) -> "CompositeStorage":
        """Create composite storage from environment variables.
        
        Environment variables:
        - POSTGRES_DSN: PostgreSQL connection string
        - ENABLE_CLICKHOUSE: Enable ClickHouse backend (default: true)
        - ENABLE_S3: Enable S3 backend (default: true)
        - CLICKHOUSE_*: ClickHouse configuration
        - S3_*: S3 configuration
        """
        # Read configuration from environment
        postgres_dsn = os.getenv("POSTGRES_DSN")
        enable_clickhouse = os.getenv("ENABLE_CLICKHOUSE", "true").lower() == "true"
        enable_s3 = os.getenv("ENABLE_S3", "true").lower() == "true"
        
        # ClickHouse configuration
        clickhouse_config = {}
        if enable_clickhouse:
            clickhouse_config = {
                "host": os.getenv("CLICKHOUSE_HOST"),
                "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
                "database": os.getenv("CLICKHOUSE_DATABASE"),
                "username": os.getenv("CLICKHOUSE_USERNAME"),
                "password": os.getenv("CLICKHOUSE_PASSWORD")
            }
        
        # S3 configuration
        s3_config = {}
        if enable_s3:
            s3_config = {
                "bucket_name": os.getenv("S3_BUCKET_NAME"),
                "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
                "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
                "region_name": os.getenv("AWS_DEFAULT_REGION"),
                "endpoint_url": os.getenv("S3_ENDPOINT_URL")
            }
        
        return cls(
            postgres_dsn=postgres_dsn,
            clickhouse_config=clickhouse_config,
            s3_config=s3_config,
            enable_clickhouse=enable_clickhouse,
            enable_s3=enable_s3
        )
    
    async def initialize(self) -> None:
        """Initialize all storage backends."""
        # Initialize primary backend first
        await self.postgres.initialize()
        
        # Initialize secondary backends
        if self.clickhouse:
            try:
                await self.clickhouse.initialize()
            except Exception as e:
                print(f"Warning: Failed to initialize ClickHouse: {e}")
                self.clickhouse = None
        
        if self.s3:
            try:
                await self.s3.initialize()
            except Exception as e:
                print(f"Warning: Failed to initialize S3: {e}")
                self.s3 = None
    
    async def close(self) -> None:
        """Close all storage backends."""
        await self.postgres.close()
        
        if self.clickhouse:
            await self.clickhouse.close()
        
        if self.s3:
            await self.s3.close()
    
    # Task Management Methods (PostgreSQL primary)
    
    async def create_task(self, task: WorkflowTask) -> WorkflowTask:
        """Create a new workflow task."""
        # Primary: PostgreSQL
        result = await self.postgres.create_task(task)
        
        # Secondary: ClickHouse event logging
        if self.clickhouse:
            try:
                await self.clickhouse.add_workflow_event(
                    task.task_id, task.user_id, task.session_id,
                    "task_created", {
                        "query": task.query[:100],
                        "status": task.status.value
                    }
                )
            except Exception as e:
                print(f"Warning: Failed to log task creation to ClickHouse: {e}")
        
        return result
    
    async def get_task(self, task_id: UUID) -> Optional[WorkflowTask]:
        """Get a task by ID."""
        return await self.postgres.get_task(task_id)
    
    async def update_task(self, task: WorkflowTask) -> WorkflowTask:
        """Update an existing task."""
        # Primary: PostgreSQL
        result = await self.postgres.update_task(task)
        
        # Secondary: ClickHouse event logging
        if self.clickhouse:
            try:
                await self.clickhouse.add_workflow_event(
                    task.task_id, task.user_id, task.session_id,
                    "task_updated", {
                        "status": task.status.value,
                        "progress": task.progress
                    }
                )
            except Exception as e:
                print(f"Warning: Failed to log task update to ClickHouse: {e}")
        
        return result
    
    async def list_tasks(
        self,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[WorkflowTask]:
        """List tasks with optional filters."""
        return await self.postgres.list_tasks(user_id, session_id, status, limit, offset)
    
    async def delete_task(self, task_id: UUID) -> bool:
        """Delete a task by ID."""
        # Primary: PostgreSQL
        result = await self.postgres.delete_task(task_id)
        
        if result:
            # Secondary: Clean up ClickHouse events (optional)
            if self.clickhouse:
                try:
                    await self.clickhouse.add_workflow_event(
                        task_id, "system", "system",
                        "task_deleted", {"task_id": str(task_id)}
                    )
                except Exception as e:
                    print(f"Warning: Failed to log task deletion to ClickHouse: {e}")
            
            # Secondary: Clean up S3 blobs
            if self.s3:
                try:
                    await self.s3.cleanup_task_blobs(task_id)
                except Exception as e:
                    print(f"Warning: Failed to clean up S3 blobs: {e}")
        
        return result
    
    # Workflow State Methods (PostgreSQL primary)
    
    async def save_workflow_state(self, state: WorkflowState) -> WorkflowState:
        """Save workflow execution state."""
        return await self.postgres.save_workflow_state(state)
    
    async def get_workflow_state(self, task_id: UUID) -> Optional[WorkflowState]:
        """Get workflow state by task ID."""
        return await self.postgres.get_workflow_state(task_id)
    
    async def update_workflow_state(self, state: WorkflowState) -> WorkflowState:
        """Update workflow state."""
        return await self.postgres.update_workflow_state(state)
    
    # Session Management Methods (PostgreSQL primary)
    
    async def get_session(self, user_id: str, session_id: str) -> Optional[SessionContext]:
        """Get session context."""
        return await self.postgres.get_session(user_id, session_id)
    
    async def save_session(self, session: SessionContext) -> SessionContext:
        """Save or update session context."""
        return await self.postgres.save_session(session)
    
    async def delete_session(self, user_id: str, session_id: str) -> bool:
        """Delete a session."""
        return await self.postgres.delete_session(user_id, session_id)
    
    # Utility Methods
    
    async def get_resumable_tasks(
        self,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get all tasks that can be resumed."""
        return await self.postgres.get_resumable_tasks(user_id, session_id)
    
    async def get_task_execution_history(self, task_id: UUID) -> Dict[str, Any]:
        """Get complete execution history for a task with data from all backends."""
        # Primary: PostgreSQL data
        history = await self.postgres.get_task_execution_history(task_id)
        
        # Secondary: ClickHouse analytics
        if self.clickhouse:
            try:
                ch_history = await self.clickhouse.get_task_execution_history(task_id)
                history.update(ch_history)
            except Exception as e:
                print(f"Warning: Failed to get ClickHouse analytics: {e}")
        
        # Secondary: S3 blob information
        if self.s3:
            try:
                s3_history = await self.s3.get_task_execution_history(task_id)
                history.update(s3_history)
            except Exception as e:
                print(f"Warning: Failed to get S3 blob information: {e}")
        
        return history
    
    async def cleanup_old_tasks(self, days: int = 30) -> int:
        """Clean up tasks older than specified days from all backends."""
        # Primary: PostgreSQL cleanup
        deleted_count = await self.postgres.cleanup_old_tasks(days)
        
        # Secondary: ClickHouse cleanup (handled by TTL, but can be manual)
        if self.clickhouse:
            try:
                await self.clickhouse.cleanup_old_tasks(days)
            except Exception as e:
                print(f"Warning: Failed to clean up ClickHouse data: {e}")
        
        # Secondary: S3 cleanup (handled by lifecycle, but can be manual)
        if self.s3:
            try:
                await self.s3.cleanup_old_tasks(days)
            except Exception as e:
                print(f"Warning: Failed to clean up S3 blobs: {e}")
        
        return deleted_count
    
    # Extended methods for multi-backend operations
    
    async def add_agent_output(
        self,
        task_id: UUID,
        agent_name: str,
        output_content: str,
        metadata: Optional[Dict[str, Any]] = None,
        execution_time_ms: Optional[int] = None
    ) -> Dict[str, Any]:
        """Add agent output with multi-backend storage.
        
        Large outputs go to S3, metadata goes to ClickHouse.
        """
        result = {"stored_in": []}
        
        # Store large content in S3 if available and content is large
        if self.s3 and len(output_content) > 10000:  # 10KB threshold
            try:
                blob_info = await self.s3.store_agent_output_blob(
                    task_id, agent_name, output_content, metadata
                )
                result["s3_blob"] = blob_info
                result["stored_in"].append("s3")
            except Exception as e:
                print(f"Warning: Failed to store agent output in S3: {e}")
        
        # Store metadata and performance data in ClickHouse
        if self.clickhouse and execution_time_ms:
            try:
                await self.clickhouse.add_agent_output(
                    task_id, agent_name, 
                    output_content[:1000] if len(output_content) > 1000 else output_content,
                    metadata or {}, execution_time_ms
                )
                result["stored_in"].append("clickhouse")
            except Exception as e:
                print(f"Warning: Failed to store agent output in ClickHouse: {e}")
        
        return result
    
    async def add_tool_result(
        self,
        task_id: UUID,
        tool_name: str,
        success: bool,
        execution_time_ms: int,
        input_data: Optional[Union[str, bytes]] = None,
        output_data: Optional[Union[str, bytes]] = None,
        error_message: str = ""
    ) -> Dict[str, Any]:
        """Add tool result with multi-backend storage."""
        result = {"stored_in": []}
        
        # Store large data in S3
        if self.s3 and output_data and len(str(output_data)) > 5000:  # 5KB threshold
            try:
                content_type = "application/octet-stream"
                if isinstance(output_data, str):
                    content_type = "text/plain"
                
                blob_info = await self.s3.store_tool_result_blob(
                    task_id, tool_name, output_data, content_type
                )
                result["s3_blob"] = blob_info
                result["stored_in"].append("s3")
            except Exception as e:
                print(f"Warning: Failed to store tool result in S3: {e}")
        
        # Store performance data in ClickHouse
        if self.clickhouse:
            try:
                input_size = len(str(input_data)) if input_data else 0
                output_size = len(str(output_data)) if output_data else 0
                
                await self.clickhouse.add_tool_result(
                    task_id, tool_name, success, execution_time_ms,
                    input_size, output_size, error_message
                )
                result["stored_in"].append("clickhouse")
            except Exception as e:
                print(f"Warning: Failed to store tool result in ClickHouse: {e}")
        
        return result
    
    async def save_checkpoint_with_blobs(
        self,
        state: WorkflowState,
        large_checkpoint_data: Optional[Dict[str, Any]] = None
    ) -> WorkflowState:
        """Save workflow state with large checkpoint data in S3."""
        # Save large checkpoint data to S3 if available
        if self.s3 and large_checkpoint_data:
            try:
                blob_info = await self.s3.store_checkpoint_blob(
                    state.task_id, state.last_checkpoint, large_checkpoint_data
                )
                # Store reference to blob in state
                state.checkpoint_data = state.checkpoint_data or {}
                state.checkpoint_data["s3_blob_reference"] = blob_info
            except Exception as e:
                print(f"Warning: Failed to store checkpoint blob in S3: {e}")
        
        # Save state to PostgreSQL
        return await self.postgres.save_workflow_state(state)
    
    # Analytics methods (ClickHouse)
    
    async def get_event_analytics(
        self,
        start_date,
        end_date,
        event_types: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Get event analytics from ClickHouse."""
        if self.clickhouse:
            try:
                return await self.clickhouse.get_event_analytics(
                    start_date, end_date, event_types
                )
            except Exception as e:
                print(f"Warning: Failed to get event analytics: {e}")
                return {}
        return {}
    
    async def get_performance_analytics(
        self,
        start_date,
        end_date
    ) -> Dict[str, Any]:
        """Get performance analytics from ClickHouse."""
        if self.clickhouse:
            try:
                return await self.clickhouse.get_performance_analytics(start_date, end_date)
            except Exception as e:
                print(f"Warning: Failed to get performance analytics: {e}")
                return {}
        return {}
    
    # Health check methods
    
    async def health_check(self) -> Dict[str, Any]:
        """Check health of all storage backends."""
        health = {
            "postgres": {"status": "unknown", "error": None},
            "clickhouse": {"status": "unknown", "error": None},
            "s3": {"status": "unknown", "error": None}
        }
        
        # Check PostgreSQL
        try:
            await self.postgres.get_resumable_tasks(limit=1)
            health["postgres"]["status"] = "healthy"
        except Exception as e:
            health["postgres"]["status"] = "unhealthy"
            health["postgres"]["error"] = str(e)
        
        # Check ClickHouse
        if self.clickhouse:
            try:
                # Try a simple query
                await self.clickhouse.get_event_analytics(
                    start_date=None, end_date=None, event_types=[]
                )
                health["clickhouse"]["status"] = "healthy"
            except Exception as e:
                health["clickhouse"]["status"] = "unhealthy"
                health["clickhouse"]["error"] = str(e)
        else:
            health["clickhouse"]["status"] = "disabled"
        
        # Check S3
        if self.s3:
            try:
                await self.s3.list_blobs(limit=1)
                health["s3"]["status"] = "healthy"
            except Exception as e:
                health["s3"]["status"] = "unhealthy"
                health["s3"]["error"] = str(e)
        else:
            health["s3"]["status"] = "disabled"
        
        return health
    
    def get_backend_info(self) -> Dict[str, Any]:
        """Get information about configured backends."""
        return {
            "primary_backend": "postgresql",
            "secondary_backends": {
                "clickhouse": self.clickhouse is not None,
                "s3": self.s3 is not None
            },
            "capabilities": {
                "transactional_operations": True,
                "analytics": self.clickhouse is not None,
                "blob_storage": self.s3 is not None,
                "high_availability": True,  # PostgreSQL can be configured for HA
                "auto_scaling": self.clickhouse is not None or self.s3 is not None
            }
        } 