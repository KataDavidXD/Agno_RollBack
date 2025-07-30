"""ClickHouse storage backend implementation.

This implementation handles OLAP operations (events, monitoring, analytics).
Uses clickhouse-connect for high-performance columnar analytics.
"""

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from ..core.models import SessionContext, WorkflowState, WorkflowTask
from .base import StorageBackend


class ClickHouseStorage(StorageBackend):
    """ClickHouse storage backend for OLAP operations.
    
    Note: This backend primarily handles events and analytics.
    For transactional operations, it delegates to a primary storage backend.
    """
    
    def __init__(
        self, 
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        primary_backend: Optional[StorageBackend] = None
    ):
        """Initialize ClickHouse storage.
        
        Args:
            host: ClickHouse host (defaults to env variable)
            port: ClickHouse port (defaults to env variable)
            database: Database name (defaults to env variable)
            username: Username (defaults to env variable)
            password: Password (defaults to env variable)
            primary_backend: Primary storage backend for transactional operations
        """
        self.host = host or os.getenv("CLICKHOUSE_HOST", "localhost")
        self.port = port or int(os.getenv("CLICKHOUSE_PORT", "8123"))
        self.database = database or os.getenv("CLICKHOUSE_DATABASE", "agno_rollback")
        self.username = username or os.getenv("CLICKHOUSE_USERNAME", "default")
        self.password = password or os.getenv("CLICKHOUSE_PASSWORD", "")
        
        self.client = None
        self.primary_backend = primary_backend
    
    async def initialize(self) -> None:
        """Initialize ClickHouse connection and create tables."""
        try:
            import clickhouse_connect
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password,
                compress=True
            )
            await self._create_tables()
        except ImportError:
            raise ImportError(
                "clickhouse-connect is required for ClickHouse backend. "
                "Install it with: pip install clickhouse-connect"
            )
    
    async def close(self) -> None:
        """Close ClickHouse connection."""
        if self.client:
            self.client.close()
            self.client = None
    
    async def _create_tables(self) -> None:
        """Create ClickHouse tables optimized for analytics."""
        
        # Events table for real-time observability
        self.client.command("""
            CREATE TABLE IF NOT EXISTS workflow_events (
                event_id UUID DEFAULT generateUUIDv4(),
                task_id UUID,
                user_id String,
                session_id String,
                event_type String,
                event_data String,
                timestamp DateTime64(3) DEFAULT now64(),
                date Date DEFAULT toDate(timestamp),
                hour UInt8 DEFAULT toHour(timestamp)
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(date)
            ORDER BY (date, event_type, task_id)
            TTL date + INTERVAL 90 DAY
            SETTINGS index_granularity = 8192
        """)
        
        # Agent outputs for performance analytics
        self.client.command("""
            CREATE TABLE IF NOT EXISTS agent_outputs (
                output_id UUID DEFAULT generateUUIDv4(),
                task_id UUID,
                agent_name String,
                output_content String,
                metadata String,
                timestamp DateTime64(3) DEFAULT now64(),
                date Date DEFAULT toDate(timestamp),
                execution_time_ms UInt32
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(date)
            ORDER BY (date, agent_name, task_id)
            TTL date + INTERVAL 30 DAY
            SETTINGS index_granularity = 8192
        """)
        
        # Tool results for usage analytics
        self.client.command("""
            CREATE TABLE IF NOT EXISTS tool_results (
                result_id UUID DEFAULT generateUUIDv4(),
                task_id UUID,
                tool_name String,
                success UInt8,
                execution_time_ms UInt32,
                input_size UInt32,
                output_size UInt32,
                error_message String,
                timestamp DateTime64(3) DEFAULT now64(),
                date Date DEFAULT toDate(timestamp)
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(date)
            ORDER BY (date, tool_name, success, task_id)
            TTL date + INTERVAL 60 DAY
            SETTINGS index_granularity = 8192
        """)
        
        # Monitoring events for system observability
        self.client.command("""
            CREATE TABLE IF NOT EXISTS monitoring_events (
                event_id UUID DEFAULT generateUUIDv4(),
                task_id UUID,
                event_type String,
                component String,
                status String,
                metrics String,
                timestamp DateTime64(3) DEFAULT now64(),
                date Date DEFAULT toDate(timestamp)
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(date)
            ORDER BY (date, component, event_type)
            TTL date + INTERVAL 180 DAY
            SETTINGS index_granularity = 8192
        """)
        
        # Create materialized views for common aggregations
        self.client.command("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS daily_event_stats
            ENGINE = SummingMergeTree()
            PARTITION BY toYYYYMM(date)
            ORDER BY (date, event_type)
            AS SELECT
                date,
                event_type,
                count() as event_count,
                uniq(task_id) as unique_tasks,
                uniq(user_id) as unique_users
            FROM workflow_events
            GROUP BY date, event_type
        """)
        
        self.client.command("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_performance_stats
            ENGINE = SummingMergeTree()
            PARTITION BY toYYYYMM(date)
            ORDER BY (date, hour, tool_name)
            AS SELECT
                date,
                hour,
                tool_name,
                count() as execution_count,
                avg(execution_time_ms) as avg_execution_time,
                quantile(0.95)(execution_time_ms) as p95_execution_time,
                sum(success) as success_count
            FROM tool_results
            GROUP BY date, hour, tool_name
        """)
    
    # Event-focused methods (ClickHouse strengths)
    
    async def add_workflow_event(
        self, 
        task_id: UUID, 
        user_id: str,
        session_id: str,
        event_type: str, 
        event_data: Dict[str, Any]
    ) -> None:
        """Add a workflow event to ClickHouse."""
        self.client.insert('workflow_events', [[
            str(task_id),
            user_id,
            session_id,
            event_type,
            json.dumps(event_data),
            datetime.utcnow()
        ]], column_names=[
            'task_id', 'user_id', 'session_id', 'event_type', 
            'event_data', 'timestamp'
        ])
    
    async def add_agent_output(
        self,
        task_id: UUID,
        agent_name: str,
        output_content: str,
        metadata: Dict[str, Any],
        execution_time_ms: int
    ) -> None:
        """Add agent output to ClickHouse."""
        self.client.insert('agent_outputs', [[
            str(task_id),
            agent_name,
            output_content,
            json.dumps(metadata),
            execution_time_ms,
            datetime.utcnow()
        ]], column_names=[
            'task_id', 'agent_name', 'output_content', 
            'metadata', 'execution_time_ms', 'timestamp'
        ])
    
    async def add_tool_result(
        self,
        task_id: UUID,
        tool_name: str,
        success: bool,
        execution_time_ms: int,
        input_size: int,
        output_size: int,
        error_message: str = ""
    ) -> None:
        """Add tool result to ClickHouse."""
        self.client.insert('tool_results', [[
            str(task_id),
            tool_name,
            1 if success else 0,
            execution_time_ms,
            input_size,
            output_size,
            error_message,
            datetime.utcnow()
        ]], column_names=[
            'task_id', 'tool_name', 'success', 'execution_time_ms',
            'input_size', 'output_size', 'error_message', 'timestamp'
        ])
    
    async def add_monitoring_event(
        self,
        task_id: UUID,
        event_type: str,
        component: str,
        status: str,
        metrics: Dict[str, Any]
    ) -> None:
        """Add monitoring event to ClickHouse."""
        self.client.insert('monitoring_events', [[
            str(task_id),
            event_type,
            component,
            status,
            json.dumps(metrics),
            datetime.utcnow()
        ]], column_names=[
            'task_id', 'event_type', 'component', 
            'status', 'metrics', 'timestamp'
        ])
    
    # Analytics methods
    
    async def get_event_analytics(
        self,
        start_date: datetime,
        end_date: datetime,
        event_types: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Get event analytics for specified date range."""
        where_conditions = [
            f"date >= '{start_date.date()}'",
            f"date <= '{end_date.date()}'"
        ]
        
        if event_types:
            event_list = "', '".join(event_types)
            where_conditions.append(f"event_type IN ('{event_list}')")
        
        where_clause = " AND ".join(where_conditions)
        
        # Get daily event counts
        daily_events = self.client.query(f"""
            SELECT 
                date,
                event_type,
                count() as event_count,
                uniq(task_id) as unique_tasks
            FROM workflow_events
            WHERE {where_clause}
            GROUP BY date, event_type
            ORDER BY date, event_type
        """).result_rows
        
        # Get hourly patterns
        hourly_patterns = self.client.query(f"""
            SELECT 
                hour,
                count() as event_count,
                uniq(task_id) as unique_tasks
            FROM workflow_events
            WHERE {where_clause}
            GROUP BY hour
            ORDER BY hour
        """).result_rows
        
        return {
            "daily_events": [
                {
                    "date": str(row[0]),
                    "event_type": row[1],
                    "event_count": row[2],
                    "unique_tasks": row[3]
                } for row in daily_events
            ],
            "hourly_patterns": [
                {
                    "hour": row[0],
                    "event_count": row[1],
                    "unique_tasks": row[2]
                } for row in hourly_patterns
            ]
        }
    
    async def get_performance_analytics(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """Get performance analytics for tools and agents."""
        where_clause = f"""
            date >= '{start_date.date()}' AND date <= '{end_date.date()}'
        """
        
        # Tool performance
        tool_stats = self.client.query(f"""
            SELECT 
                tool_name,
                count() as total_executions,
                avg(execution_time_ms) as avg_execution_time,
                quantile(0.5)(execution_time_ms) as median_execution_time,
                quantile(0.95)(execution_time_ms) as p95_execution_time,
                sum(success) as success_count,
                sum(success) / count() * 100 as success_rate
            FROM tool_results
            WHERE {where_clause}
            GROUP BY tool_name
            ORDER BY total_executions DESC
        """).result_rows
        
        # Agent performance
        agent_stats = self.client.query(f"""
            SELECT 
                agent_name,
                count() as total_outputs,
                avg(execution_time_ms) as avg_execution_time,
                quantile(0.95)(execution_time_ms) as p95_execution_time
            FROM agent_outputs
            WHERE {where_clause}
            GROUP BY agent_name
            ORDER BY total_outputs DESC
        """).result_rows
        
        return {
            "tool_performance": [
                {
                    "tool_name": row[0],
                    "total_executions": row[1],
                    "avg_execution_time_ms": float(row[2]),
                    "median_execution_time_ms": float(row[3]),
                    "p95_execution_time_ms": float(row[4]),
                    "success_count": row[5],
                    "success_rate": float(row[6])
                } for row in tool_stats
            ],
            "agent_performance": [
                {
                    "agent_name": row[0],
                    "total_outputs": row[1],
                    "avg_execution_time_ms": float(row[2]),
                    "p95_execution_time_ms": float(row[3])
                } for row in agent_stats
            ]
        }
    
    # Delegate transactional operations to primary backend
    
    async def create_task(self, task: WorkflowTask) -> WorkflowTask:
        """Delegate to primary backend."""
        if self.primary_backend:
            result = await self.primary_backend.create_task(task)
            # Async log to ClickHouse
            await self.add_workflow_event(
                task.task_id, task.user_id, task.session_id,
                "task_created", {"query": task.query[:100]}
            )
            return result
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def get_task(self, task_id: UUID) -> Optional[WorkflowTask]:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.get_task(task_id)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def update_task(self, task: WorkflowTask) -> WorkflowTask:
        """Delegate to primary backend."""
        if self.primary_backend:
            result = await self.primary_backend.update_task(task)
            # Async log to ClickHouse
            await self.add_workflow_event(
                task.task_id, task.user_id, task.session_id,
                "task_updated", {"status": task.status.value, "progress": task.progress}
            )
            return result
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def list_tasks(
        self,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[WorkflowTask]:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.list_tasks(user_id, session_id, status, limit, offset)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def delete_task(self, task_id: UUID) -> bool:
        """Delegate to primary backend."""
        if self.primary_backend:
            result = await self.primary_backend.delete_task(task_id)
            if result:
                await self.add_workflow_event(
                    task_id, "system", "system",
                    "task_deleted", {"task_id": str(task_id)}
                )
            return result
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def save_workflow_state(self, state: WorkflowState) -> WorkflowState:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.save_workflow_state(state)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def get_workflow_state(self, task_id: UUID) -> Optional[WorkflowState]:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.get_workflow_state(task_id)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def update_workflow_state(self, state: WorkflowState) -> WorkflowState:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.update_workflow_state(state)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def get_session(self, user_id: str, session_id: str) -> Optional[SessionContext]:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.get_session(user_id, session_id)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def save_session(self, session: SessionContext) -> SessionContext:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.save_session(session)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def delete_session(self, user_id: str, session_id: str) -> bool:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.delete_session(user_id, session_id)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def get_resumable_tasks(
        self,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Delegate to primary backend."""
        if self.primary_backend:
            return await self.primary_backend.get_resumable_tasks(user_id, session_id)
        raise NotImplementedError("Primary backend required for transactional operations")
    
    async def get_task_execution_history(self, task_id: UUID) -> Dict[str, Any]:
        """Enhanced execution history with ClickHouse analytics."""
        base_history = {}
        if self.primary_backend:
            base_history = await self.primary_backend.get_task_execution_history(task_id)
        
        # Add ClickHouse analytics
        events = self.client.query(f"""
            SELECT event_type, count() as count, max(timestamp) as latest
            FROM workflow_events
            WHERE task_id = '{task_id}'
            GROUP BY event_type
            ORDER BY latest DESC
        """).result_rows
        
        tool_usage = self.client.query(f"""
            SELECT tool_name, count() as executions, avg(execution_time_ms) as avg_time
            FROM tool_results
            WHERE task_id = '{task_id}'
            GROUP BY tool_name
            ORDER BY executions DESC
        """).result_rows
        
        base_history.update({
            "clickhouse_analytics": {
                "events_summary": [
                    {"event_type": row[0], "count": row[1], "latest": str(row[2])}
                    for row in events
                ],
                "tool_usage": [
                    {"tool_name": row[0], "executions": row[1], "avg_time_ms": float(row[2])}
                    for row in tool_usage
                ]
            }
        })
        
        return base_history
    
    async def cleanup_old_tasks(self, days: int = 30) -> int:
        """Delegate to primary backend and clean ClickHouse data."""
        deleted_count = 0
        if self.primary_backend:
            deleted_count = await self.primary_backend.cleanup_old_tasks(days)
        
        # ClickHouse uses TTL for automatic cleanup, but we can also manually clean
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # Clean old events (beyond TTL if needed)
        self.client.command(f"""
            ALTER TABLE workflow_events DELETE WHERE date < '{cutoff_date.date()}'
        """)
        
        return deleted_count 