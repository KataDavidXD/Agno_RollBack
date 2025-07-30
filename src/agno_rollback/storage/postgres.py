"""PostgreSQL storage backend implementation.

This implementation handles OLTP operations (tasks, workflow states, sessions).
Uses asyncpg for high-performance async PostgreSQL operations.
"""

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from uuid import UUID

import asyncpg

from ..core.models import SessionContext, TaskStatus, WorkflowState, WorkflowTask
from .base import StorageBackend


class PostgresStorage(StorageBackend):
    """PostgreSQL storage backend for OLTP operations."""
    
    def __init__(self, dsn: Optional[str] = None):
        """Initialize PostgreSQL storage.
        
        Args:
            dsn: PostgreSQL connection string (defaults to env variables)
        """
        self.dsn = dsn or os.getenv(
            "POSTGRES_DSN", 
            "postgresql://user:password@localhost:5432/agno_rollback"
        )
        self.pool: Optional[asyncpg.Pool] = None
    
    async def initialize(self) -> None:
        """Initialize connection pool and create tables."""
        self.pool = await asyncpg.create_pool(
            self.dsn,
            min_size=2,
            max_size=10,
            command_timeout=60
        )
        await self._create_tables()
    
    async def close(self) -> None:
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            self.pool = None
    
    async def _create_tables(self) -> None:
        """Create database tables and indexes."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS workflow_tasks (
                    task_id UUID PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    query TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    started_at TIMESTAMP WITH TIME ZONE,
                    completed_at TIMESTAMP WITH TIME ZONE,
                    progress REAL DEFAULT 0.0,
                    result TEXT,
                    error TEXT,
                    current_step TEXT,
                    workflow_events JSONB DEFAULT '[]'::jsonb,
                    agent_outputs JSONB DEFAULT '[]'::jsonb,
                    tool_results JSONB DEFAULT '[]'::jsonb,
                    step_results JSONB DEFAULT '[]'::jsonb,
                    web_retrieval_results JSONB,
                    news_retrieval_results JSONB,
                    created_at_date DATE GENERATED ALWAYS AS (created_at::date) STORED
                );
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tasks_user_session 
                ON workflow_tasks(user_id, session_id);
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tasks_status 
                ON workflow_tasks(status);
            """)
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tasks_created_date 
                ON workflow_tasks(created_at_date);
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS workflow_states (
                    task_id UUID PRIMARY KEY REFERENCES workflow_tasks(task_id) ON DELETE CASCADE,
                    last_checkpoint TEXT NOT NULL,
                    checkpoint_data JSONB,
                    available_resumption_points JSONB DEFAULT '[]'::jsonb,
                    recommended_resumption_point TEXT,
                    web_retrieval_completed BOOLEAN DEFAULT false,
                    news_retrieval_completed BOOLEAN DEFAULT false,
                    summarization_started BOOLEAN DEFAULT false,
                    summarization_completed BOOLEAN DEFAULT false,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    user_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    context_data JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    PRIMARY KEY (user_id, session_id)
                );
            """)
    
    # Task Management Methods
    
    async def create_task(self, task: WorkflowTask) -> WorkflowTask:
        """Create a new workflow task."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO workflow_tasks (
                    task_id, user_id, session_id, query, status, created_at,
                    started_at, completed_at, progress, result, error, current_step,
                    workflow_events, agent_outputs, tool_results, step_results,
                    web_retrieval_results, news_retrieval_results
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
                )
            """, 
                task.task_id, task.user_id, task.session_id, task.query,
                task.status.value, task.created_at, task.started_at, task.completed_at,
                task.progress, task.result, task.error, task.current_step,
                json.dumps([e.to_dict() for e in task.workflow_events]),
                json.dumps([o.to_dict() for o in task.agent_outputs]),
                json.dumps([t.to_dict() for t in task.tool_results]),
                json.dumps([s.to_dict() for s in task.step_results]),
                json.dumps(task.web_retrieval_results) if task.web_retrieval_results else None,
                json.dumps(task.news_retrieval_results) if task.news_retrieval_results else None
            )
        return task
    
    async def get_task(self, task_id: UUID) -> Optional[WorkflowTask]:
        """Get a task by ID."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM workflow_tasks WHERE task_id = $1
            """, task_id)
            
            if not row:
                return None
            
            return self._row_to_task(row)
    
    async def update_task(self, task: WorkflowTask) -> WorkflowTask:
        """Update an existing task."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE workflow_tasks SET
                    status = $2, started_at = $3, completed_at = $4,
                    progress = $5, result = $6, error = $7, current_step = $8,
                    workflow_events = $9, agent_outputs = $10, tool_results = $11,
                    step_results = $12, web_retrieval_results = $13, news_retrieval_results = $14
                WHERE task_id = $1
            """,
                task.task_id, task.status.value, task.started_at, task.completed_at,
                task.progress, task.result, task.error, task.current_step,
                json.dumps([e.to_dict() for e in task.workflow_events]),
                json.dumps([o.to_dict() for o in task.agent_outputs]),
                json.dumps([t.to_dict() for t in task.tool_results]),
                json.dumps([s.to_dict() for s in task.step_results]),
                json.dumps(task.web_retrieval_results) if task.web_retrieval_results else None,
                json.dumps(task.news_retrieval_results) if task.news_retrieval_results else None
            )
        return task
    
    async def list_tasks(
        self,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[WorkflowTask]:
        """List tasks with optional filters."""
        conditions = []
        params = []
        param_count = 0
        
        if user_id:
            param_count += 1
            conditions.append(f"user_id = ${param_count}")
            params.append(user_id)
            
        if session_id:
            param_count += 1
            conditions.append(f"session_id = ${param_count}")
            params.append(session_id)
            
        if status:
            param_count += 1
            conditions.append(f"status = ${param_count}")
            params.append(status)
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        param_count += 1
        limit_clause = f"LIMIT ${param_count}"
        params.append(limit)
        
        param_count += 1
        offset_clause = f"OFFSET ${param_count}"
        params.append(offset)
        
        query = f"""
            SELECT * FROM workflow_tasks
            {where_clause}
            ORDER BY created_at DESC
            {limit_clause} {offset_clause}
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [self._row_to_task(row) for row in rows]
    
    async def delete_task(self, task_id: UUID) -> bool:
        """Delete a task by ID."""
        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM workflow_tasks WHERE task_id = $1
            """, task_id)
            return result.split()[-1] == "1"
    
    # Workflow State Methods
    
    async def save_workflow_state(self, state: WorkflowState) -> WorkflowState:
        """Save workflow execution state."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO workflow_states (
                    task_id, last_checkpoint, checkpoint_data,
                    available_resumption_points, recommended_resumption_point,
                    web_retrieval_completed, news_retrieval_completed,
                    summarization_started, summarization_completed
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (task_id) DO UPDATE SET
                    last_checkpoint = EXCLUDED.last_checkpoint,
                    checkpoint_data = EXCLUDED.checkpoint_data,
                    available_resumption_points = EXCLUDED.available_resumption_points,
                    recommended_resumption_point = EXCLUDED.recommended_resumption_point,
                    web_retrieval_completed = EXCLUDED.web_retrieval_completed,
                    news_retrieval_completed = EXCLUDED.news_retrieval_completed,
                    summarization_started = EXCLUDED.summarization_started,
                    summarization_completed = EXCLUDED.summarization_completed,
                    updated_at = NOW()
            """,
                state.task_id, state.last_checkpoint, json.dumps(state.checkpoint_data),
                json.dumps([p.value for p in state.available_resumption_points]),
                state.recommended_resumption_point.value if state.recommended_resumption_point else None,
                state.web_retrieval_completed, state.news_retrieval_completed,
                state.summarization_started, state.summarization_completed
            )
        return state
    
    async def get_workflow_state(self, task_id: UUID) -> Optional[WorkflowState]:
        """Get workflow state by task ID."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM workflow_states WHERE task_id = $1
            """, task_id)
            
            if not row:
                return None
            
            return self._row_to_workflow_state(row)
    
    async def update_workflow_state(self, state: WorkflowState) -> WorkflowState:
        """Update workflow state."""
        return await self.save_workflow_state(state)  # Uses UPSERT
    
    # Session Management Methods
    
    async def get_session(self, user_id: str, session_id: str) -> Optional[SessionContext]:
        """Get session context."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM sessions WHERE user_id = $1 AND session_id = $2
            """, user_id, session_id)
            
            if not row:
                return None
            
            return SessionContext(
                user_id=row['user_id'],
                session_id=row['session_id'],
                context_data=row['context_data']
            )
    
    async def save_session(self, session: SessionContext) -> SessionContext:
        """Save or update session context."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO sessions (user_id, session_id, context_data)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id, session_id) DO UPDATE SET
                    context_data = EXCLUDED.context_data,
                    updated_at = NOW()
            """, session.user_id, session.session_id, json.dumps(session.context_data))
        return session
    
    async def delete_session(self, user_id: str, session_id: str) -> bool:
        """Delete a session."""
        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM sessions WHERE user_id = $1 AND session_id = $2
            """, user_id, session_id)
            return result.split()[-1] == "1"
    
    # Utility Methods
    
    async def get_resumable_tasks(
        self,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get all tasks that can be resumed."""
        conditions = ["status IN ('failed', 'cancelled', 'running')"]
        params = []
        param_count = 0
        
        if user_id:
            param_count += 1
            conditions.append(f"user_id = ${param_count}")
            params.append(user_id)
            
        if session_id:
            param_count += 1
            conditions.append(f"session_id = ${param_count}")
            params.append(session_id)
        
        where_clause = f"WHERE {' AND '.join(conditions)}"
        
        query = f"""
            SELECT t.*, ws.recommended_resumption_point
            FROM workflow_tasks t
            LEFT JOIN workflow_states ws ON t.task_id = ws.task_id
            {where_clause}
            ORDER BY t.created_at DESC
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            
            return [{
                "task_id": str(row['task_id']),
                "user_id": row['user_id'],
                "session_id": row['session_id'],
                "query": row['query'][:100],
                "status": row['status'],
                "progress": row['progress'],
                "created_at": row['created_at'].isoformat(),
                "error": row['error'],
                "recommended_resumption": row['recommended_resumption_point']
            } for row in rows]
    
    async def get_task_execution_history(self, task_id: UUID) -> Dict[str, Any]:
        """Get complete execution history for a task."""
        async with self.pool.acquire() as conn:
            task_row = await conn.fetchrow("""
                SELECT * FROM workflow_tasks WHERE task_id = $1
            """, task_id)
            
            if not task_row:
                return {}
            
            state_row = await conn.fetchrow("""
                SELECT * FROM workflow_states WHERE task_id = $1
            """, task_id)
            
            return {
                "task": dict(task_row),
                "workflow_state": dict(state_row) if state_row else None,
                "summary": {
                    "total_events": len(json.loads(task_row['workflow_events'] or '[]')),
                    "total_agent_outputs": len(json.loads(task_row['agent_outputs'] or '[]')),
                    "total_tool_results": len(json.loads(task_row['tool_results'] or '[]')),
                    "has_web_results": task_row['web_retrieval_results'] is not None,
                    "has_news_results": task_row['news_retrieval_results'] is not None
                }
            }
    
    async def cleanup_old_tasks(self, days: int = 30) -> int:
        """Clean up tasks older than specified days."""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM workflow_tasks 
                WHERE created_at < $1 AND status IN ('completed', 'failed', 'cancelled')
            """, cutoff_date)
            return int(result.split()[-1])
    
    def _row_to_task(self, row) -> WorkflowTask:
        """Convert database row to WorkflowTask."""
        from ..core.models import AgentOutput, ToolResult, WorkflowEvent, StepResult
        
        # Parse JSON fields
        workflow_events = [
            WorkflowEvent.from_dict(e) 
            for e in json.loads(row['workflow_events'] or '[]')
        ]
        agent_outputs = [
            AgentOutput.from_dict(o) 
            for o in json.loads(row['agent_outputs'] or '[]')
        ]
        tool_results = [
            ToolResult.from_dict(t) 
            for t in json.loads(row['tool_results'] or '[]')
        ]
        step_results = [
            StepResult.from_dict(s) 
            for s in json.loads(row['step_results'] or '[]')
        ]
        
        return WorkflowTask(
            task_id=row['task_id'],
            user_id=row['user_id'],
            session_id=row['session_id'],
            query=row['query'],
            status=TaskStatus(row['status']),
            created_at=row['created_at'],
            started_at=row['started_at'],
            completed_at=row['completed_at'],
            progress=row['progress'],
            result=row['result'],
            error=row['error'],
            current_step=row['current_step'],
            workflow_events=workflow_events,
            agent_outputs=agent_outputs,
            tool_results=tool_results,
            step_results=step_results,
            web_retrieval_results=json.loads(row['web_retrieval_results']) if row['web_retrieval_results'] else None,
            news_retrieval_results=json.loads(row['news_retrieval_results']) if row['news_retrieval_results'] else None
        )
    
    def _row_to_workflow_state(self, row) -> WorkflowState:
        """Convert database row to WorkflowState."""
        from ..core.models import ResumptionPoint
        
        available_points = [
            ResumptionPoint(p) 
            for p in json.loads(row['available_resumption_points'] or '[]')
        ]
        recommended_point = (
            ResumptionPoint(row['recommended_resumption_point']) 
            if row['recommended_resumption_point'] else None
        )
        
        state = WorkflowState(
            task_id=row['task_id'],
            last_checkpoint=row['last_checkpoint']
        )
        
        state.checkpoint_data = json.loads(row['checkpoint_data'] or '{}')
        state.available_resumption_points = available_points
        state.recommended_resumption_point = recommended_point
        state.web_retrieval_completed = row['web_retrieval_completed']
        state.news_retrieval_completed = row['news_retrieval_completed']
        state.summarization_started = row['summarization_started']
        state.summarization_completed = row['summarization_completed']
        
        return state 