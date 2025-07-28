"""SQLite storage backend implementation.

This implementation uses aiosqlite for async SQLite operations.
It's suitable for development and small-scale deployments.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import UUID

import aiosqlite

from ..core.models import SessionContext, TaskStatus, WorkflowState, WorkflowTask
from .base import StorageBackend


class SQLiteStorage(StorageBackend):
    """SQLite storage backend implementation."""
    
    def __init__(self, db_path: str = "data/workflow_storage.db"):
        """Initialize SQLite storage.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection: Optional[aiosqlite.Connection] = None
    
    async def initialize(self) -> None:
        """Initialize database and create tables."""
        self._connection = await aiosqlite.connect(str(self.db_path))
        await self._create_tables()
    
    async def close(self) -> None:
        """Close database connection."""
        if self._connection:
            await self._connection.close()
            self._connection = None
    
    async def _create_tables(self) -> None:
        """Create database tables."""
        await self._connection.executescript("""
            CREATE TABLE IF NOT EXISTS workflow_tasks (
                task_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                query TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                progress REAL DEFAULT 0.0,
                result TEXT,
                error TEXT,
                current_step TEXT,
                workflow_events TEXT,
                agent_outputs TEXT,
                tool_results TEXT,
                step_results TEXT,
                web_retrieval_results TEXT,
                news_retrieval_results TEXT
            );
            
            CREATE INDEX IF NOT EXISTS idx_user_session ON workflow_tasks(user_id, session_id);
            CREATE INDEX IF NOT EXISTS idx_status ON workflow_tasks(status);
            CREATE INDEX IF NOT EXISTS idx_created_at ON workflow_tasks(created_at);
            
            CREATE TABLE IF NOT EXISTS workflow_states (
                task_id TEXT PRIMARY KEY,
                last_checkpoint TEXT NOT NULL,
                checkpoint_data TEXT,
                available_resumption_points TEXT,
                recommended_resumption_point TEXT,
                web_retrieval_completed BOOLEAN DEFAULT 0,
                news_retrieval_completed BOOLEAN DEFAULT 0,
                summarization_started BOOLEAN DEFAULT 0,
                summarization_completed BOOLEAN DEFAULT 0,
                FOREIGN KEY (task_id) REFERENCES workflow_tasks(task_id)
            );
            
            CREATE TABLE IF NOT EXISTS user_sessions (
                session_key TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                retrieval_history TEXT,
                search_context TEXT,
                preferences TEXT
            );
            
            CREATE INDEX IF NOT EXISTS idx_user_id ON user_sessions(user_id);
        """)
        await self._connection.commit()
    
    # Task Management Methods
    
    async def create_task(self, task: WorkflowTask) -> WorkflowTask:
        """Create a new workflow task."""
        async with self._connection.execute(
            """
            INSERT INTO workflow_tasks (
                task_id, user_id, session_id, query, status, created_at,
                started_at, completed_at, progress, result, error, current_step,
                workflow_events, agent_outputs, tool_results, step_results,
                web_retrieval_results, news_retrieval_results
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(task.task_id),
                task.user_id,
                task.session_id,
                task.query,
                task.status.value,
                task.created_at.isoformat(),
                task.started_at.isoformat() if task.started_at else None,
                task.completed_at.isoformat() if task.completed_at else None,
                task.progress,
                task.result,
                task.error,
                task.current_step,
                json.dumps([json.loads(e.model_dump_json()) for e in task.workflow_events]),
                json.dumps([json.loads(a.model_dump_json()) for a in task.agent_outputs]),
                json.dumps([json.loads(t.model_dump_json()) for t in task.tool_results]),
                json.dumps([json.loads(s.model_dump_json()) for s in task.step_results]),
                json.dumps(task.web_retrieval_results) if task.web_retrieval_results else None,
                json.dumps(task.news_retrieval_results) if task.news_retrieval_results else None,
            )
        ):
            pass
        await self._connection.commit()
        return task
    
    async def get_task(self, task_id: UUID) -> Optional[WorkflowTask]:
        """Get a task by ID."""
        async with self._connection.execute(
            "SELECT * FROM workflow_tasks WHERE task_id = ?",
            (str(task_id),)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return self._row_to_task(row)
        return None
    
    async def update_task(self, task: WorkflowTask) -> WorkflowTask:
        """Update an existing task."""
        async with self._connection.execute(
            """
            UPDATE workflow_tasks SET
                status = ?, started_at = ?, completed_at = ?, progress = ?,
                result = ?, error = ?, current_step = ?, workflow_events = ?,
                agent_outputs = ?, tool_results = ?, step_results = ?,
                web_retrieval_results = ?, news_retrieval_results = ?
            WHERE task_id = ?
            """,
            (
                task.status.value,
                task.started_at.isoformat() if task.started_at else None,
                task.completed_at.isoformat() if task.completed_at else None,
                task.progress,
                task.result,
                task.error,
                task.current_step,
                json.dumps([json.loads(e.model_dump_json()) for e in task.workflow_events]),
                json.dumps([json.loads(a.model_dump_json()) for a in task.agent_outputs]),
                json.dumps([json.loads(t.model_dump_json()) for t in task.tool_results]),
                json.dumps([json.loads(s.model_dump_json()) for s in task.step_results]),
                json.dumps(task.web_retrieval_results) if task.web_retrieval_results else None,
                json.dumps(task.news_retrieval_results) if task.news_retrieval_results else None,
                str(task.task_id),
            )
        ):
            pass
        await self._connection.commit()
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
        query = "SELECT * FROM workflow_tasks WHERE 1=1"
        params = []
        
        if user_id:
            query += " AND user_id = ?"
            params.append(user_id)
        
        if session_id:
            query += " AND session_id = ?"
            params.append(session_id)
        
        if status:
            query += " AND status = ?"
            params.append(status)
        
        query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        tasks = []
        async with self._connection.execute(query, params) as cursor:
            async for row in cursor:
                task = self._row_to_task(row)
                if task:
                    tasks.append(task)
        
        return tasks
    
    async def delete_task(self, task_id: UUID) -> bool:
        """Delete a task by ID."""
        async with self._connection.execute(
            "DELETE FROM workflow_tasks WHERE task_id = ?",
            (str(task_id),)
        ) as cursor:
            deleted = cursor.rowcount > 0
        
        if deleted:
            # Also delete associated state
            async with self._connection.execute(
                "DELETE FROM workflow_states WHERE task_id = ?",
                (str(task_id),)
            ):
                pass
        
        await self._connection.commit()
        return deleted
    
    # Workflow State Methods
    
    async def save_workflow_state(self, state: WorkflowState) -> WorkflowState:
        """Save workflow execution state."""
        async with self._connection.execute(
            """
            INSERT OR REPLACE INTO workflow_states (
                task_id, last_checkpoint, checkpoint_data,
                available_resumption_points, recommended_resumption_point,
                web_retrieval_completed, news_retrieval_completed,
                summarization_started, summarization_completed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(state.task_id),
                state.last_checkpoint,
                json.dumps(state.checkpoint_data),
                json.dumps([p.value for p in state.available_resumption_points]),
                state.recommended_resumption_point.value if state.recommended_resumption_point else None,
                state.web_retrieval_completed,
                state.news_retrieval_completed,
                state.summarization_started,
                state.summarization_completed,
            )
        ):
            pass
        await self._connection.commit()
        return state
    
    async def get_workflow_state(self, task_id: UUID) -> Optional[WorkflowState]:
        """Get workflow state by task ID."""
        async with self._connection.execute(
            "SELECT * FROM workflow_states WHERE task_id = ?",
            (str(task_id),)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return self._row_to_workflow_state(row)
        return None
    
    async def update_workflow_state(self, state: WorkflowState) -> WorkflowState:
        """Update workflow state."""
        return await self.save_workflow_state(state)
    
    # Session Management Methods
    
    async def get_session(self, user_id: str, session_id: str) -> Optional[SessionContext]:
        """Get session context."""
        session_key = f"{user_id}:{session_id}"
        async with self._connection.execute(
            "SELECT * FROM user_sessions WHERE session_key = ?",
            (session_key,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return self._row_to_session(row)
        return None
    
    async def save_session(self, session: SessionContext) -> SessionContext:
        """Save or update session context."""
        session_key = f"{session.user_id}:{session.session_id}"
        async with self._connection.execute(
            """
            INSERT OR REPLACE INTO user_sessions (
                session_key, user_id, session_id, created_at,
                retrieval_history, search_context, preferences
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_key,
                session.user_id,
                session.session_id,
                session.created_at.isoformat(),
                json.dumps(session.retrieval_history),
                json.dumps(session.search_context),
                json.dumps(session.preferences),
            )
        ):
            pass
        await self._connection.commit()
        return session
    
    async def delete_session(self, user_id: str, session_id: str) -> bool:
        """Delete a session."""
        session_key = f"{user_id}:{session_id}"
        async with self._connection.execute(
            "DELETE FROM user_sessions WHERE session_key = ?",
            (session_key,)
        ) as cursor:
            deleted = cursor.rowcount > 0
        await self._connection.commit()
        return deleted
    
    # Utility Methods
    
    async def get_resumable_tasks(
        self,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get all tasks that can be resumed."""
        query = """
            SELECT t.*, s.* FROM workflow_tasks t
            LEFT JOIN workflow_states s ON t.task_id = s.task_id
            WHERE t.status IN ('failed', 'cancelled', 'running')
        """
        params = []
        
        if user_id:
            query += " AND t.user_id = ?"
            params.append(user_id)
        
        if session_id:
            query += " AND t.session_id = ?"
            params.append(session_id)
        
        query += " ORDER BY t.created_at DESC"
        
        resumable_tasks = []
        async with self._connection.execute(query, params) as cursor:
            async for row in cursor:
                task = self._row_to_task(row)
                if task:
                    state = await self.get_workflow_state(task.task_id)
                    if state:
                        state.analyze_resumption_points()
                        recommended = state.get_recommended_resumption_point()
                    
                    resumable_tasks.append({
                        "task_id": str(task.task_id),
                        "user_id": task.user_id,
                        "session_id": task.session_id,
                        "query": task.query,
                        "status": task.status.value,
                        "progress": task.progress,
                        "created_at": task.created_at.isoformat(),
                        "recommended_resumption": recommended.value if state and recommended else None,
                        "available_resumptions": [p.value for p in state.available_resumption_points] if state else [],
                    })
        
        return resumable_tasks
    
    async def get_task_execution_history(self, task_id: UUID) -> Dict[str, Any]:
        """Get complete execution history for a task."""
        task = await self.get_task(task_id)
        if not task:
            return {}
        
        state = await self.get_workflow_state(task_id)
        
        return {
            "task": task.dict(),
            "state": state.dict() if state else None,
            "events_count": len(task.workflow_events),
            "agent_outputs_count": len(task.agent_outputs),
            "tool_results_count": len(task.tool_results),
            "step_results_count": len(task.step_results),
        }
    
    async def cleanup_old_tasks(self, days: int = 30) -> int:
        """Clean up tasks older than specified days."""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # Get task IDs to delete
        task_ids = []
        async with self._connection.execute(
            "SELECT task_id FROM workflow_tasks WHERE created_at < ? AND status IN ('completed', 'failed', 'cancelled')",
            (cutoff_date.isoformat(),)
        ) as cursor:
            async for row in cursor:
                task_ids.append(row[0])
        
        # Delete states
        if task_ids:
            placeholders = ','.join('?' * len(task_ids))
            async with self._connection.execute(
                f"DELETE FROM workflow_states WHERE task_id IN ({placeholders})",
                task_ids
            ):
                pass
        
        # Delete tasks
        async with self._connection.execute(
            "DELETE FROM workflow_tasks WHERE created_at < ? AND status IN ('completed', 'failed', 'cancelled')",
            (cutoff_date.isoformat(),)
        ) as cursor:
            deleted_count = cursor.rowcount
        
        await self._connection.commit()
        return deleted_count
    
    # Helper methods
    
    def _row_to_task(self, row: aiosqlite.Row) -> Optional[WorkflowTask]:
        """Convert database row to WorkflowTask."""
        try:
            return WorkflowTask(
                task_id=UUID(row[0]),
                user_id=row[1],
                session_id=row[2],
                query=row[3],
                status=TaskStatus(row[4]),
                created_at=datetime.fromisoformat(row[5]),
                started_at=datetime.fromisoformat(row[6]) if row[6] else None,
                completed_at=datetime.fromisoformat(row[7]) if row[7] else None,
                progress=row[8],
                result=row[9],
                error=row[10],
                current_step=row[11],
                workflow_events=json.loads(row[12]) if row[12] else [],
                agent_outputs=json.loads(row[13]) if row[13] else [],
                tool_results=json.loads(row[14]) if row[14] else [],
                step_results=json.loads(row[15]) if row[15] else [],
                web_retrieval_results=json.loads(row[16]) if row[16] else None,
                news_retrieval_results=json.loads(row[17]) if row[17] else None,
            )
        except Exception:
            return None
    
    def _row_to_workflow_state(self, row: aiosqlite.Row) -> Optional[WorkflowState]:
        """Convert database row to WorkflowState."""
        try:
            from ..core.models import ResumptionPoint
            
            return WorkflowState(
                task_id=UUID(row[0]),
                last_checkpoint=row[1],
                checkpoint_data=json.loads(row[2]) if row[2] else {},
                available_resumption_points=[ResumptionPoint(p) for p in json.loads(row[3])] if row[3] else [],
                recommended_resumption_point=ResumptionPoint(row[4]) if row[4] else None,
                web_retrieval_completed=bool(row[5]),
                news_retrieval_completed=bool(row[6]),
                summarization_started=bool(row[7]),
                summarization_completed=bool(row[8]),
            )
        except Exception:
            return None
    
    def _row_to_session(self, row: aiosqlite.Row) -> Optional[SessionContext]:
        """Convert database row to SessionContext."""
        try:
            return SessionContext(
                user_id=row[1],
                session_id=row[2],
                created_at=datetime.fromisoformat(row[3]),
                retrieval_history=json.loads(row[4]) if row[4] else [],
                search_context=json.loads(row[5]) if row[5] else {},
                preferences=json.loads(row[6]) if row[6] else {},
            )
        except Exception:
            return None