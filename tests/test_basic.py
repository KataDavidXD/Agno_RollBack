"""Basic tests for the Agno Rollback system."""

import asyncio
import pytest
from uuid import uuid4

from agno_rollback import (
    WorkflowManager,
    TaskStatus,
    ResumptionPoint,
    SQLiteStorage,
)


@pytest.fixture
async def manager():
    """Create a workflow manager for testing."""
    storage = SQLiteStorage(":memory:")  # In-memory database for tests
    manager = WorkflowManager(storage=storage)
    await manager.initialize()
    yield manager
    await manager.close()


@pytest.mark.asyncio
async def test_workflow_creation(manager):
    """Test creating a new workflow."""
    task_id = await manager.start_workflow(
        user_id="test_user",
        query="test query"
    )
    
    assert task_id is not None
    assert isinstance(task_id, str)
    
    # Check task was created
    task = await manager.get_task_status(task_id)
    assert task is not None
    assert task.user_id == "test_user"
    assert task.query == "test query"
    assert task.status == TaskStatus.RUNNING


@pytest.mark.asyncio
async def test_task_analysis(manager):
    """Test analyzing a task."""
    # Create a task
    task_id = await manager.start_workflow(
        user_id="test_user",
        query="test query"
    )
    
    # Analyze it
    analysis = await manager.analyze_task(task_id)
    
    assert analysis is not None
    assert "original_info" in analysis
    assert "resumption_analysis" in analysis
    assert analysis["original_info"]["query"] == "test query"


@pytest.mark.asyncio
async def test_list_resumable_tasks(manager):
    """Test listing resumable tasks."""
    # Create some tasks
    for i in range(3):
        await manager.start_workflow(
            user_id="test_user",
            query=f"test query {i}"
        )
    
    # List resumable tasks
    resumable = await manager.list_resumable_tasks(user_id="test_user")
    
    assert resumable is not None
    assert "total_resumable_tasks" in resumable
    assert "tasks" in resumable
    assert isinstance(resumable["tasks"], list)


@pytest.mark.asyncio
async def test_storage_operations(manager):
    """Test storage operations."""
    # Create a task
    task_id = uuid4()
    from agno_rollback.core import WorkflowTask
    
    task = WorkflowTask(
        task_id=task_id,
        user_id="test_user",
        session_id="test_session",
        query="test query"
    )
    
    # Save task
    await manager.storage.create_task(task)
    
    # Retrieve task
    retrieved = await manager.storage.get_task(task_id)
    assert retrieved is not None
    assert retrieved.user_id == "test_user"
    assert retrieved.query == "test query"
    
    # Update task
    retrieved.progress = 50.0
    await manager.storage.update_task(retrieved)
    
    # Check update
    updated = await manager.storage.get_task(task_id)
    assert updated.progress == 50.0


@pytest.mark.asyncio
async def test_resumption_points():
    """Test resumption point logic."""
    from agno_rollback.core import WorkflowState
    
    state = WorkflowState(task_id=uuid4(), last_checkpoint="test")
    
    # Test with nothing completed
    points = state.analyze_resumption_points()
    assert ResumptionPoint.RESTART_FROM_BEGINNING in points
    assert ResumptionPoint.RESUME_BEFORE_PARALLEL_RETRIEVAL in points
    
    # Test with web retrieval completed
    state.web_retrieval_completed = True
    points = state.analyze_resumption_points()
    assert ResumptionPoint.RESUME_AFTER_WEB_RETRIEVAL in points
    
    # Test with both retrievals completed
    state.news_retrieval_completed = True
    points = state.analyze_resumption_points()
    assert ResumptionPoint.RESUME_AFTER_PARALLEL_RETRIEVAL in points
    assert ResumptionPoint.RESUME_BEFORE_SUMMARIZATION in points


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])