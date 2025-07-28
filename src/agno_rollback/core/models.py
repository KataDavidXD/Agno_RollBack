"""Core data models for the workflow resumption system.

These models define the structure of our data and provide validation.
Using Pydantic ensures type safety and automatic validation.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator


class TaskStatus(str, Enum):
    """Workflow task status enumeration."""
    
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ResumptionPoint(str, Enum):
    """Available workflow resumption points."""
    
    RESTART_FROM_BEGINNING = "restart_from_beginning"
    RESUME_BEFORE_PARALLEL_RETRIEVAL = "resume_before_parallel_retrieval"
    RESUME_AFTER_WEB_RETRIEVAL = "resume_after_web_retrieval"
    RESUME_AFTER_NEWS_RETRIEVAL = "resume_after_news_retrieval"
    RESUME_AFTER_PARALLEL_RETRIEVAL = "resume_after_parallel_retrieval"
    RESUME_BEFORE_SUMMARIZATION = "resume_before_summarization"
    RESUME_FROM_PARTIAL_SUMMARIZATION = "resume_from_partial_summarization"
    CONTINUE_FROM_WHERE_LEFT_OFF = "continue_from_where_left_off"


class WorkflowEvent(BaseModel):
    """Represents an event in the workflow execution."""
    
    event_id: UUID = Field(default_factory=uuid4)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    event_type: str
    event_data: Dict[str, Any] = Field(default_factory=dict)
    metadata: Optional[Dict[str, Any]] = None


class AgentOutput(BaseModel):
    """Represents output from an agent."""
    
    agent_name: str
    output: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: Optional[Dict[str, Any]] = None


class ToolResult(BaseModel):
    """Represents a tool call result."""
    
    tool_name: str
    tool_arguments: Dict[str, Any]
    tool_result: Any
    success: bool
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    execution_time_ms: Optional[int] = None
    error_message: Optional[str] = None


class StepResult(BaseModel):
    """Represents the result of a workflow step."""
    
    step_name: str
    result: Any
    started_at: datetime
    completed_at: datetime
    metadata: Optional[Dict[str, Any]] = None
    
    @property
    def duration_ms(self) -> int:
        """Calculate step duration in milliseconds."""
        delta = self.completed_at - self.started_at
        return int(delta.total_seconds() * 1000)


class WorkflowTask(BaseModel):
    """Main workflow task information."""
    
    task_id: UUID = Field(default_factory=uuid4)
    user_id: str
    session_id: str
    query: str
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    progress: float = Field(default=0.0, ge=0.0, le=100.0)
    result: Optional[str] = None
    error: Optional[str] = None
    current_step: Optional[str] = None
    
    # Execution history
    workflow_events: List[WorkflowEvent] = Field(default_factory=list)
    agent_outputs: List[AgentOutput] = Field(default_factory=list)
    tool_results: List[ToolResult] = Field(default_factory=list)
    step_results: List[StepResult] = Field(default_factory=list)
    
    # Retrieval results
    web_retrieval_results: Optional[Dict[str, Any]] = None
    news_retrieval_results: Optional[Dict[str, Any]] = None
    
    @field_validator('progress')
    def validate_progress(cls, v: float) -> float:
        """Ensure progress is between 0 and 100."""
        return max(0.0, min(100.0, v))
    
    def update_progress(self, step_name: str, step_progress: float) -> None:
        """Update task progress based on step completion."""
        self.current_step = step_name
        self.progress = step_progress
    
    def add_event(self, event_type: str, event_data: Dict[str, Any]) -> None:
        """Add a workflow event."""
        event = WorkflowEvent(
            event_type=event_type,
            event_data=event_data
        )
        self.workflow_events.append(event)
    
    def add_agent_output(self, agent_name: str, output: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Add agent output."""
        agent_output = AgentOutput(
            agent_name=agent_name,
            output=output,
            metadata=metadata
        )
        self.agent_outputs.append(agent_output)
    
    def add_tool_result(self, tool_name: str, tool_args: Dict[str, Any], 
                       result: Any, success: bool, execution_time_ms: Optional[int] = None) -> None:
        """Add tool call result."""
        tool_result = ToolResult(
            tool_name=tool_name,
            tool_arguments=tool_args,
            tool_result=result,
            success=success,
            execution_time_ms=execution_time_ms
        )
        self.tool_results.append(tool_result)
    
    def add_step_result(self, step_name: str, result: Any, 
                       started_at: datetime, completed_at: datetime, 
                       metadata: Optional[Dict[str, Any]] = None) -> None:
        """Add step result."""
        step_result = StepResult(
            step_name=step_name,
            result=result,
            started_at=started_at,
            completed_at=completed_at,
            metadata=metadata
        )
        self.step_results.append(step_result)
    
    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v),
        }


class WorkflowState(BaseModel):
    """Current workflow execution state for resumption."""
    
    task_id: UUID
    last_checkpoint: str
    checkpoint_data: Dict[str, Any] = Field(default_factory=dict)
    available_resumption_points: List[ResumptionPoint] = Field(default_factory=list)
    recommended_resumption_point: Optional[ResumptionPoint] = None
    
    # Completion analysis
    web_retrieval_completed: bool = False
    news_retrieval_completed: bool = False
    summarization_started: bool = False
    summarization_completed: bool = False
    
    def analyze_resumption_points(self) -> List[ResumptionPoint]:
        """Analyze available resumption points based on completion state."""
        points = [ResumptionPoint.RESTART_FROM_BEGINNING]
        
        if not self.web_retrieval_completed and not self.news_retrieval_completed:
            points.append(ResumptionPoint.RESUME_BEFORE_PARALLEL_RETRIEVAL)
        
        if self.web_retrieval_completed and not self.news_retrieval_completed:
            points.append(ResumptionPoint.RESUME_AFTER_WEB_RETRIEVAL)
        
        if not self.web_retrieval_completed and self.news_retrieval_completed:
            points.append(ResumptionPoint.RESUME_AFTER_NEWS_RETRIEVAL)
        
        if self.web_retrieval_completed and self.news_retrieval_completed:
            points.append(ResumptionPoint.RESUME_AFTER_PARALLEL_RETRIEVAL)
            
            if not self.summarization_started:
                points.append(ResumptionPoint.RESUME_BEFORE_SUMMARIZATION)
            elif self.summarization_started and not self.summarization_completed:
                points.append(ResumptionPoint.RESUME_FROM_PARTIAL_SUMMARIZATION)
        
        points.append(ResumptionPoint.CONTINUE_FROM_WHERE_LEFT_OFF)
        
        self.available_resumption_points = points
        return points
    
    def get_recommended_resumption_point(self) -> ResumptionPoint:
        """Get recommended resumption point based on state."""
        if not self.available_resumption_points:
            self.analyze_resumption_points()
        
        # Smart recommendation logic
        if self.summarization_started and not self.summarization_completed:
            self.recommended_resumption_point = ResumptionPoint.RESUME_FROM_PARTIAL_SUMMARIZATION
        elif self.web_retrieval_completed and self.news_retrieval_completed:
            self.recommended_resumption_point = ResumptionPoint.RESUME_BEFORE_SUMMARIZATION
        elif self.web_retrieval_completed or self.news_retrieval_completed:
            self.recommended_resumption_point = ResumptionPoint.RESUME_AFTER_PARALLEL_RETRIEVAL
        else:
            self.recommended_resumption_point = ResumptionPoint.RESUME_BEFORE_PARALLEL_RETRIEVAL
        
        return self.recommended_resumption_point


class SessionContext(BaseModel):
    """User session context for maintaining state across workflows."""
    
    user_id: str
    session_id: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    retrieval_history: List[Dict[str, Any]] = Field(default_factory=list)
    search_context: Dict[str, Any] = Field(default_factory=dict)
    preferences: Dict[str, Any] = Field(default_factory=dict)
    
    def add_retrieval(self, query: str, results: Dict[str, Any]) -> None:
        """Add retrieval to history."""
        self.retrieval_history.append({
            "query": query,
            "results": results,
            "timestamp": datetime.utcnow().isoformat()
        })
    
    def update_search_context(self, key: str, value: Any) -> None:
        """Update search context."""
        self.search_context[key] = value