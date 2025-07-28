# Quick Start Guide

## Installation

```bash
# Install Poetry
curl -sSL https://install.python-poetry.org | python3 -

# Clone the repository
git clone https://github.com/yourusername/agno-rollback.git
cd agno-rollback

# Install dependencies
poetry install

# Activate virtual environment
poetry shell
```

## Basic Usage

```python
from agno_rollback import WorkflowManager

# Create manager
manager = WorkflowManager()
await manager.initialize()

# Start a workflow
task_id = await manager.start_workflow(
    user_id="user123",
    query="Research AI trends"
)

# Check status
task = await manager.get_task_status(task_id)
print(f"Progress: {task.progress}%")

# Resume if failed
if task.status == "failed":
    result = await manager.resume_workflow(task_id)
```

## Key Concepts

### Workflow States
- **PENDING**: Workflow created but not started
- **RUNNING**: Currently executing
- **COMPLETED**: Successfully finished
- **FAILED**: Encountered an error
- **CANCELLED**: User cancelled

### Resumption Points
1. **RESTART_FROM_BEGINNING**: Complete restart
2. **RESUME_BEFORE_PARALLEL_RETRIEVAL**: Start retrieval phase
3. **RESUME_AFTER_WEB_RETRIEVAL**: Web done, continue with news
4. **RESUME_AFTER_NEWS_RETRIEVAL**: News done, continue with web
5. **RESUME_AFTER_PARALLEL_RETRIEVAL**: Both done, start summary
6. **RESUME_BEFORE_SUMMARIZATION**: Redo summary only
7. **RESUME_FROM_PARTIAL_SUMMARIZATION**: Continue partial summary
8. **CONTINUE_FROM_WHERE_LEFT_OFF**: Smart auto-resume

## Architecture Overview

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────┐
│   Workflow      │────▶│   Storage    │────▶│  SQLite DB  │
│   Manager       │     │  Abstraction │     │             │
└─────────────────┘     └──────────────┘     └─────────────┘
         │                      ▲
         │                      │
         ▼                      │
┌─────────────────┐     ┌──────────────┐
│   Resumption    │     │  Monitoring  │
│   Manager       │     │   Service    │
└─────────────────┘     └──────────────┘
         │
         ▼
┌─────────────────┐
│   Workflow      │
│  (Web + News +  │
│   Summarize)    │
└─────────────────┘
```

## Advanced Features

### Custom Storage Backend

```python
from agno_rollback import StorageBackend

class MyCustomStorage(StorageBackend):
    async def initialize(self):
        # Your initialization
        pass
    
    # Implement other methods...

manager = WorkflowManager(storage=MyCustomStorage())
```

### Event Monitoring

```python
from agno_rollback import WorkflowMonitor, EventType

monitor = WorkflowMonitor()

# Register event handler
def on_progress(event):
    print(f"Progress: {event.data['progress']}%")

monitor.register_handler(EventType.PROGRESS_UPDATE, on_progress)
```

### Analyze Failed Workflows

```python
# Get detailed analysis
analysis = await manager.analyze_task(task_id)

# Check what completed
completion = analysis['resumption_analysis']['completion_analysis']
print(f"Web search: {'✅' if completion['web_retrieval_completed'] else '❌'}")
print(f"News search: {'✅' if completion['news_retrieval_completed'] else '❌'}")

# Get recommendation
recommended = analysis['resumption_analysis']['recommended_point']
print(f"Recommended: {recommended}")
```

## Best Practices

1. **Always Initialize**: Call `manager.initialize()` before use
2. **Handle Errors**: Wrap operations in try-except blocks
3. **Monitor Progress**: Use the monitoring service for real-time updates
4. **Clean Up**: Call `manager.close()` when done
5. **Check Resumability**: Analyze before resuming to understand state

## Troubleshooting

### Workflow Stuck in RUNNING
- Check if the process crashed
- Use `analyze_task()` to see last checkpoint
- Resume from last known good state

### Storage Errors
- Ensure database file has write permissions
- Check disk space
- Verify SQLite is installed

### Agent Failures
- Check API keys are configured
- Verify network connectivity
- Review agent logs for details