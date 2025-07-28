# Agno Rollback - Workflow Resumption System

A modern, production-ready workflow resumption system built with Agno 1.7.6. This system allows workflows to be paused, stored, and resumed from any point, making it ideal for handling failures, optimizing resources, and debugging complex multi-agent workflows.

## Features

- **8 Resumption Points**: From complete restart to intelligent auto-resumption
- **Parallel Processing**: Concurrent web and news retrieval with progress tracking
- **Flexible Storage**: Support for multiple storage backends (SQLite, PostgreSQL)
- **Event-Driven Monitoring**: Real-time progress updates and comprehensive logging
- **Clean Architecture**: Modular design with clear separation of concerns
- **Type Safety**: Full type hints with Pydantic models

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/agno-rollback.git
cd agno-rollback

# Install Poetry (if not already installed)
curl -sSL https://install.python-poetry.org | python3 -

# Install dependencies
poetry install

# Activate the virtual environment
poetry shell
```

## Configuration

The system uses environment variables for API keys. Create a `.env` file in the project root:

```bash
# Copy the example file
cp .env.example .env

# Edit .env and add your OpenAI API key
OPENAI_API_KEY=sk-proj-your-actual-key-here
or
echo "OPENAI_API_KEY=sk-proj-YOUR-ACTUAL-API-KEY" > .env
#run
poetry run python filepath 
```

The `.env` file is automatically loaded when you import the package.

## Quick Start

```python
from agno_rollback import ResumableWorkflow, WorkflowManager

# Create a workflow manager
manager = WorkflowManager()

# Start a new workflow
task_id = await manager.start_workflow(
    user_id="user123",
    query="Research latest AI developments"
)

# Monitor progress
status = await manager.get_task_status(task_id)
print(f"Progress: {status.progress}%")

# If workflow fails, resume from last checkpoint
if status.status == "failed":
    new_task_id = await manager.resume_workflow(task_id)
```

## Architecture

The system is built with a modular architecture:

- **Core**: Base classes and data models
- **Storage**: Abstracted storage layer with multiple backends
- **Resumption**: Smart resumption strategies and state analysis
- **Monitoring**: Event-driven progress tracking
- **Workflows**: Concrete workflow implementations

## Development

```bash
# Run tests
poetry run pytest

# Format code
poetry run black src tests
poetry run isort src tests

# Type checking
poetry run mypy src

# Linting
poetry run ruff src tests
```

## License

Apache-2.0 license - see LICENSE file for details.
