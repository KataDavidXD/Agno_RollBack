# Agno Rollback - Workflow Resumption System

A modern, production-ready workflow resumption system built with Agno 1.7.6. This system allows workflows to be paused, stored, and resumed from any point, making it ideal for handling failures, optimizing resources, and debugging complex multi-agent workflows.

## Features

- **8 Resumption Points**: From complete restart to intelligent auto-resumption
- **Parallel Processing**: Concurrent web and news retrieval with progress tracking
- **Multi-Backend Storage**: SQLite (dev), PostgreSQL (prod), ClickHouse (analytics), S3 (blobs)
- **Real-time Analytics**: Performance monitoring and event analytics with ClickHouse
- **Blob Storage**: Large file handling with S3-compatible storage
- **Event-Driven Monitoring**: Real-time progress updates and comprehensive logging
- **Enhanced Validation**: Automatic checkpoint success/failure detection
- **Clean Architecture**: Modular design with clear separation of concerns
- **Type Safety**: Full type hints with Pydantic models

## Installation

### Development Setup

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

### Production Installation

Choose the appropriate installation based on your needs:

```bash
# Basic installation (SQLite only)
pip install agno-rollback

# Production with PostgreSQL
pip install agno-rollback[postgres]

# With analytics (ClickHouse)
pip install agno-rollback[analytics]

# With blob storage (S3)
pip install agno-rollback[blobs]

# Enterprise (all backends)
pip install agno-rollback[enterprise]

# All optional dependencies
pip install agno-rollback[all]
```

## Configuration

### Basic Configuration

The system uses environment variables for API keys. Create a `.env` file in the project root:

```bash
# Copy the example file
cp .env.example .env

# Edit .env and add your OpenAI API key
OPENAI_API_KEY=sk-proj-your-actual-key-here
# Optional: Custom OpenAI base URL
BASE_URL=https://api.openai.com/v1
```

### Storage Backend Configuration

Choose your storage backend by setting environment variables:

```bash
# Development (SQLite - default)
STORAGE_BACKEND=sqlite
SQLITE_DB_PATH=data/workflow.db

# Production (PostgreSQL)
STORAGE_BACKEND=postgres
POSTGRES_DSN=postgresql://user:password@localhost:5432/agno_rollback

# Enterprise (Composite with all backends)
STORAGE_BACKEND=composite
POSTGRES_DSN=postgresql://user:password@localhost:5432/agno_rollback

# ClickHouse for analytics
ENABLE_CLICKHOUSE=true
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_DATABASE=agno_rollback
CLICKHOUSE_USERNAME=default
CLICKHOUSE_PASSWORD=

# S3 for blob storage
ENABLE_S3=true
S3_BUCKET_NAME=agno-rollback-blobs
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_DEFAULT_REGION=us-east-1
# S3_ENDPOINT_URL=http://localhost:9000  # For MinIO/LocalStack
```

The `.env` file is automatically loaded when you import the package.

## Quick Start

```python
from agno_rollback import WorkflowManager

# Create a workflow manager (auto-detects storage from environment)
manager = WorkflowManager()

# Or use factory methods for specific configurations
# manager = WorkflowManager.create_development()  # SQLite
# manager = WorkflowManager.create_production()   # PostgreSQL
# manager = WorkflowManager.create_enterprise()   # All backends

await manager.initialize()

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
    result = await manager.resume_workflow(task_id)
    print(f"Resumed as task: {result['new_task_id']}")

# Get analytics (if ClickHouse enabled)
analytics = await manager.get_analytics()
metrics = await manager.get_performance_metrics()

# Health check
health = await manager.health_check()
print(f"System health: {health}")

await manager.close()
```

## Storage Backends

The system supports multiple storage backends optimized for different use cases:

### SQLite (Development)
- **Use case**: Local development, testing, small deployments
- **Features**: Zero configuration, file-based storage
- **Limitations**: Single writer, limited concurrency

### PostgreSQL (Production OLTP)
- **Use case**: Production deployments, transactional workloads
- **Features**: ACID transactions, concurrent access, JSON support
- **Best for**: Task management, workflow states, user sessions

### ClickHouse (Analytics OLAP)
- **Use case**: Real-time analytics, monitoring, performance metrics
- **Features**: Columnar storage, fast aggregations, automatic TTL
- **Best for**: Event tracking, usage analytics, system monitoring

### S3 (Blob Storage)
- **Use case**: Large file storage, attachments, binary data
- **Features**: Unlimited storage, lifecycle policies, presigned URLs
- **Best for**: Agent outputs, tool results, checkpoint data

### Composite (Enterprise)
- **Use case**: Large-scale production with full observability
- **Features**: Combines all backends with intelligent routing
- **Architecture**: PostgreSQL (primary) + ClickHouse (analytics) + S3 (blobs)

## Architecture

The system is built with a modular architecture:

- **Core**: Base classes and data models
- **Storage**: Multi-backend storage layer with intelligent routing
- **Resumption**: Smart resumption strategies and state analysis
- **Monitoring**: Event-driven progress tracking with success validation
- **Workflows**: Concrete workflow implementations

## Recent Improvements

### Enhanced Monitoring (v2.3.0)
- **AGENT_SUCCESS Events**: Automatic checkpoint validation with component status breakdown
- **Success Detection**: Validates `result.success` fields in checkpoint data
- **Error Reporting**: Enhanced completion analysis with detailed error information
- **Progress Tracking**: Real-time monitoring with failure detection

### Agent Configuration
- **Centralized Models**: Unified `_default_openai_chat()` for consistent configuration
- **Environment Integration**: Automatic BASE_URL and OPENAI_API_KEY detection
- **Standardized Defaults**: Consistent model configuration across all agents

### Display Improvements
- **Error Visualization**: Separated error messages from boolean status indicators
- **Inline Errors**: Show error details directly in completion analysis
- **Comprehensive Summaries**: Detailed error summary sections

### Migration Tools
- **Storage Migration**: Helper scripts for SQLite → PostgreSQL migration
- **Data Validation**: Integrity checking and validation tools
- **Configuration Testing**: Health checks for storage backends

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

# Migration helper
python examples/migration_helper.py export data/workflow.db
python examples/migration_helper.py import export.json "postgresql://..."

# Example usage
python examples/basic_usage.py              # Basic workflow example
python examples/enhanced_usage.py           # Multi-backend examples
python examples/storage_config_example.py   # Storage configuration guide
```

## Examples

The project includes comprehensive examples:

- **`basic_usage.py`**: Getting started with workflows and resumption
- **`enhanced_usage.py`**: Multi-backend storage and analytics
- **`storage_config_example.py`**: Storage backend configuration guide
- **`migration_helper.py`**: Database migration utilities

## License

Apache-2.0 license - see LICENSE file for details.
