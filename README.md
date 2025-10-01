![](docs/images/temp_logo.png)
# Mongrations
## Easy MongoDB migrations in Python

## Features

- **Powerful Migration Pipeline**: Define complex data migrations with phases, dependencies, and operations
- **Comprehensive Logging**: Built-in detailed logging for monitoring and debugging
- **Async Execution**: Efficient async operations using Motor
- **Dependency Management**: Automatic dependency resolution between migration phases
- **State Tracking**: Track migration status and progress in MongoDB
- **Flexible Operations**: Aggregations, transformations, imports, exports, and more

## Logging

Mongrations includes comprehensive logging support using Python's standard `logging` library. Each component has its own logger, making it easy to control logging granularity.

### Quick Start

```python
import logging

# Enable basic logging
logging.basicConfig(level=logging.INFO)

# Or configure specific components
logging.getLogger('mongrations.program').setLevel(logging.INFO)
logging.getLogger('mongrations.phase').setLevel(logging.DEBUG)
```

### Available Loggers

- `mongrations.program` - Main execution flow
- `mongrations.plan` - State management
- `mongrations.engine.*` - Execution engine
- `mongrations.phase` - Phase execution
- `mongrations.operations.*` - Operations
- `mongrations.io.*` - IO operations

For detailed logging configuration examples and best practices, see:
- [Logging Documentation](docs/logging.md)
- [Logging Configuration Examples](examples/logging_configuration.py)