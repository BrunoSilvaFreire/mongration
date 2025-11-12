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

## Testing

Mongrations uses BDD (Behavior-Driven Development) testing with Behave and includes comprehensive test coverage across all features.

### Running Tests

```bash
# Run all tests
task test

# Run specific feature
task test -- tests/features/phase_operations.feature

# Run tests with debug mode
task test:debug

# Re-run only failed tests (fast iteration)
task test:rerun

# Show which tests failed
task test:failed

# Clean test artifacts
task clean
```

### Test Structure

Tests are organized by feature area for better maintainability:

- **mongrations.feature** - Core migration functionality
- **phase_operations.feature** - Phase-specific operations
- **graph_operations.feature** - Dependency graph operations
- **state_management.feature** - Migration state tracking
- **io_operations.feature** - Input/output operations
- **error_handling.feature** - Error scenarios and edge cases
- **advanced_operations.feature** - Advanced features (UUID conversion, indexes, etc.)

### Coverage

Current test coverage: **~70%** (target: 75%+)

View coverage reports:
```bash
# HTML report
open tests/reports/coverage/index.html

# JSON report
cat tests/reports/coverage/coverage.json
```

### HTML Reports

Beautiful HTML test reports are generated automatically using `behave-html-pretty-formatter`:

```bash
# Run tests (generates HTML report)
task test

# Open the HTML report
task html:open
# Or manually: open tests/reports/behave-report.html
```

The HTML reports provide:
- ✅ Collapsible scenarios and features
- 🎨 Color-coded test results
- 📊 Summary statistics and duration tracking
- 🔍 Detailed step execution information

See [HTML_REPORTS.md](HTML_REPORTS.md) for complete documentation.

See [TEST_COVERAGE_IMPROVEMENTS.md](TEST_COVERAGE_IMPROVEMENTS.md) for detailed coverage information.