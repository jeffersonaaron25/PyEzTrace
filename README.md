# PyEzTrace

A powerful, lightweight Python tracing and logging library with hierarchical logging, context management, and performance metrics.

**Note:** This was a fun, 3 hour experiment for one of my local projects. I'm happy to continue building this out if the community finds it useful and thinks it's a good idea!

## Features

- 🌳 **Hierarchical Logging**: Visualize nested operations with tree-style output
- 🎨 **Multiple Formats**: Support for color, plain text, JSON, CSV, and logfmt outputs
- 📊 **Performance Metrics**: Built-in timing and tracing capabilities
- 🔄 **Context Management**: Thread-safe context propagation
- 🔄 **Log Rotation**: Automatic log file management
- 🎯 **Decorator-based Tracing**: Easy function and method tracing
- 💪 **Thread-Safe**: Fully thread-safe implementation
- 🚀 **High Performance**: Buffered logging and optimized output

## Installation

```bash
pip install pyeztrace
```

## Quick Start

```python
from pyeztrace.tracer import trace
from pyeztrace.custom_logging import Logging

# Initialize the logging system
log = Logging(log_format="color")  # or "json", "plain", "csv", "logfmt"

# Use the tracer decorator
@trace()
def process_order(order_id):
    with log.with_context(order_id=order_id):
        log.log_info("Processing order")
        validate_order(order_id)
        process_payment(order_id)
        log.log_info("Order processed successfully")

@trace()
def validate_order(order_id):
    log.log_info("Validating order")
    # Your validation logic here
```

Output example:
```
2025-05-13T10:00:00 - INFO - [MyApp] ├── process_order called...
2025-05-13T10:00:00 - INFO - [MyApp] ├── Processing order Data: {order_id: "123"}
2025-05-13T10:00:00 - INFO - [MyApp] ├────── validate_order called... 
2025-05-13T10:00:00 - INFO - [MyApp] ├────── Validating order
2025-05-13T10:00:00 - INFO - [MyApp] ├────── validate_order Ok. (took 0.50010 seconds)
2025-05-13T10:00:01 - INFO - [MyApp] ├── Order processed successfully
2025-05-13T10:00:01 - INFO - [MyApp] ├── process_order Ok. (took 1.23456 seconds)
```

## Usage

### 1. Setup and Configuration

```python
# Auto-initialization
from pyeztrace.tracer import trace  # Automatically initializes with script name

# Optional: customize settings
from pyeztrace.setup import Setup
Setup.set_project("MyApp")          # Change project name
Setup.set_show_metrics(True)        # Enable performance metrics
```

### 2. Tracing with Fine-grained Control

```python
@trace(
    message="Custom trace message",  # Optional custom message
    stack=True,  # Include stack trace on errors
    modules_or_classes=[my_module],  # Trace specific modules
    include=["specific_function_*"],  # Include only specific functions
    exclude=["ignored_function_*"]  # Exclude specific functions
)
"""
Decorator for parent function. Enables tracing for all child functions in the given modules or classes.
If modules_or_classes is None, it will automatically patch the module where the parent function is defined.
Accepts a single module/class or a list of modules/classes for cross-module tracing.
Handles both sync and async parent functions.
Supports selective tracing via include/exclude patterns (function names).
"""
def function():
    # Your code here
    pass
```

### 3. Context Management

Thread-safe context propagation for structured logging:

```python
with log.with_context(user_id="123", action="login"):
    log.log_info("User logged in")  # Will include context automatically
    
    with log.with_context(session="abc"):
        # Nested context, inherits parent context
        log.log_info("Session started")  # Includes both user_id and session
```

### 4. Multiple Output Formats

```python
# Color-coded console output with hierarchical visualization
log = Logging(log_format="color")

# JSON format for machine processing
log = Logging(log_format="json")
# Output: {"timestamp": "2025-05-13T10:00:00", "level": "INFO", "message": "Log message", "data": {"context": "value"}}

# Plain text for simple logging
log = Logging(log_format="plain")

# CSV format for spreadsheet analysis
log = Logging(log_format="csv")

# logfmt for system logging
log = Logging(log_format="logfmt")
# Output: time=2025-05-13T10:00:00 level=INFO message="Log message" data.context=value
```

### 5. Async Support

```python
@trace()
async def async_function():
    await some_async_task()
    log.log_info("Async operation completed")
```

### 6. Performance Metrics

Enable automatic performance tracking:

```python
# Either during initialization
Setup.initialize("MyApp", show_metrics=True)

# Or anytime using
Setup.set_show_metrics(True)

@trace()
def monitored_function():
    # Function execution time will be automatically logged
    pass

# At program exit, prints performance summary:
# === Tracing Performance Metrics Summary ===
# Function                                    Calls    Total(s)      Avg(s)
# --------------------------------------------------------------------
# my_module.monitored_function                   10      1.23456      0.12346
```

### 7. Log Rotation

Configure automatic log rotation based on file size:

```python
from pyeztrace.config import config

config.max_size = 10 * 1024 * 1024  # 10MB
config.backup_count = 5  # Keep 5 backup files
config.log_dir = "logs"  # Custom log directory
config.log_file = "app.log"  # Custom log filename
```

### 8. Error Handling and Debug Support

```python
# Different log levels
log.log_debug("Debug information")
log.log_info("Normal operation")
log.log_warning("Warning message")
log.log_error("Error occurred")

try:
    # Your code
except Exception as e:
    # Automatically log exception with stack trace
    log.raise_exception_to_log(e, "Custom error message", stack=True)
```

### 9. Thread-Safe High-Volume Logging

The logging system is designed for high-volume scenarios with thread-safe implementation:

```python
from concurrent.futures import ThreadPoolExecutor

@trace()
def concurrent_operation(worker_id):
    with log.with_context(worker_id=worker_id):
        log.log_info("Worker started")
        # ... work ...
        log.log_info("Worker finished")

with ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(concurrent_operation, range(5))
```

## Configuration

All configuration options can be set via environment variables or code:

```python
# Via environment variables
export EZTRACE_LOG_FORMAT="json"
export EZTRACE_LOG_LEVEL="DEBUG"
export EZTRACE_LOG_FILE="custom.log"
export EZTRACE_MAX_SIZE="10485760"  # 10MB
export EZTRACE_BACKUP_COUNT="5"

# Via code
from pyeztrace.config import config
config.format = "json"
config.log_level = "DEBUG"
config.log_file = "custom.log"
config.max_size = 10 * 1024 * 1024  # 10MB
config.backup_count = 5
```

## Contributing

Contributions are welcome! Please read our [Contributing Guidelines](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
