# PyEzTrace

A dependency-free, lightweight Python tracing and logging library with hierarchical logging, context management, and performance metrics.

## Features

- 🌳 **Hierarchical Logging**: Visualize nested operations with tree-style output
- 🎨 **Multiple Formats**: Support for color, plain text, JSON, CSV, and logfmt outputs
- 📊 **Performance Metrics**: Built-in timing and tracing capabilities
- 🔄 **Context Management**: Thread-safe context propagation
- 🔄 **Log Rotation**: Automatic log file management
- 🎯 **Decorator-based Tracing**: Easy function and method tracing
- 💪 **Thread-Safe**: Fully thread-safe implementation
- 🚀 **High Performance**: Buffered logging and optimized output
 - 🌐 **OpenTelemetry Bridge (optional)**: Emit spans to OTLP/console, or export batches to S3/Azure Blob

### New
- 🧭 **Interactive Viewer**: Explore hierarchical traces with input/output previews, time, CPU, and memory

## Installation

```bash
pip install pyeztrace
```

Optional extras (keep default dependency-free):

```bash
# OpenTelemetry SDK and OTLP exporter
pip install "pyeztrace[otel]"

# S3 exporter
pip install "pyeztrace[s3]"

# Azure Blob exporter
pip install "pyeztrace[azure]"

# Everything
pip install "pyeztrace[all]"
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
2025-05-13T10:00:00 - INFO - [MyApp] │    ├─── validate_order called... 
2025-05-13T10:00:00 - INFO - [MyApp] │    ├─── Validating order
2025-05-13T10:00:00 - INFO - [MyApp] │    ├─── validate_order Ok. (took 0.50010 seconds)
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
    exclude=["ignored_function_*"],  # Exclude specific functions
    recursive_depth=2,  # How many levels of imports to trace (0 = only direct module)
    module_pattern="myapp.*"  # Pattern to match module names for recursive tracing
)
def function():
    # Your code here
    pass
```

#### Recursive Tracing

PyEzTrace supports recursive tracing of imported modules:

```python
# Basic function tracing (only traces the function itself)
@trace()
def basic_function():
    pass

# Trace function plus all functions in directly imported modules (depth=1)
@trace(recursive_depth=1, module_pattern="myapp.*")
def app_function():
    # This will trace any imported modules that match "myapp.*"
    pass

# Deep recursive tracing (caution: can be performance-intensive)
@trace(recursive_depth=3, module_pattern="myapp.services.*")
def service_function():
    # This will trace the function, direct imports, imports of imports, 
    # and imports of imports of imports that match the pattern
    pass
```

Using `module_pattern` is strongly recommended when enabling recursive tracing to prevent tracing system libraries or third-party packages. Be sure to limit recursive tracing to avoid unexpected issues
and unnecessary traces.

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

### 5. Interactive Viewer (Live Trace UI)

The built-in viewer renders hierarchical traces with argument/result previews and timing/CPU/memory metrics.

Requirements:
- Set the log format to JSON so the viewer can parse entries.

```python
from pyeztrace.custom_logging import Logging
from pyeztrace.setup import Setup

Setup.initialize("MyApp", show_metrics=True)
log = Logging(log_format="json")
```

Run your app to generate logs, then start the viewer pointing to your log file:

```bash
python -m pyeztrace.cli serve logs/app.log --host 127.0.0.1 --port 8765
# open http://127.0.0.1:8765
```

What you get:
- Hierarchical tree (parent/child calls)
- Input previews (args/kwargs), output preview (result)
- Time (duration), CPU time, memory delta and peak
- Filter by function or error, auto-refresh every 2.5s

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
# must be setup before importing trace
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

### 10. OpenTelemetry Spans (Optional)

PyEzTrace can emit OpenTelemetry spans alongside its logs using a lazy bridge.
By default it is disabled; enable it with environment variables at runtime.

Enable OTEL with OTLP HTTP (default) to a collector:

```bash
export EZTRACE_OTEL_ENABLED=true
export EZTRACE_OTEL_EXPORTER=otlp            # or omit to use 'otlp' by default
export EZTRACE_OTLP_ENDPOINT="http://localhost:4318/v1/traces"
# optional: comma-separated headers like "api-key=xyz,x-tenant=abc"
export EZTRACE_OTLP_HEADERS=""

# Optional: override service name (defaults to Setup project)
export EZTRACE_SERVICE_NAME="my-service"
```

Use console exporter for local development:

```bash
export EZTRACE_OTEL_ENABLED=true
export EZTRACE_OTEL_EXPORTER=console
```

Export span batches to S3 as JSONL (gzipped by default) without a collector:

```bash
pip install "pyeztrace[s3]"
export EZTRACE_OTEL_ENABLED=true
export EZTRACE_OTEL_EXPORTER=s3
export EZTRACE_S3_BUCKET="my-trace-bucket"
export EZTRACE_S3_PREFIX="traces/"               # optional, default traces/
export EZTRACE_S3_REGION="us-east-1"             # optional
export EZTRACE_COMPRESS=true                      # optional, default true
```

Export span batches to Azure Blob Storage:

```bash
pip install "pyeztrace[azure]"
export EZTRACE_OTEL_ENABLED=true
export EZTRACE_OTEL_EXPORTER=azure
export EZTRACE_AZURE_CONTAINER="trace-container"
export EZTRACE_AZURE_PREFIX="traces/"            # optional, default traces/
# One of the following must be provided:
export EZTRACE_AZURE_CONNECTION_STRING="<connection-string>"
# or
export EZTRACE_AZURE_ACCOUNT_URL="https://<account>.blob.core.windows.net"
```

Notes:
- The bridge is lazy-loaded; if OTEL packages are missing, the library remains functional without spans.
- Spans are created for both parent and child wrappers using function `__qualname__` as span names.
- Exceptions are recorded on the active span when OTEL is enabled.

## Advanced Usage

### 1. Applying to Classes

You can apply the `@trace` decorator directly to a class, which will automatically trace all methods:

```python
from pyeztrace import trace

@trace()
class MyService:
    def __init__(self, name):
        self.name = name
    
    def process(self, data):
        # This method will be traced
        return data.upper()
    
    def analyze(self, data):
        # This method will also be traced
        return len(data)
```

When applied to a class, all methods (including `__init__`) will be traced with full tracing capabilities.

### 2. Recursive Tracing

For comprehensive application-wide tracing, you can use recursive tracing to automatically trace functions in imported modules:

```python
from pyeztrace import trace

@trace(
    recursive_depth=2,  # Trace this module and modules it imports (up to 2 levels deep)
    module_pattern="myapp.*"  # Only trace modules matching this pattern
)
def main():
    # All functions called directly or indirectly will be traced
    # as long as they're in modules matching the pattern
    result = process_data()
    return result
```

#### Parameters

- `recursive_depth`: How many levels of imports to trace (0 = only direct module)
- `module_pattern`: Pattern to match module names for recursive tracing (e.g., "myapp.*")

### 3. Double-Tracing Prevention

PyEzTrace intelligently prevents double-tracing when functions are traced via multiple mechanisms:

1. Functions directly decorated with `@trace` will not be traced again if they're called from another traced function
2. When a class is decorated with `@trace` and also traced via recursive tracing from another function
3. When the same function is traced via recursive tracing from multiple parent functions

This ensures clean logs without duplicate entries while maintaining comprehensive tracing coverage.

## Configuration

All configuration options can be set via environment variables or code:

```python
# Via environment variables
export EZTRACE_LOG_FORMAT="json"
export EZTRACE_LOG_LEVEL="DEBUG"
export EZTRACE_LOG_FILE="custom.log"
export EZTRACE_MAX_SIZE="10485760"  # 10MB
export EZTRACE_BACKUP_COUNT="5"

# Via code - must be setup before importing trace

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
