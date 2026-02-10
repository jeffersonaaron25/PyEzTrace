# PyEzTrace

A **dependency-free**, lightweight Python tracing and logging library with hierarchical logging, context management, and performance metrics.

## Features

- **Hierarchical logging** — Tree-style output for nested operations
- **Multiple formats** — Color, plain, JSON, CSV, logfmt
- **Performance metrics** — Built-in timing and tracing
- **Context management** — Thread-safe context propagation
- **Log rotation** — Automatic log file management
- **Decorator-based tracing** — Easy function and method tracing
- **Thread- and async-safe** — Safe for concurrent and async code
- **Interactive viewer** — Explore traces with input/output previews, CPU, and memory
- **OpenTelemetry bridge** (optional) — Emit spans to OTLP/console, S3, or Azure Blob

## Quick start

```python
from pyeztrace.setup import Setup
from pyeztrace import trace
from pyeztrace.custom_logging import Logging

Setup.initialize("MyApp", show_metrics=True, log_format="json", log_dir="logs", log_file="app.log")
log = Logging()

@trace()
def process_order(order_id):
    with log.with_context(order_id=order_id):
        log.log_info("Processing order")
        # ... your code ...
```

Run your app, then open the trace viewer:

```bash
pyeztrace serve logs/app.log --host 127.0.0.1 --port 8765
# open http://127.0.0.1:8765
```

## Next steps

- [Getting Started](getting-started.md) — Installation and initialization
- [Usage](usage.md) — Tracing, context, formats, viewer, async
- [Configuration](configuration.md) — Environment variables and `Setup.initialize()` options
