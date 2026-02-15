# Getting Started

## Installation

```
pip install pyeztrace
```

Optional extras (library stays dependency-free by default):

| Extra              | Purpose                                 |
| ------------------ | --------------------------------------- |
| `pyeztrace[otel]`  | OpenTelemetry SDK and OTLP exporter     |
| `pyeztrace[gcp]`   | Google ADC auth for OTLP to Cloud Trace |
| `pyeztrace[s3]`    | S3 exporter for span batches            |
| `pyeztrace[azure]` | Azure Blob exporter                     |
| `pyeztrace[all]`   | All optional dependencies               |

For the full test suite including OTEL coverage:

```
pip install "pyeztrace[otel]"
```

## Initialize once at startup

Recommended: call `Setup.initialize(...)` before any traced code runs. Logging is configured once; doing this first gives predictable behavior.

```
from pyeztrace.setup import Setup
from pyeztrace import trace
from pyeztrace.custom_logging import Logging

Setup.initialize(
    "MyApp",
    show_metrics=True,
    log_format="json",
    log_file="app.log",
    log_dir="logs",
    disable_file_logging=False,
)
log = Logging()
```

## Initialization order

- Importing `trace` does **not** initialize setup; initialization is lazy on first use.
- For predictable behavior, either:
- Set **environment variables** before process start (e.g. `EZTRACE_LOG_DIR=logs`, `EZTRACE_LOG_FILE=app.log`), or
- Call **`Setup.initialize(...)`** before the first traced call.

Configuration precedence (highest to lowest):

1. Arguments passed to `Setup.initialize(...)`
1. Environment variables (`EZTRACE_*`)
1. Built-in defaults

## Quick example

```
@trace()
def process_order(order_id):
    with log.with_context(order_id=order_id):
        log.log_info("Processing order")
        validate_order(order_id)
        log.log_info("Order processed successfully")

@trace()
def validate_order(order_id):
    log.log_info("Validating order")
    # your logic
```

Example console output (with `log_format="color"` or `"plain"`):

```
2025-05-13T10:00:00 - INFO - [MyApp] ├── process_order called...
2025-05-13T10:00:00 - INFO - [MyApp] ├── Processing order Data: {order_id: "123"}
2025-05-13T10:00:00 - INFO - [MyApp] │    ├─── validate_order called...
2025-05-13T10:00:00 - INFO - [MyApp] │    ├─── validate_order Ok. (took 0.50 seconds)
2025-05-13T10:00:01 - INFO - [MyApp] ├── process_order Ok. (took 1.23 seconds)
```

See [Usage](https://jeffersonaaron25.github.io/pyeztrace/usage/index.md) for tracing options, formats, CLI, and the interactive viewer.
