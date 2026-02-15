# PyEzTrace

Dependency-free tracing, structured logs, and an interactive viewer for understanding real execution paths.

[Get Started](https://jeffersonaaron25.github.io/pyeztrace/getting-started) [Explore Usage](https://jeffersonaaron25.github.io/pyeztrace/usage)

## Why teams use PyEzTrace

### Hierarchical traces

Tree-style output makes nested execution paths readable without extra tooling.

### Flexible log formats

Color, plain, JSON, CSV, and logfmt support one code path from local dev to production.

### Runtime metrics

Collect timing data and context propagation details where latency and failures actually happen.

### Interactive viewer

Inspect input/output previews, CPU, and memory from trace logs in a focused web UI.

> **Note:** The trace viewer UI (`pyeztrace serve`) is designed for **local development and analysis**—it is **not** intended to be used as a hosted or production solution.

## Quick start

```
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

```
pyeztrace serve logs/app.log --host 127.0.0.1 --port 8765
# open http://127.0.0.1:8765
```

## Next steps

- [Getting Started](https://jeffersonaaron25.github.io/pyeztrace/getting-started/index.md) - Installation and initialization
- [Usage](https://jeffersonaaron25.github.io/pyeztrace/usage/index.md) - Tracing, context, formats, viewer, and async support
- [Configuration](https://jeffersonaaron25.github.io/pyeztrace/configuration/index.md) - Environment variables and `Setup.initialize()` options
