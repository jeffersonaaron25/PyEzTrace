# PyEzTrace {.hero-title}

<div class="hero-min">
  <p class="hero-subtitle">
    Dependency-free tracing, structured logs, and an interactive viewer for understanding real execution paths.
  </p>
  <div class="hero-actions">
    <a class="md-button md-button--primary" href="getting-started">Get Started</a>
    <a class="md-button" href="usage">Explore Usage</a>
  </div>
</div>

## Why teams use PyEzTrace

<div class="feature-grid">
  <article class="feature-card">
    <h3>Hierarchical traces</h3>
    <p>Tree-style output makes nested execution paths readable without extra tooling.</p>
  </article>
  <article class="feature-card">
    <h3>Flexible log formats</h3>
    <p>Color, plain, JSON, CSV, and logfmt support one code path from local dev to production.</p>
  </article>
  <article class="feature-card">
    <h3>Runtime metrics</h3>
    <p>Collect timing data and context propagation details where latency and failures actually happen.</p>
  </article>
  <article class="feature-card">
    <h3>Interactive viewer</h3>
    <p>Inspect input/output previews, CPU, and memory from trace logs in a focused web UI.</p>
  </article>
</div>

> **Note:** The trace viewer UI (`pyeztrace serve`) is designed for **local development and analysis**—it is **not** intended to be used as a hosted or production solution.

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

- [Getting Started](getting-started.md) - Installation and initialization
- [Usage](usage.md) - Tracing, context, formats, viewer, and async support
- [Configuration](configuration.md) - Environment variables and `Setup.initialize()` options
