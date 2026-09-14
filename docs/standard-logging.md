# PyEzTrace and standard logging

Use Python's standard `logging` module when you need application messages,
levels, handlers, and control over where records go. It also lets your application
collect messages from libraries that use the same logging system. See the
[Python logging reference](https://docs.python.org/3/library/logging.html).

Use PyEzTrace when your debugging question is about **execution**: which function
called which, how long a call took, what it returned, and where an exception
occurred. Its decorators emit call records that the local viewer can reconstruct
as a tree. The default installation has no required third-party dependencies.

## What each provides

| Need | Standard logging | PyEzTrace |
| --- | --- | --- |
| Application messages and levels | Loggers, handlers, filters, and formatters | Logging helpers with color, plain, JSON, CSV, and logfmt output |
| Hierarchy | Logger names, usually matching Python modules | Nested traced calls with call and parent identifiers |
| Call start, completion, and duration | Add instrumentation yourself | `@trace()` records these for traced functions |
| Context fields | `extra`, adapters, filters, or custom configuration | `Logging().with_context(...)` adds scoped logging context |
| Local call-tree inspection | Supply your own viewer and record format | `pyeztrace serve` reads PyEzTrace JSON logs |
| Exporting spans | Requires a tracing integration | Optional OpenTelemetry bridge; see [when to use OTEL](opentelemetry.md) |

Standard logging's logger hierarchy describes names such as `app.database`.
PyEzTrace's call hierarchy describes execution such as `process_order` calling
`validate_order`. They answer different questions.

## Try a call tree

Save this as `app.py` and run it with `python app.py`:

```python
from pyeztrace import trace
from pyeztrace.setup import Setup
from pyeztrace.custom_logging import Logging

Setup.initialize(
    "orders", disable_file_logging=False,
    log_dir="logs", log_file="orders.log", file_format="json",
)
log = Logging()

@trace()
def validate_order(order_id):
    log.log_info("Validating order", order_id=order_id)
    return {"valid": True}

@trace()
def process_order(order_id):
    with log.with_context(order_id=order_id):
        return validate_order(order_id)

process_order("example-123")
Logging.flush_logs()
```

Inspect the recorded calls:

```bash
pyeztrace serve logs/orders.log --open
```

The viewer shows `process_order` and its nested `validate_order` call, along with
completion, duration, and recorded data. It is a local development and analysis
tool; it is not a hosted observability service.

## Keep existing logging where it fits

PyEzTrace uses a dedicated `pyeztrace` logger and configures its own handlers;
its records do not propagate to the root logger. Existing calls to
`logging.getLogger(__name__)` can stay in your application, but their records do
not automatically become PyEzTrace call events. Configuring the root logger with
`basicConfig()` does not configure PyEzTrace's sinks.

Initialize PyEzTrace once, before constructing `Logging` or running traced code.
Start with explicit decorators on the functions you want to inspect. Recursive
tracing can wrap additional calls and increase output and overhead; review the
[tracing options](usage.md) before enabling it broadly. Review captured inputs and
outputs and configure redaction for your data.

Keep standard logging if messages and your existing handler configuration already
answer your questions. Add PyEzTrace where a call tree helps; enable
[OpenTelemetry](opentelemetry.md) when those spans need an export destination.
