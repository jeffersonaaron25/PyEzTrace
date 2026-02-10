# Usage

## Tracing options

Fine-grained control with the `@trace()` decorator:

```python
@trace(
    message="Custom trace message",
    stack=True,  # Include stack trace on errors
    modules_or_classes=[my_module],
    include=["specific_function_*"],
    exclude=["ignored_function_*"],
    recursive_depth=2,
    module_pattern="myapp.*",
)
def function():
    pass
```

### Recursive tracing

Trace the function and functions in imported modules:

```python
@trace(recursive_depth=1, module_pattern="myapp.*")
def app_entry():
    # Traces this and matching imported modules
    pass
```

Use `module_pattern` to avoid tracing system or third-party code.

### Redacting sensitive data

```python
@trace(
    redact_keys=["password", "token"],
    redact_value_patterns=[r"secret\d+"],
    redact_presets=["pii"],
)
def process(user, password, token):
    return {"user": user, "status": "ok"}
```

Environment defaults: `EZTRACE_REDACT_KEYS`, `EZTRACE_REDACT_PATTERN`, `EZTRACE_REDACT_VALUE_PATTERNS`, `EZTRACE_REDACT_PRESETS`.

## Context management

Thread-safe context propagation:

```python
with log.with_context(user_id="123", action="login"):
    log.log_info("User logged in")  # Includes context

    with log.with_context(session="abc"):
        log.log_info("Session started")  # Inherits parent context
```

## Output formats

| Format | Use case |
|--------|----------|
| `color` | Default console; hierarchical tree |
| `json` | Machine-readable; required for the viewer |
| `plain` | Simple text |
| `csv` | Spreadsheet analysis |
| `logfmt` | System-style key=value |

```python
log = Logging(log_format="json")  # or "color", "plain", "csv", "logfmt"
```

Per-sink: set `console_format` and `file_format` via `Setup.initialize(..., console_format="color", file_format="json")` or env `EZTRACE_CONSOLE_LOG_FORMAT` / `EZTRACE_FILE_LOG_FORMAT`.

## Interactive viewer

1. Enable JSON file logging (e.g. `Setup.initialize(..., file_format="json", disable_file_logging=False)`).
2. Run your app to produce logs.
3. Start the viewer:

```bash
pyeztrace serve logs/app.log --host 127.0.0.1 --port 8765
```

Open `http://127.0.0.1:8765`. You get:

- Hierarchical tree (parent/child calls)
- Input/output previews, duration, CPU, memory
- Filters (function, error, min duration), auto-refresh

## Async support

```python
@trace()
async def async_handler():
    await some_async_work()
    log.log_info("Done")
```

Setup and level tracking are async-safe.

## Redirecting `print` to logging

```python
from pyeztrace import print  # noqa: A001

Setup.initialize("MyApp")
print("Hello")                      # INFO
print("Warning", level="WARNING")  # WARNING
```

Falls back to built-in `print` if EzTrace is not initialized or when writing to a custom file.

## Performance metrics

```python
Setup.initialize("MyApp", show_metrics=True)

@trace()
def monitored():
    pass
```

At exit, a summary is printed: calls, total time, average per function.

## Error handling

```python
log.log_debug("Debug")
log.log_info("Info")
log.log_warning("Warning")
log.log_error("Error")

try:
    risky()
except Exception as e:
    log.raise_exception_to_log(e, "Custom message", stack=True)
```

## Applying `@trace` to classes

Decorate a class to trace all its methods (including `__init__`):

```python
from pyeztrace import trace

@trace()
class MyService:
    def __init__(self, name):
        self.name = name

    def process(self, data):
        return data.upper()

    def analyze(self, data):
        return len(data)
```

Each method gets full tracing (start/end, duration, args/result preview).

## Double-tracing prevention

PyEzTrace avoids duplicate trace entries when:

- A function is decorated with `@trace` and also called from another traced function
- A class is decorated and also traced via recursive tracing
- The same function is traced from multiple parent functions (recursive tracing)

Only one trace is emitted per call, so logs stay clean while coverage stays full.

## Thread-safe high-volume logging

The logger and tracer are thread-safe. Example with a thread pool:

```python
from concurrent.futures import ThreadPoolExecutor
from pyeztrace import trace
from pyeztrace.custom_logging import Logging

log = Logging()

@trace()
def worker(worker_id):
    with log.with_context(worker_id=worker_id):
        log.log_info("Started")
        # ... work ...
        log.log_info("Finished")

with ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(worker, range(5))
```

## Global redaction (programmatic)

Set default redaction for all traces in code:

```python
from pyeztrace import set_global_redaction

set_global_redaction(
    redact_keys=["password", "token"],
    redact_pattern=r"(?i)secret",
    redact_value_patterns=[r"secret\d+"],
    presets=["pii"],
)
```

Same options as per-decorator `redact_keys`, `redact_pattern`, `redact_value_patterns`, and `redact_presets`.

## CLI

| Command | Description |
|--------|-------------|
| `pyeztrace serve <log_file>` | Start the interactive trace viewer (default: http://127.0.0.1:8765) |
| `pyeztrace print <log_file>` | Print or filter log entries from a file |

**Viewer:**

```bash
pyeztrace serve logs/app.log --host 127.0.0.1 --port 8765
```

Optional env: `EZTRACE_VIEW_HOST`, `EZTRACE_VIEW_PORT`.

**Print / analyze logs:**

```bash
pyeztrace print logs/app.log
pyeztrace print logs/app.log --analyze
pyeztrace print logs/app.log --function my_func --format json
```

Use `--level`, `--since`, `--until` to filter. `--analyze` shows performance metrics; `--function` limits analysis to that function.

## OpenTelemetry (optional)

Install: `pip install "pyeztrace[otel]"`. Enable with environment variables.

**OTLP (collector):**

```bash
export EZTRACE_OTEL_ENABLED=true
export EZTRACE_OTEL_EXPORTER=otlp
export EZTRACE_OTLP_ENDPOINT="http://localhost:4318/v1/traces"
# optional: export EZTRACE_SERVICE_NAME="my-service"
```

**Console (local dev):**

```bash
export EZTRACE_OTEL_ENABLED=true
export EZTRACE_OTEL_EXPORTER=console
```

**S3 / Azure:** Install `pyeztrace[s3]` or `pyeztrace[azure]`, set `EZTRACE_OTEL_EXPORTER=s3` or `azure`, and the bucket/container and credential env vars. See the [README OpenTelemetry section](https://github.com/jeffersonaaron25/pyeztrace#10-opentelemetry-spans-optional) for full S3/Azure options.

The bridge is lazy-loaded; if OTEL packages are missing, the library still works without spans. Spans use function `__qualname__`; exceptions are recorded on the active span.
