# Usage

## Tracing options

Fine-grained control with the `@trace()` decorator:

```python
@trace(
    message="Custom trace message",
    stack=True,  # Include stack trace on errors
    sample_rate=0.5,  # Optional local override for this trace
    adaptive_sampling=True,  # Optional local adaptive override
    adaptive_slow_threshold=0.25,  # Optional local slow-threshold override (seconds)
    modules_or_classes=[my_module],
    include=["specific_function_*"],
    exclude=["ignored_function_*"],
    recursive_depth=2,
    module_pattern="myapp.*",
)
def function():
    pass
```

### Sampling

Global env controls:

- `EZTRACE_SAMPLE_RATE` (`0.0` to `1.0`, default `1.0`)
- `EZTRACE_ADAPTIVE_SAMPLING` (`true`/`false`, default `false`)
- `EZTRACE_ADAPTIVE_SLOW_THRESHOLD` (seconds, `>= 0.0`, default `1.0`)

Local override:

```python
@trace(
    sample_rate=1.0,            # Force this trace to always be kept
    adaptive_sampling=True,     # Local adaptive mode override
    adaptive_slow_threshold=0.1 # Local slow threshold override
)
def critical_path():
    pass
```

Adaptive mode keeps slow/error traces at 100% and samples normal traces using the configured rate.

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

1. Enable JSON file logging for your instrumented application:

```bash
export EZTRACE_DISABLE_FILE_LOGGING=0
export EZTRACE_FILE_LOG_FORMAT=json
export EZTRACE_LOG_FILE="$PWD/logs/app.log"
```

Or initialize explicitly with `Setup.initialize("MyApp", log_dir="logs", log_file="app.log", file_format="json", disable_file_logging=False)`.

2. Start the viewer — you can do this *before* the app runs:

```bash
pyeztrace serve logs/app.log --open
```

`--open` launches your default browser once the server is listening. The log
file does not need to exist yet: the viewer explains what it is waiting for and
starts rendering automatically as soon as trace records appear, so the usual
workflow is to leave it running in one terminal and start and restart your app
in another.

3. Run your app to produce logs.

You get:

- Hierarchical tree (parent/child calls)
- Input/output previews, duration, CPU, memory
- Filters (function, error, min duration), auto-refresh

### Reading the live status bar

The header always shows two independent facts. They are deliberately not merged
into a single "last updated" value, because they answer different questions:

| Indicator | Meaning |
|-----------|---------|
| **Connection state** | `Connected`, `Reconnecting`, `Disconnected`, or `Paused - tab in background`. If the viewer cannot reach the server, the last snapshot stays on screen and is labelled stale rather than silently presented as current. |
| **Last checked** | When the viewer last reached the server. This is connection health; it advances on every successful poll even when no new traces arrive. |
| **Newest trace** | Age of the most recent record in the log file. This is data freshness. If it stops advancing, your application has stopped emitting traces. |

An inactive log does not prove the application stopped — it only means no new
records have been written. The viewer reports what it can actually observe.

### Calls that are still running

A call that has started but not finished is shown with a **running** badge and a
live elapsed timer instead of a duration. Its CPU and memory values read
`pending`, never `0`, because those are only recorded on completion. Running
calls are excluded from success and error rates so in-flight work is never
counted as a failure; they appear under **Running now** instead.

### Pausing

The **Auto refresh** toggle pauses the *view* only. Your application keeps
running and the viewer keeps reading the log; while paused the status bar
reports how many new calls arrived but are not being shown. Selection, scroll
position and filters are preserved when you resume — including across a restart
of the viewer itself, since the log is identified by its contents rather than by
the server process. Elapsed timers freeze while paused or disconnected, dimmed
and marked with a pause glyph; a frozen timer shows the elapsed time as of the
last successful read and never counts past it. Retry checks the connection
without resuming a paused view; the Refresh button explicitly loads a new
snapshot. Log replacement or truncation prompts you to resume rather than
reporting a misleading call count.
A start record without an end record cannot prove a process is alive: calls open
for over five minutes are labeled as possibly still running.

### When the dashboard is empty

Instead of a grid of zeroes, the viewer names the reason and what to do next. It
distinguishes four cases: the log file does not exist yet, it exists but has no
records yet, it has content that is not JSON trace output (usually
`EZTRACE_FILE_LOG_FORMAT` was set to a text format), and records exist but the current filters
hide all of them.

> **Note:** The trace viewer UI (`pyeztrace serve`) is designed for **local development and analysis**—it is **not** intended to be used as a hosted or production solution.

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
pyeztrace serve logs/app.log --host 127.0.0.1 --port 8765 --open
```

| Flag | Description |
|------|-------------|
| `--host` | Bind address (default `127.0.0.1`) |
| `--port` | Port (default `8765`); a clash reports the conflict and suggests another port |
| `--open` | Open the viewer in your default browser once the server is listening |

Optional env: `EZTRACE_VIEW_HOST`, `EZTRACE_VIEW_PORT`.

The log file may be missing when the viewer starts; it will wait for it and say
so both in the terminal and in the browser.

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
# optional, default false: export EZTRACE_OTEL_FALLBACK_TO_CONSOLE=true
```

**Google Cloud Trace (OTLP + ADC):**

```bash
pip install "pyeztrace[otel,gcp]"
export EZTRACE_OTEL_ENABLED=true
export EZTRACE_OTEL_EXPORTER=gcp
# optional override; defaults to telemetry endpoint for exporter=gcp
export EZTRACE_OTLP_ENDPOINT="https://telemetry.googleapis.com/v1/traces"
# optional explicit toggle
export EZTRACE_OTLP_GCP_AUTH=true
```

If Cloud Trace returns `Resource is missing required attribute "gcp.project_id"`, set:
`EZTRACE_GCP_PROJECT_ID` (or `GOOGLE_CLOUD_PROJECT` / `GCLOUD_PROJECT` / `GCP_PROJECT`).

**Console (local dev):**

```bash
export EZTRACE_OTEL_ENABLED=true
export EZTRACE_OTEL_EXPORTER=console
```

**S3 / Azure:** Install `pyeztrace[s3]` or `pyeztrace[azure]`, set `EZTRACE_OTEL_EXPORTER=s3` or `azure`, and the bucket/container and credential env vars. See the [README OpenTelemetry section](https://github.com/jeffersonaaron25/pyeztrace#10-opentelemetry-spans-optional) for full S3/Azure options.

The bridge is lazy-loaded; if OTEL packages are missing, the library still works without spans. Spans use function `__qualname__`; exceptions are recorded on the active span. Remote exporter initialization fails closed unless `EZTRACE_OTEL_FALLBACK_TO_CONSOLE=true` is explicitly set.

For troubleshooting, enable OTEL diagnostics:

```bash
export EZTRACE_OTEL_DEBUG=true
```

And inspect runtime OTEL state in code:

```python
from pyeztrace import otel
print(otel.get_otel_status())
```

`otel.get_otel_status()` is available in newer builds after `0.1.1`.

## Scripted and agent-driven CLI use

Commands are non-interactive; `serve` only opens a browser with `--open`.
Use explicit subcommands and `--format json` to consume one JSON value on stdout:

```bash
pyeztrace print logs/app.log --format json > records.json
pyeztrace print logs/app.log --analyze --since 2026-09-01 --format json > metrics.json
pyeztrace print logs/app.log --errors --until 2026-10-01 --context order_id=123 --format json
pyeztrace print logs/app.log --analyze --function process_order --format json
```

Date, level, and context filters apply to all three modes. Context values match
strings exactly; values may contain `=`. Dates without time zones use local time.
`--analyze` and `--errors` are mutually exclusive; `--function` requires `--analyze`.
Empty matches are successful results (`[]` or `{}`), including an error search
with no matching errors. Error records in the input do not change the exit code.

| Exit code | Meaning |
|-----------|---------|
| 0 | Command succeeded, including no matches |
| 1 | File access or server startup failed |
| 2 | Invalid arguments or configuration |

Diagnostics go to stderr; successful JSON output goes to stdout. Text uses
`--color auto` by default (no ANSI when redirected or when `NO_COLOR` is set).
Use `--color always` or `--color never` to override. JSON is always uncolored.
A missing source is allowed for `serve`, which waits for it, but fails for `print`.
Invalid log lines are skipped; the viewer reports their count. JSON output shapes
remain the existing record list or function-to-metrics mapping.

### Future CLI improvements

Later work should add versioned output schemas and structured error envelopes,
explicit tree/call/payload inspection commands, bounded output with cursor-based
pagination, stdin and JSONL streaming, a doctor command, and optional CI failure
predicates. A combined application/viewer launcher needs deliberate process and
signal handling. These are proposals, not flags in this release.

The current conventions follow the [Command Line Interface Guidelines](https://clig.dev/)
for machine-readable output, stderr diagnostics, terminal-aware color, and
non-interactive use, and [Python argparse](https://docs.python.org/3/library/argparse.html)
for usage errors and argument validation.
