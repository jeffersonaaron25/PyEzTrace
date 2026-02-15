# Configuration

All options can be set via **environment variables** or **code**. Precedence: `Setup.initialize(...)` kwargs > env vars > defaults.

## Via `Setup.initialize()`

Recommended for applications:

```
from pyeztrace.setup import Setup

Setup.initialize(
    "MyApp",
    show_metrics=True,
    disable_file_logging=False,
    log_format="json",
    console_format="color",
    file_format="json",
    log_level="DEBUG",
    log_file="app.log",
    log_dir="logs",
    max_size=10 * 1024 * 1024,  # 10MB
    backup_count=5,
    buffer_enabled=False,
    buffer_flush_interval=1.0,
)
```

## Via environment variables

```
export EZTRACE_LOG_FORMAT="json" # legacy: sets both console and file
export EZTRACE_CONSOLE_LOG_FORMAT="color"
export EZTRACE_FILE_LOG_FORMAT="json"
export EZTRACE_LOG_LEVEL="DEBUG"
export EZTRACE_LOG_FILE="app.log"
export EZTRACE_LOG_DIR="logs"
export EZTRACE_MAX_SIZE="10485760" # 10MB
export EZTRACE_BACKUP_COUNT="5" # Keep 5 rotated files
export EZTRACE_DISABLE_FILE_LOGGING="0"
export EZTRACE_BUFFER_ENABLED="false" # Disable buffering by default (configurable via env)
export EZTRACE_BUFFER_FLUSH_INTERVAL="1.0" # Seconds between flushes when buffering
export EZTRACE_SAMPLE_RATE="1.0" # Global fixed sampling rate in [0.0, 1.0]
export EZTRACE_ADAPTIVE_SAMPLING="false" # Keep slow/error traces at 100%, sample normal traces
export EZTRACE_ADAPTIVE_SLOW_THRESHOLD="1.0" # Seconds; slow traces are always kept when adaptive is enabled
```

Set these **before** the first traced or logging use so they apply when the logger is created.

## Console-only (no file logging)

```
export EZTRACE_DISABLE_FILE_LOGGING=1
```

Or in code:

```
Setup.initialize("MyApp", disable_file_logging=True)
```

## Log rotation

Log files are rotated by size. Configure before first use:

```
# In Setup.initialize()
max_size=10 * 1024 * 1024,  # 10MB per file
backup_count=5,              # Keep 5 rotated files
log_dir="logs",
log_file="app.log",
```

Env: `EZTRACE_MAX_SIZE`, `EZTRACE_BACKUP_COUNT`, `EZTRACE_LOG_DIR`, `EZTRACE_LOG_FILE`.

## Buffered logging

When enabled, log records are buffered and flushed on interval or buffer full. Reduces I/O under high volume.

- **Env:** `EZTRACE_BUFFER_ENABLED=true`, `EZTRACE_BUFFER_FLUSH_INTERVAL=1.0`
- **Code:** `Setup.initialize(..., buffer_enabled=True, buffer_flush_interval=1.0)`

Default is disabled. Set before the first traced/logging use.

## Redaction (env defaults)

```
export EZTRACE_REDACT_KEYS="password,token,secret"
export EZTRACE_REDACT_PATTERN="(?i)auth|secret"
export EZTRACE_REDACT_VALUE_PATTERNS="secret\d+"
export EZTRACE_REDACT_PRESETS="pii,phi"
```

For programmatic defaults, use `set_global_redaction()` — see [Usage: Global redaction](https://jeffersonaaron25.github.io/pyeztrace/usage/#global-redaction-programmatic).

## Sampling

- **Global fixed sampling:** `EZTRACE_SAMPLE_RATE` (float from `0.0` to `1.0`, default `1.0`)
- **Global adaptive mode:** `EZTRACE_ADAPTIVE_SAMPLING` (`true`/`false`, default `false`)
- **Global adaptive slow threshold:** `EZTRACE_ADAPTIVE_SLOW_THRESHOLD` (float seconds, `>= 0.0`, default `1.0`)

When adaptive sampling is enabled, slow/error traces are always kept; normal traces use the configured sample rate.
