# When to use OpenTelemetry

PyEzTrace's optional OpenTelemetry (OTEL) bridge emits spans alongside the logs
from traced functions. Enable it when you want those spans sent to an existing
telemetry destination. For a local call tree, JSON logs and the bundled viewer
work without OTEL or a collector.

| Your task | Starting point |
| --- | --- |
| Inspect a script or debug nested calls locally | Core PyEzTrace and `pyeztrace serve` |
| Check the spans emitted by a traced function | OTEL extra with the console exporter |
| Send PyEzTrace spans to your telemetry pipeline | OTEL extra with the OTLP HTTP exporter and a configured receiver |
| Instrument HTTP clients, databases, or incoming requests across services | Plan the appropriate OpenTelemetry instrumentation and context propagation separately |

OpenTelemetry provides APIs and SDKs for instrumentation; a telemetry destination
handles storage and querying. Its [Python instrumentation guide](https://opentelemetry.io/docs/languages/python/instrumentation/)
explains spans and context propagation. PyEzTrace's bridge does not automatically
instrument every dependency or configure cross-service propagation.

## Try spans without a collector

Install the optional dependencies:

```bash
python -m pip install "pyeztrace[otel]"
```

Save this as `otel_demo.py`:

```python
from pyeztrace import trace
from pyeztrace.setup import Setup
from pyeztrace.custom_logging import Logging

Setup.initialize("otel-demo", disable_file_logging=True)
log = Logging()

@trace()
def calculate_total(quantity, unit_price):
    log.log_info("Calculating total")
    return quantity * unit_price

calculate_total(2, 12)
Logging.flush_logs()
```

Set the environment before starting Python. For a POSIX shell:

```bash
export EZTRACE_OTEL_ENABLED=true
export EZTRACE_OTEL_EXPORTER=console
export EZTRACE_SERVICE_NAME=otel-demo
python otel_demo.py
```

For PowerShell:

```powershell
$env:EZTRACE_OTEL_ENABLED = "true"
$env:EZTRACE_OTEL_EXPORTER = "console"
$env:EZTRACE_SERVICE_NAME = "otel-demo"
python otel_demo.py
```

You should see the normal PyEzTrace logs and an exported span named
`calculate_total`. The console exporter writes spans to stdout. Export is batched,
so spans may appear at normal process shutdown. `Logging.flush_logs()` flushes
PyEzTrace log handlers; it is not an OTEL export flush API.

## Send spans to an OTLP receiver

With an OTLP HTTP receiver already listening locally, replace the console
exporter settings (POSIX shell):

```bash
export EZTRACE_OTEL_ENABLED=true
export EZTRACE_OTEL_EXPORTER=otlp
export EZTRACE_OTLP_ENDPOINT=http://localhost:4318/v1/traces
export EZTRACE_SERVICE_NAME=otel-demo
python otel_demo.py
```

This bridge uses OTLP over HTTP, with the full traces endpoint. Starting your
application does not start a collector. Set the endpoint and any authentication
for your own receiver; the local address above is an example.

## Existing exporters and dependencies

| Exporter | Installation | Purpose |
| --- | --- | --- |
| `console` | `pyeztrace[otel]` | Print spans locally |
| `otlp` | `pyeztrace[otel]` | Send spans to an OTLP HTTP endpoint |
| `gcp` | `pyeztrace[otel,gcp]` | Google Cloud Trace through OTLP and application default credentials |
| `s3` | `pyeztrace[otel,s3]` | Store serialized span batches in S3 |
| `azure` | `pyeztrace[otel,azure]` | Store serialized span batches in Azure Blob Storage |

The storage extras alone install their storage client, not the OTEL SDK. Use the
combined extras above or `pyeztrace[all]`. See the [existing exporter configuration](usage.md#opentelemetry-optional)
for credentials, buckets, containers, and diagnostics.

## Initialization and troubleshooting

The bridge initializes lazily on tracing. Set environment variables before the
first traced call. If required packages are missing or an exporter cannot
initialize, PyEzTrace reports diagnostics and continues logging without spans.
A remote exporter initialization failure does not switch to console unless you
explicitly set `EZTRACE_OTEL_FALLBACK_TO_CONSOLE=true`; that fallback does not
promise recovery from later delivery failures.

Enable `EZTRACE_OTEL_DEBUG=true` for initialization diagnostics. Check the selected
extra, exporter, endpoint, credentials, and receiver if spans are absent. Logs
working locally does not prove remote span delivery succeeded.

Since 0.1.6, the bridge checks provider ownership before configuring an exporter.
If your application or auto-instrumentation already installed a provider,
PyEzTrace disables its bridge and reports the conflict. The existing provider
continues to work; PyEzTrace logs remain available. Choose one provider owner
instead of expecting the bridge to replace or reconfigure an existing provider.
The bridge exports tracing spans, not every ordinary log message
or PyEzTrace metric as an OTEL signal.

Disable the bridge with `EZTRACE_OTEL_ENABLED=false` before starting a new process
to return to local-only tracing. The core remains dependency-free.

## Inspect bridge status

```python
from pyeztrace import get_otel_status

print(get_otel_status())
```

Available at the package top level since 0.1.6. This reports `enabled`,
`initialized`, `error`, exporter type, and selected environment values without
initializing the bridge or loading the optional SDK. Call it after your first
traced operation to inspect initialization results. It is a state snapshot, not
proof that a remote destination received spans. Review endpoint and error strings
before sharing diagnostic output.

## Test against a local Collector

From a source checkout, use a Python environment with `pyeztrace[otel]` installed
and an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/configuration/)
binary that includes the OTLP receiver and file exporter:

```bash
python scripts/smoke_otel.py --collector /path/to/otelcol
```

The script binds an available loopback port, runs nested async calls and a handled
exception, checks the received span identities, parent links, error event, and
service name, then stops its Collector. It prints a temporary evidence directory
containing the configuration, application output, Collector log, and received
spans. It configures no external destination. Verified with Collector 0.160.0 on
macOS arm64; this manual integration check is separate from the unit-test suite.
