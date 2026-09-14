# Environment variable reference

This reference covers every `EZTRACE_*` setting read by the package. Set values
before process startup: logging configuration and some metrics settings are read
at import or first initialization. OTEL initializes lazily on the first traced
call. Changing the environment after handlers or exporters exist does not
reconfigure them.

For the options accepted by `Setup.initialize`, explicit code options take
precedence over environment defaults. Not every variable below has a matching
setup argument. See [Configuration](configuration.md) for examples.

## Logging and metrics

| Variable | Default | Meaning |
| --- | --- | --- |
| `EZTRACE_LOG_FORMAT` | Unset | Legacy format for both sinks; `color`, `plain`, `json`, `csv`, or `logfmt` |
| `EZTRACE_CONSOLE_LOG_FORMAT` | `color` | Console format; a sink-specific choice overrides the legacy format |
| `EZTRACE_FILE_LOG_FORMAT` | `json` | File format; use JSON for the viewer |
| `EZTRACE_LOG_LEVEL` | `DEBUG` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`, or `NOTSET` |
| `EZTRACE_LOG_DIR` | `logs` | Directory for relative log paths |
| `EZTRACE_LOG_FILE` | `app.log` | File name or explicit path |
| `EZTRACE_MAX_SIZE` | `10485760` | Rotation threshold in bytes; nonnegative integer |
| `EZTRACE_BACKUP_COUNT` | `5` | Rotated-file backup count; nonnegative integer |
| `EZTRACE_DISABLE_FILE_LOGGING` | `1` | Disable file output; set `0` to create viewer logs |
| `EZTRACE_BUFFER_ENABLED` | `false` | Enable buffered logging |
| `EZTRACE_BUFFER_FLUSH_INTERVAL` | `1.0` | Seconds between buffer flushes; positive, configuration minimum `0.001` |
| `EZTRACE_DISABLE_RESOURCE_METRICS` | `false` | Disable CPU/memory resource measurements |
| `EZTRACE_METRICS_INTERVAL` | `5.0` | Minimum interval in seconds between metrics sidecar writes; positive number |
| `EZTRACE_SHOW_DATA_IN_CLI` | `0` | Include structured data in the logger's human-readable output; not the CLI JSON-output switch |

Logging boolean settings recognize `1`, `true`, `yes`, and `on`, case-insensitively.
Invalid validated numeric/format settings warn and fall back to defaults.

## Redaction and sampling

| Variable | Default | Meaning |
| --- | --- | --- |
| `EZTRACE_REDACT_KEYS` | Unset | Comma-separated keys to redact |
| `EZTRACE_REDACT_PATTERN` | Unset | Regular expression matching keys |
| `EZTRACE_REDACT_VALUE_PATTERNS` | Unset | Comma-separated regular expressions matching values |
| `EZTRACE_REDACT_PRESETS` | Unset | Comma-separated `pii` / `phi` presets |
| `EZTRACE_SAMPLE_RATE` | `1.0` | Fraction of ordinary traces kept, between 0 and 1 |
| `EZTRACE_ADAPTIVE_SAMPLING` | `false` | Retain slow/error traces while sampling ordinary traces |
| `EZTRACE_ADAPTIVE_SLOW_THRESHOLD` | `1.0` | Slow-call threshold in seconds; nonnegative |

Decorator options and programmatic redaction defaults also affect capture. See
[Usage](usage.md) for precedence and supported patterns. Redaction does not make
all arbitrary log messages safe to publish.

## Viewer and CLI

| Variable | Default | Meaning |
| --- | --- | --- |
| `EZTRACE_VIEW_HOST` | `127.0.0.1` | `serve` bind host; overridden by `--host` |
| `EZTRACE_VIEW_PORT` | `8765` | `serve` port, 0–65535; `0` selects an available port; overridden by `--port` |

`NO_COLOR` is a standard, non-prefixed variable: its presence disables ANSI color
for CLI `--color auto`. Explicit `--color always` or `never` takes precedence.
`BROWSER` is interpreted by Python's `webbrowser` module when `serve --open` is used.

## OpenTelemetry bridge

| Variable | Default | Meaning |
| --- | --- | --- |
| `EZTRACE_OTEL_ENABLED` | `false` | Enable optional spans; requires OTEL packages |
| `EZTRACE_OTEL_EXPORTER` | `otlp` | `otlp`, `console`, `gcp`, `s3`, or `azure` |
| `EZTRACE_SERVICE_NAME` | Setup project, otherwise `PyEzTrace` | Span resource service name |
| `EZTRACE_OTLP_ENDPOINT` | `http://localhost:4318/v1/traces` | Full OTLP HTTP traces URL; `gcp` defaults to `https://telemetry.googleapis.com/v1/traces` |
| `EZTRACE_OTLP_HEADERS` | Unset | Comma-separated `key=value` exporter headers |
| `EZTRACE_OTEL_DEBUG` | `false` | Write bridge initialization diagnostics to stderr |
| `EZTRACE_OTEL_FALLBACK_TO_CONSOLE` | `false` | Allow console fallback after exporter initialization failure; not a delivery retry policy |
| `EZTRACE_OTLP_GCP_AUTH` | Automatic | Use Google ADC auth for GCP exporter/telemetry endpoint; explicit boolean overrides detection |
| `EZTRACE_GCP_PROJECT_ID` | Auto-detected | GCP project resource attribute override |
| `EZTRACE_GCP_SCOPES` | `https://www.googleapis.com/auth/cloud-platform` | Comma- or space-separated ADC scopes |

OTEL boolean settings additionally recognize `y`. For project discovery, the bridge
checks `EZTRACE_GCP_PROJECT_ID`, `GOOGLE_CLOUD_PROJECT`, `GCLOUD_PROJECT`, and
`GCP_PROJECT` in that order, then tries ADC. Explicit authorization headers take
precedence over automatic Google bearer-header configuration.

## Storage exporters

Install `pyeztrace[otel,s3]` or `pyeztrace[otel,azure]` for these exporters.

| Variable | Default | Meaning |
| --- | --- | --- |
| `EZTRACE_COMPRESS` | `true` | Gzip serialized span batches for storage exporters |
| `EZTRACE_S3_BUCKET` | Required for S3 | Destination bucket |
| `EZTRACE_S3_PREFIX` | `traces/` | Object-name prefix |
| `EZTRACE_S3_REGION` | SDK default | Boto3 region override |
| `EZTRACE_AZURE_CONTAINER` | Required for Azure | Destination container |
| `EZTRACE_AZURE_PREFIX` | `traces/` | Blob-name prefix |
| `EZTRACE_AZURE_CONNECTION_STRING` | Unset | Azure connection string; preferred over account URL if both are set |
| `EZTRACE_AZURE_ACCOUNT_URL` | Unset | Alternative account URL passed to the blob client; include a SAS token when needed |

At least one Azure connection setting is required. The account-URL path does not
construct a `DefaultAzureCredential`. AWS credentials are resolved by boto3's
normal credential chain. Google ADC and SDK-specific credentials have their own
configuration outside the `EZTRACE_*` namespace.

See [When to use OpenTelemetry](opentelemetry.md) for working examples and
provider-ownership limitations.
