# Changelog

## v0.1.2
- Implemented trace sampling options

## v0.1.1
- Added Google Cloud OTLP support with ADC-based authentication for App Engine/GCP runtimes.
- Added `gcp` optional extra (`google-auth`) and updated OTEL documentation/examples for Cloud Trace endpoint usage.
- Updated package and OTEL resource version metadata to `0.1.1`.

## v0.1.0
- **Breaking (behavior):** trace() now initializes lazily on first use instead of automatically during module import.
- Improved viewer with more useful data.

## v0.0.14
- Added a convenience function `print()` for printing to the console with default EzTrace logger.
- New logging sinks for file and console
- Improvements to redaction

## v0.0.13
- **Breaking (behavior):** When file logging is enabled, the default file format is now JSON while the console remains colored output.
- Added `EZTRACE_CONSOLE_LOG_FORMAT` and `EZTRACE_FILE_LOG_FORMAT` to configure formats independently (legacy `EZTRACE_LOG_FORMAT` still sets both).
- Updated viewer/CLI messaging to require JSON **file** logs (not necessarily JSON console logs).
- **Breaking (behavior):** Periodic `metrics_summary` snapshots are no longer emitted into the main trace log stream; they are persisted to a sidecar file `"<logfile>.metrics"` for the viewer/UI.
- Viewer now prefers reading performance metrics snapshots from `"<logfile>.metrics"`, with fallback support for legacy in-log `metrics_summary` entries.
- Metrics scheduler runs only when metrics are enabled and file logging is enabled; console output shows only the final summary at process exit.

## v0.0.12
- Added CLI log analysis utilities (filtering, hierarchy formatting, performance summaries).
- Added memory tracking (RSS/peak/delta) to trace events and viewer display.
- Viewer improvements for JSON logs (metrics handling and richer node details).

## v0.0.11
- Enhanced trace viewer and CLI integration.
- Added redaction for argument/result previews (env-configurable and via API helpers).
- Expanded tracer previews/metadata to be safer and more informative.

## v0.0.10
- Added more logging configuration knobs (buffering, flush interval, disable file logging).
- Added CI/release GitHub workflows and expanded test coverage (including optional OpenTelemetry tests).
- Updated packaging metadata (dependency-free default install, newer Python classifiers, pytest config).

## v0.0.9
- Fixed class tracing to preserve descriptor semantics.

## v0.0.8
- Added optional OpenTelemetry integration (`pyeztrace[otel]`) and OTLP export bridge.
- Added interactive trace viewer and CLI tooling.

## v0.0.7
- Fixed issue with class method tracing.
- Added recursive tracing.
- Added double tracing prevention.

## v0.0.6
- Edge case fixes

## v0.0.5
- Fixed I/O error from logging library
- Cleaned up background logs

## v0.0.4
- Updated README with improved documentation and usage examples.

## v0.0.3
- Bug fixes:
  - Fixed issues with background process logs being captured.
  - Resolved I/O operation on closed file errors in logging.

## v0.0.2
- Cleared unnecessary dependencies.

## v0.0.1
- Initial release: PyEzTrace - Python tracing and logging library.
