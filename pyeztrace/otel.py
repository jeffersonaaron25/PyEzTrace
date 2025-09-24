import os
import json
import time
import gzip
import uuid
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Optional

# Internal EZTrace Setup for project name
from pyeztrace.setup import Setup


class _OtelState:
    """Holds OpenTelemetry state lazily, without importing heavy deps by default."""
    enabled: bool = False
    initialized: bool = False
    tracer_provider = None
    tracer = None
    span_processor = None
    exporter = None
    error: Optional[str] = None


_state = _OtelState()


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.environ.get(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _build_exporter(exporter_name: str):
    """
    Build an exporter instance based on name. Import heavy libs only on demand.
    Supported: 'console', 'otlp', 's3', 'azure'.
    Returns (exporter, error_str)
    """
    name = (exporter_name or "").strip().lower()

    # Console exporter (dev friendly, no extra deps)
    if name in ("console", "stdout"):
        try:
            from opentelemetry.sdk.trace.export import ConsoleSpanExporter
            return ConsoleSpanExporter(), None
        except Exception as e:
            return None, f"Console exporter requires OpenTelemetry SDK: {e}"

    # OTLP HTTP exporter (default OTEL path)
    if name in ("otlp", "otlphttp", "otlp-http"):
        endpoint = os.environ.get("EZTRACE_OTLP_ENDPOINT", "http://localhost:4318/v1/traces")
        headers = _parse_headers(os.environ.get("EZTRACE_OTLP_HEADERS", ""))
        try:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
            return OTLPSpanExporter(endpoint=endpoint, headers=headers or None), None
        except Exception as e:
            return None, f"OTLP HTTP exporter requires opentelemetry-exporter-otlp: {e}"

    # S3 exporter (optional, requires boto3)
    if name in ("s3",):
        try:
            import boto3  # noqa: F401
        except Exception as e:
            return None, f"S3 exporter requires boto3: {e}"
        try:
            return _S3SpanExporter(), None
        except Exception as e:
            return None, f"Error creating S3 exporter: {e}"

    # Azure Blob exporter (optional)
    if name in ("azure", "azureblob", "azure-blob"):
        try:
            from azure.storage.blob import BlobServiceClient  # noqa: F401
        except Exception as e:
            return None, f"Azure exporter requires azure-storage-blob: {e}"
        try:
            return _AzureBlobSpanExporter(), None
        except Exception as e:
            return None, f"Error creating Azure exporter: {e}"

    # Fallback to no exporter
    return None, f"Unknown exporter '{exporter_name}'"


def _parse_headers(header_str: str) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    if not header_str:
        return headers
    # Format: key1=val1,key2=val2
    for part in header_str.split(','):
        if '=' in part:
            k, v = part.split('=', 1)
            headers[k.strip()] = v.strip()
    return headers


def _span_to_dict(span) -> Dict[str, Any]:
    # Convert ReadableSpan to a JSONable dict (best-effort, stable subset)
    ctx = span.get_span_context()
    attrs = {}
    try:
        if span.attributes:
            for k, v in span.attributes.items():
                try:
                    json.dumps(v)
                    attrs[str(k)] = v
                except Exception:
                    attrs[str(k)] = str(v)
    except Exception:
        pass

    events = []
    try:
        for ev in span.events or []:
            events.append({
                "name": getattr(ev, "name", "event"),
                "timestamp": getattr(ev, "timestamp", 0),
                "attributes": getattr(ev, "attributes", {}) or {}
            })
    except Exception:
        pass

    def _hex(id_int: int, width: int) -> str:
        try:
            return format(id_int, f"0{width}x")
        except Exception:
            return ""

    return {
        "trace_id": _hex(getattr(ctx, "trace_id", 0), 32),
        "span_id": _hex(getattr(ctx, "span_id", 0), 16),
        "parent_span_id": _hex(getattr(span.parent, "span_id", 0), 16) if getattr(span, "parent", None) else "",
        "name": getattr(span, "name", ""),
        "start_time_unix_nano": getattr(span, "start_time", 0),
        "end_time_unix_nano": getattr(span, "end_time", 0),
        "status": getattr(getattr(span, "status", None), "status_code", "UNSET"),
        "kind": getattr(span, "kind", "INTERNAL"),
        "attributes": attrs,
        "events": events,
        "resource": getattr(getattr(span, "resource", None), "attributes", {}) or {},
        "instrumentation": {
            "name": "pyeztrace",
            "version": "0.0.7",
        },
    }


def enable_from_env() -> bool:
    """Enable OpenTelemetry if EZTRACE_OTEL_ENABLED is true. Idempotent."""
    _state.enabled = _env_bool("EZTRACE_OTEL_ENABLED", False)
    if not _state.enabled or _state.initialized:
        return _state.enabled

    exporter_name = os.environ.get("EZTRACE_OTEL_EXPORTER", "")
    service_name = os.environ.get("EZTRACE_SERVICE_NAME") or (Setup.get_project() if Setup.is_setup_done() else "PyEzTrace")

    try:
        # Import OTEL lazily here
        from opentelemetry import trace as ot_trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource.create({
            "service.name": service_name,
            "library.name": "pyeztrace",
            "library.version": "0.0.7",
        })

        provider = TracerProvider(resource=resource)

        exporter, err = _build_exporter(exporter_name or "otlp")
        if exporter is None:
            # If exporter fails, fallback to console if possible; otherwise disable
            exporter, err2 = _build_exporter("console")
            if exporter is None:
                _state.error = err or err2
                _state.enabled = False
                return False

        processor = BatchSpanProcessor(exporter)
        provider.add_span_processor(processor)
        ot_trace.set_tracer_provider(provider)

        _state.tracer_provider = provider
        _state.span_processor = processor
        _state.exporter = exporter
        _state.tracer = ot_trace.get_tracer("pyeztrace")
        _state.initialized = True
        return True
    except Exception as e:
        _state.error = str(e)
        _state.enabled = False
        _state.initialized = False
        return False


def is_enabled() -> bool:
    if not _state.initialized:
        enable_from_env()
    return _state.enabled and _state.tracer is not None


def get_tracer():
    if not _state.initialized:
        enable_from_env()
    return _state.tracer


@contextmanager
def start_span(name: str, attributes: Optional[Dict[str, Any]] = None):
    """
    Context manager that starts an OTEL span if enabled, else no-op.
    Safe to use in sync or async functions (regular 'with' works in async).
    """
    if is_enabled():
        tracer = get_tracer()
        try:
            if attributes is None:
                attributes = {}
            # Always add function-friendly attribute names
            with tracer.start_as_current_span(name, attributes=attributes) as span:
                yield span
        except Exception:
            # If anything goes wrong, degrade gracefully to no-op
            yield None
    else:
        yield None


def record_exception(span, exc: BaseException):
    try:
        if span is None:
            return
        # Record exception and mark status as error
        from opentelemetry.trace.status import Status, StatusCode
        try:
            span.record_exception(exc)
        except Exception:
            pass
        try:
            span.set_status(Status(StatusCode.ERROR))
        except Exception:
            pass
    except Exception:
        pass


# -----------------
# Custom Exporters
# -----------------

class _BaseJsonBatchExporter:
    """Utility for exporting batches of spans as JSON-Lines, optionally gzipped."""
    def __init__(self):
        self.compress = _env_bool("EZTRACE_COMPRESS", True)

    def _serialize(self, spans: Iterable[Any]) -> bytes:
        lines = []
        for sp in spans:
            try:
                data = _span_to_dict(sp)
                lines.append(json.dumps(data, separators=(",", ":")))
            except Exception:
                # Best-effort: fallback minimal representation
                try:
                    lines.append(json.dumps({"name": getattr(sp, "name", "span")}))
                except Exception:
                    pass
        payload = ("\n".join(lines)).encode("utf-8")
        if self.compress:
            return gzip.compress(payload)
        return payload

    def _object_name(self, prefix: str) -> str:
        ts = time.strftime("%Y/%m/%d/%H/%M/%S", time.gmtime())
        rid = uuid.uuid4().hex
        suffix = ".jsonl.gz" if self.compress else ".jsonl"
        return f"{prefix.rstrip('/')}/{ts}-{rid}{suffix}"


class _S3SpanExporter(_BaseJsonBatchExporter):
    def __init__(self):
        super().__init__()
        import boto3
        self.bucket = os.environ.get("EZTRACE_S3_BUCKET")
        if not self.bucket:
            raise ValueError("EZTRACE_S3_BUCKET is required for S3 exporter")
        self.prefix = os.environ.get("EZTRACE_S3_PREFIX", "traces/")
        region = os.environ.get("EZTRACE_S3_REGION")
        session = boto3.session.Session(region_name=region) if region else boto3.session.Session()
        self.client = session.client("s3")

    def export(self, spans: Iterable[Any]):
        body = self._serialize(spans)
        key = self._object_name(self.prefix)
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/json")
        return self._result_success()

    def shutdown(self):
        return True

    # OpenTelemetry SpanExporter API compatibility shim
    def __call__(self, *args, **kwargs):  # pragma: no cover
        return self

    def _result_success(self):
        try:
            from opentelemetry.sdk.trace.export import SpanExportResult
            return SpanExportResult.SUCCESS
        except Exception:
            return 0


class _AzureBlobSpanExporter(_BaseJsonBatchExporter):
    def __init__(self):
        super().__init__()
        from azure.storage.blob import BlobServiceClient
        container = os.environ.get("EZTRACE_AZURE_CONTAINER")
        if not container:
            raise ValueError("EZTRACE_AZURE_CONTAINER is required for Azure exporter")
        self.prefix = os.environ.get("EZTRACE_AZURE_PREFIX", "traces/")
        connection_string = os.environ.get("EZTRACE_AZURE_CONNECTION_STRING")
        account_url = os.environ.get("EZTRACE_AZURE_ACCOUNT_URL")
        if connection_string:
            service_client = BlobServiceClient.from_connection_string(connection_string)
        elif account_url:
            # Credential via env default credentials chain or SAS token
            service_client = BlobServiceClient(account_url=account_url)
        else:
            raise ValueError("Provide EZTRACE_AZURE_CONNECTION_STRING or EZTRACE_AZURE_ACCOUNT_URL")
        self.container_client = service_client.get_container_client(container)
        try:
            self.container_client.create_container()
        except Exception:
            pass

    def export(self, spans: Iterable[Any]):
        body = self._serialize(spans)
        blob_name = self._object_name(self.prefix)
        self.container_client.upload_blob(name=blob_name, data=body, overwrite=False, content_type="application/json")
        return self._result_success()

    def shutdown(self):
        return True

    def _result_success(self):
        try:
            from opentelemetry.sdk.trace.export import SpanExportResult
            return SpanExportResult.SUCCESS
        except Exception:
            return 0

