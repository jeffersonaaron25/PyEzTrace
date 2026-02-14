import os
import json
import time
import gzip
import uuid
import sys
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Optional, Set
from urllib.parse import urlparse

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


_GCP_TELEMETRY_ENDPOINT = "https://telemetry.googleapis.com/v1/traces"
_GCP_DEFAULT_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
_GCP_EXPORTER_NAMES = {"gcp", "google", "googlecloud", "google-cloud"}
_GCP_PROJECT_ENV_KEYS = (
    "EZTRACE_GCP_PROJECT_ID",
    "GOOGLE_CLOUD_PROJECT",
    "GCLOUD_PROJECT",
    "GCP_PROJECT",
)
_DIAGNOSTIC_ONCE_KEYS: Set[str] = set()


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.environ.get(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _otel_debug_enabled() -> bool:
    return _env_bool("EZTRACE_OTEL_DEBUG", False)


def _emit_diagnostic(
    message: str,
    *,
    level: str = "WARN",
    once_key: Optional[str] = None,
    debug_only: bool = False,
) -> None:
    if debug_only and not _otel_debug_enabled():
        return
    if once_key and once_key in _DIAGNOSTIC_ONCE_KEYS:
        return
    if once_key:
        _DIAGNOSTIC_ONCE_KEYS.add(once_key)
    try:
        sys.__stderr__.write(f"[PyEzTrace OTEL {level}] {message}\n")
        sys.__stderr__.flush()
    except Exception:
        pass


def _span_export_result_failure():
    try:
        from opentelemetry.sdk.trace.export import SpanExportResult
        return SpanExportResult.FAILURE
    except Exception:
        return 1


class _DiagnosticSpanExporter:
    """Wrap an exporter and surface runtime export failures."""

    def __init__(self, inner):
        self._inner = inner

    def export(self, spans: Iterable[Any]):
        try:
            result = self._inner.export(spans)
        except Exception as e:
            _state.error = f"Span export failed: {e}"
            _emit_diagnostic(
                f"Span export failed at runtime: {e}",
                level="ERROR",
                once_key=f"export-exc:{type(e).__name__}:{e}",
            )
            return _span_export_result_failure()

        try:
            from opentelemetry.sdk.trace.export import SpanExportResult
            if result != SpanExportResult.SUCCESS:
                _emit_diagnostic(
                    f"Span exporter returned non-success result: {result}",
                    once_key=f"export-non-success:{result}",
                )
        except Exception:
            pass
        return result

    def shutdown(self):
        fn = getattr(self._inner, "shutdown", None)
        if callable(fn):
            return fn()
        return True

    def force_flush(self, *args, **kwargs):
        fn = getattr(self._inner, "force_flush", None)
        if callable(fn):
            return fn(*args, **kwargs)
        return True

    def __getattr__(self, item):
        return getattr(self._inner, item)


class _RefreshingGoogleBearerSpanExporter:
    """
    Wraps an OTLP exporter and refreshes GCP bearer auth before each export.
    Used for OTLP exporter versions that do not support a custom requests session.
    """

    def __init__(self, inner, credentials):
        self._inner = inner
        self._credentials = credentials

    def _set_authorization_header(self, token: str) -> None:
        authorization = f"Bearer {token}"

        headers = getattr(self._inner, "_headers", None)
        if isinstance(headers, dict):
            headers["Authorization"] = authorization

        session = getattr(self._inner, "_session", None)
        session_headers = getattr(session, "headers", None)
        if isinstance(session_headers, dict):
            session_headers["Authorization"] = authorization

    def export(self, spans: Iterable[Any]):
        token, err = _refresh_google_access_token(self._credentials)
        if token is None:
            _state.error = f"Google bearer token refresh failed: {err}"
            _emit_diagnostic(
                f"Google bearer token refresh failed: {err}",
                level="ERROR",
                once_key=f"gcp-token-refresh-failed:{err}",
            )
            return _span_export_result_failure()

        self._set_authorization_header(token)
        return self._inner.export(spans)

    def shutdown(self):
        fn = getattr(self._inner, "shutdown", None)
        if callable(fn):
            return fn()
        return True

    def force_flush(self, *args, **kwargs):
        fn = getattr(self._inner, "force_flush", None)
        if callable(fn):
            return fn(*args, **kwargs)
        return True

    def __getattr__(self, item):
        return getattr(self._inner, item)


def _is_google_telemetry_endpoint(endpoint: str) -> bool:
    if not endpoint:
        return False
    try:
        host = (urlparse(endpoint).netloc or "").lower()
        if not host:
            host = endpoint.lower()
        return "telemetry.googleapis.com" in host
    except Exception:
        return False


def _has_authorization_header(headers: Dict[str, str]) -> bool:
    for k in headers.keys():
        if str(k).strip().lower() == "authorization":
            return True
    return False


def _parse_scopes(raw_scopes: str):
    if not raw_scopes:
        return [_GCP_DEFAULT_SCOPE]
    cleaned = raw_scopes.replace(",", " ")
    scopes = [s.strip() for s in cleaned.split() if s.strip()]
    return scopes or [_GCP_DEFAULT_SCOPE]


def _should_use_gcp_auth(endpoint: str, exporter_name: str) -> bool:
    explicit = os.environ.get("EZTRACE_OTLP_GCP_AUTH")
    if explicit is not None:
        return explicit.strip().lower() in ("1", "true", "yes", "y", "on")

    name = (exporter_name or "").strip().lower()
    if name in _GCP_EXPORTER_NAMES:
        return True

    return _is_google_telemetry_endpoint(endpoint)


def _resolve_otlp_endpoint(exporter_name: str) -> str:
    name = (exporter_name or "").strip().lower()
    default_endpoint = _GCP_TELEMETRY_ENDPOINT if name in _GCP_EXPORTER_NAMES else "http://localhost:4318/v1/traces"
    return os.environ.get("EZTRACE_OTLP_ENDPOINT", default_endpoint)


def _resolve_gcp_project_id() -> Optional[str]:
    for key in _GCP_PROJECT_ENV_KEYS:
        value = os.environ.get(key)
        if value and value.strip():
            return value.strip()

    try:
        import google.auth
        _credentials, project_id = google.auth.default(scopes=_parse_scopes(os.environ.get("EZTRACE_GCP_SCOPES", "")))
        if project_id:
            return str(project_id)
    except Exception:
        pass
    return None


def _load_google_credentials():
    try:
        import google.auth
    except Exception as e:
        return None, f"GCP auth requires google-auth: {e}"

    scopes = _parse_scopes(os.environ.get("EZTRACE_GCP_SCOPES", ""))
    try:
        credentials, _project = google.auth.default(scopes=scopes)
        if credentials is None:
            return None, "Unable to load Google credentials from ADC."
        return credentials, None
    except Exception as e:
        return None, f"Unable to load Google credentials from ADC: {e}"


def _build_google_authorized_session(credentials):
    try:
        from google.auth.transport.requests import AuthorizedSession
        return AuthorizedSession(credentials), None
    except Exception as e:
        return None, f"Unable to create Google AuthorizedSession: {e}"


def _refresh_google_access_token(credentials):
    try:
        from google.auth.transport.requests import Request
    except Exception as e:
        return None, f"Unable to create Google auth request transport: {e}"

    try:
        credentials.refresh(Request())
        token = getattr(credentials, "token", None)
        if not token:
            return None, "Google credentials did not provide an access token."
        return token, None
    except Exception as e:
        return None, f"Unable to refresh Google credentials: {e}"


def _build_otlp_http_exporter(endpoint: str, headers: Dict[str, str], exporter_name: str):
    try:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    except Exception as e:
        return None, f"OTLP HTTP exporter requires opentelemetry-exporter-otlp: {e}"

    resolved_headers = dict(headers or {})
    use_gcp_auth = _should_use_gcp_auth(endpoint, exporter_name) and not _has_authorization_header(resolved_headers)

    if not use_gcp_auth:
        try:
            return OTLPSpanExporter(endpoint=endpoint, headers=resolved_headers or None), None
        except Exception as e:
            return None, f"Error creating OTLP HTTP exporter: {e}"

    credentials, cred_err = _load_google_credentials()
    if credentials is None:
        return None, cred_err

    session, session_err = _build_google_authorized_session(credentials)
    if session is not None:
        try:
            return OTLPSpanExporter(endpoint=endpoint, headers=resolved_headers or None, session=session), None
        except TypeError as e:
            # Older OTLP exporter versions do not accept "session"; fallback to bearer token header.
            if "session" not in str(e):
                return None, f"Error creating OTLP HTTP exporter with Google session auth: {e}"
        except Exception as e:
            return None, f"Error creating OTLP HTTP exporter with Google session auth: {e}"
    else:
        session_err = session_err or "Unknown error creating Google AuthorizedSession."

    token, token_err = _refresh_google_access_token(credentials)
    if token is None:
        return None, f"{session_err} Fallback bearer token setup failed: {token_err}"

    resolved_headers["Authorization"] = f"Bearer {token}"
    try:
        exporter = OTLPSpanExporter(endpoint=endpoint, headers=resolved_headers or None)
        return _RefreshingGoogleBearerSpanExporter(exporter, credentials), None
    except Exception as e:
        return None, f"Error creating OTLP HTTP exporter with Google bearer auth: {e}"


def _build_exporter(exporter_name: str):
    """
    Build an exporter instance based on name. Import heavy libs only on demand.
    Supported: 'console', 'otlp', 'gcp', 's3', 'azure'.
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
    if name in ("otlp", "otlphttp", "otlp-http", "gcp", "google", "googlecloud", "google-cloud"):
        endpoint = _resolve_otlp_endpoint(name)
        headers = _parse_headers(os.environ.get("EZTRACE_OTLP_HEADERS", ""))
        return _build_otlp_http_exporter(endpoint=endpoint, headers=headers, exporter_name=name)

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
            "version": "0.1.1",
        },
    }


def enable_from_env() -> bool:
    """Enable OpenTelemetry if EZTRACE_OTEL_ENABLED is true. Idempotent."""
    _state.enabled = _env_bool("EZTRACE_OTEL_ENABLED", False)
    _state.error = None
    if not _state.enabled:
        _emit_diagnostic(
            "OTEL bridge is disabled (EZTRACE_OTEL_ENABLED is false). Spans will be no-op.",
            level="DEBUG",
            once_key="otel-disabled",
            debug_only=True,
        )
        return False
    if not _state.enabled or _state.initialized:
        return _state.enabled

    exporter_name = os.environ.get("EZTRACE_OTEL_EXPORTER", "")
    resolved_exporter_name = exporter_name or "otlp"
    otlp_endpoint = _resolve_otlp_endpoint(resolved_exporter_name)
    service_name = os.environ.get("EZTRACE_SERVICE_NAME") or (Setup.get_project() if Setup.is_setup_done() else "PyEzTrace")

    try:
        # Import OTEL lazily here
        from opentelemetry import trace as ot_trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource_attrs: Dict[str, Any] = {
            "service.name": service_name,
            "library.name": "pyeztrace",
            "library.version": "0.1.1",
        }

        if _should_use_gcp_auth(otlp_endpoint, resolved_exporter_name):
            project_id = _resolve_gcp_project_id()
            if project_id:
                resource_attrs["gcp.project_id"] = project_id
            else:
                _emit_diagnostic(
                    "Unable to resolve GCP project id for OTLP. "
                    "Cloud Trace may reject spans without resource attribute 'gcp.project_id'.",
                    once_key="missing-gcp-project-id",
                )

        resource = Resource.create(resource_attrs)

        provider = TracerProvider(resource=resource)

        exporter, err = _build_exporter(resolved_exporter_name)
        if exporter is None:
            # If exporter fails, fallback to console if possible; otherwise disable
            _emit_diagnostic(
                f"Failed to initialize exporter '{resolved_exporter_name}': {err}. Falling back to console exporter.",
                once_key=f"exporter-fallback:{resolved_exporter_name}:{err}",
            )
            exporter, err2 = _build_exporter("console")
            if exporter is None:
                _state.error = err or err2
                _emit_diagnostic(
                    f"OTEL disabled because no exporter could be initialized: {_state.error}",
                    level="ERROR",
                    once_key=f"exporter-fatal:{_state.error}",
                )
                _state.enabled = False
                return False

        processor = BatchSpanProcessor(_DiagnosticSpanExporter(exporter))
        provider.add_span_processor(processor)
        ot_trace.set_tracer_provider(provider)

        _state.tracer_provider = provider
        _state.span_processor = processor
        _state.exporter = exporter
        _state.tracer = ot_trace.get_tracer("pyeztrace")
        _state.initialized = True
        _emit_diagnostic(
            f"OTEL enabled. service.name={service_name} exporter={type(exporter).__name__}",
            level="DEBUG",
            debug_only=True,
        )
        return True
    except Exception as e:
        _state.error = str(e)
        _emit_diagnostic(
            f"OTEL initialization failed: {e}",
            level="ERROR",
            once_key=f"otel-init-error:{type(e).__name__}:{e}",
        )
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


def get_otel_status() -> Dict[str, Any]:
    exporter = _state.exporter
    return {
        "enabled": bool(_state.enabled),
        "initialized": bool(_state.initialized),
        "error": _state.error,
        "exporter": type(exporter).__name__ if exporter is not None else None,
        "otel_enabled_env": os.environ.get("EZTRACE_OTEL_ENABLED"),
        "otel_exporter_env": os.environ.get("EZTRACE_OTEL_EXPORTER"),
        "otlp_endpoint_env": os.environ.get("EZTRACE_OTLP_ENDPOINT"),
    }


@contextmanager
def start_span(name: str, attributes: Optional[Dict[str, Any]] = None):
    """
    Context manager that starts an OTEL span if enabled, else no-op.
    Safe to use in sync or async functions (regular 'with' works in async).
    """
    if is_enabled():
        tracer = get_tracer()
        if attributes is None:
            attributes = {}

        try:
            span_cm = tracer.start_as_current_span(name, attributes=attributes)
        except Exception as e:
            _emit_diagnostic(
                f"start_span('{name}') degraded to no-op: unable to create span context manager ({e}).",
                once_key=f"start-span-create-failed:{type(e).__name__}:{e}",
            )
            yield None
            return

        try:
            span = span_cm.__enter__()
        except Exception as e:
            _emit_diagnostic(
                f"start_span('{name}') degraded to no-op: unable to enter span context ({e}).",
                once_key=f"start-span-enter-failed:{type(e).__name__}:{e}",
            )
            yield None
            return

        try:
            yield span
        except Exception:
            exc_type, exc_value, exc_tb = sys.exc_info()
            try:
                span_cm.__exit__(exc_type, exc_value, exc_tb)
            except Exception as e:
                _emit_diagnostic(
                    f"Error while closing span for '{name}' after exception: {e}",
                    level="ERROR",
                    once_key=f"start-span-exit-after-error:{type(e).__name__}:{e}",
                )
            raise
        else:
            try:
                span_cm.__exit__(None, None, None)
            except Exception as e:
                _emit_diagnostic(
                    f"Error while closing span for '{name}': {e}",
                    level="ERROR",
                    once_key=f"start-span-exit:{type(e).__name__}:{e}",
                )
    else:
        if _state.error:
            _emit_diagnostic(
                f"start_span('{name}') is no-op because OTEL failed to initialize: {_state.error}",
                once_key=f"start-span-init-error:{_state.error}",
            )
        else:
            _emit_diagnostic(
                f"start_span('{name}') is no-op because OTEL is disabled.",
                once_key=f"start-span-disabled:{name}",
                debug_only=True,
            )
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


def _reset_diagnostics_state_for_tests():
    _DIAGNOSTIC_ONCE_KEYS.clear()


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
