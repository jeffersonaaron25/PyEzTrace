import io
import json
import queue
import sys
import threading
import types
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

pytest.importorskip("opentelemetry", reason="OpenTelemetry bridge tests require optional dependencies")

from opentelemetry import trace as ot_trace

from pyeztrace import otel
from pyeztrace.setup import Setup


@pytest.fixture(autouse=True)
def reset_otel_state(monkeypatch):
    keys = [
        "EZTRACE_OTEL_ENABLED",
        "EZTRACE_OTEL_EXPORTER",
        "EZTRACE_OTEL_DEBUG",
        "EZTRACE_SERVICE_NAME",
        "EZTRACE_OTLP_ENDPOINT",
        "EZTRACE_OTLP_HEADERS",
        "EZTRACE_OTLP_GCP_AUTH",
        "EZTRACE_GCP_PROJECT_ID",
        "EZTRACE_GCP_SCOPES",
        "GOOGLE_CLOUD_PROJECT",
        "GCLOUD_PROJECT",
        "GCP_PROJECT",
        "EZTRACE_S3_BUCKET",
        "EZTRACE_S3_PREFIX",
        "EZTRACE_S3_REGION",
        "EZTRACE_COMPRESS",
        "EZTRACE_AZURE_CONTAINER",
        "EZTRACE_AZURE_PREFIX",
        "EZTRACE_AZURE_CONNECTION_STRING",
        "EZTRACE_AZURE_ACCOUNT_URL",
    ]
    Setup.reset()
    otel._state = otel._OtelState()
    if hasattr(otel, "_reset_diagnostics_state_for_tests"):
        otel._reset_diagnostics_state_for_tests()
    _reset_tracer_provider()
    for key in keys:
        monkeypatch.delenv(key, raising=False)
    yield
    try:
        if otel._state.span_processor:
            otel._state.span_processor.shutdown()
    except Exception:
        pass
    try:
        if otel._state.tracer_provider:
            otel._state.tracer_provider.shutdown()
    except Exception:
        pass
    Setup.reset()
    otel._state = otel._OtelState()
    if hasattr(otel, "_reset_diagnostics_state_for_tests"):
        otel._reset_diagnostics_state_for_tests()
    _reset_tracer_provider()


def _reset_tracer_provider():
    ot_trace._TRACER_PROVIDER_SET_ONCE = ot_trace.Once()
    ot_trace._TRACER_PROVIDER = None


def capture_span(name: str):
    with otel.start_span(name, {"test": "value"}):
        pass


def test_console_exporter_emits_span(monkeypatch):
    Setup.initialize("CONSOLE_APP", show_metrics=False)
    monkeypatch.setenv("EZTRACE_OTEL_ENABLED", "true")
    monkeypatch.setenv("EZTRACE_OTEL_EXPORTER", "console")
    buffer = io.StringIO()
    monkeypatch.setattr(sys, "stdout", buffer)

    enabled = otel.enable_from_env()
    assert enabled is True
    assert otel.is_enabled() is True

    capture_span("test_console_span")
    assert otel._state.span_processor.force_flush(timeout_millis=5000)

    output = buffer.getvalue()
    assert "test_console_span" in output
    assert "test" in output


def test_otlp_exporter_sends_data_to_local_collector(monkeypatch):
    received = queue.Queue()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("content-length", 0))
            body = self.rfile.read(length)
            received.put((self.path, body, dict(self.headers)))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *_args, **_kwargs):  # pragma: no cover - suppress noise
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        Setup.initialize("OTLP_APP", show_metrics=False)
        endpoint = f"http://127.0.0.1:{server.server_address[1]}/v1/traces"
        monkeypatch.setenv("EZTRACE_OTEL_ENABLED", "true")
        monkeypatch.setenv("EZTRACE_OTEL_EXPORTER", "otlp")
        monkeypatch.setenv("EZTRACE_OTLP_ENDPOINT", endpoint)

        assert otel.enable_from_env() is True
        capture_span("test_otlp_span")
        assert otel._state.span_processor.force_flush(timeout_millis=5000)

        path, body, headers = received.get(timeout=5)
        assert path == "/v1/traces"
        assert len(body) > 0
        header_key = next((k for k in headers if k.lower() == "content-type"), None)
        assert header_key is not None
        assert headers[header_key].startswith("application/x-protobuf")
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_otlp_exporter_uses_google_authorized_session(monkeypatch):
    captured = {}

    class FakeOTLPSpanExporter:
        def __init__(self, endpoint=None, headers=None, session=None):
            captured["endpoint"] = endpoint
            captured["headers"] = headers or {}
            captured["session"] = session

    class FakeCreds:
        token = None

        def refresh(self, _request):
            self.token = "should-not-be-needed"

    class FakeAuthorizedSession:
        def __init__(self, credentials):
            self.credentials = credentials

    class FakeRequest:
        pass

    trace_exporter_module = types.ModuleType("opentelemetry.exporter.otlp.proto.http.trace_exporter")
    trace_exporter_module.OTLPSpanExporter = FakeOTLPSpanExporter

    google_module = types.ModuleType("google")
    google_auth_module = types.ModuleType("google.auth")
    google_transport_module = types.ModuleType("google.auth.transport")
    google_requests_module = types.ModuleType("google.auth.transport.requests")

    google_auth_module.default = lambda scopes=None: (FakeCreds(), "unit-project")
    google_requests_module.AuthorizedSession = FakeAuthorizedSession
    google_requests_module.Request = FakeRequest
    google_auth_module.transport = google_transport_module
    google_transport_module.requests = google_requests_module
    google_module.auth = google_auth_module

    monkeypatch.setitem(sys.modules, "opentelemetry.exporter.otlp.proto.http.trace_exporter", trace_exporter_module)
    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.auth", google_auth_module)
    monkeypatch.setitem(sys.modules, "google.auth.transport", google_transport_module)
    monkeypatch.setitem(sys.modules, "google.auth.transport.requests", google_requests_module)

    monkeypatch.setenv("EZTRACE_OTLP_ENDPOINT", "https://telemetry.googleapis.com/v1/traces")
    monkeypatch.setenv("EZTRACE_OTLP_HEADERS", "x-tenant=abc")

    exporter, err = otel._build_exporter("otlp")
    assert err is None
    assert isinstance(exporter, FakeOTLPSpanExporter)
    assert captured["endpoint"] == "https://telemetry.googleapis.com/v1/traces"
    assert captured["headers"]["x-tenant"] == "abc"
    assert isinstance(captured["session"], FakeAuthorizedSession)


def test_otlp_exporter_google_auth_falls_back_to_bearer_header(monkeypatch):
    captured = {}

    class FakeOTLPSpanExporter:
        def __init__(self, endpoint=None, headers=None):
            captured["endpoint"] = endpoint
            captured["headers"] = headers or {}
            self._headers = dict(headers or {})

        def export(self, _spans):
            return True

    class FakeCreds:
        token = None

        def refresh(self, _request):
            self.token = "unit-token"

    class FakeAuthorizedSession:
        def __init__(self, credentials):
            self.credentials = credentials

    class FakeRequest:
        pass

    trace_exporter_module = types.ModuleType("opentelemetry.exporter.otlp.proto.http.trace_exporter")
    trace_exporter_module.OTLPSpanExporter = FakeOTLPSpanExporter

    google_module = types.ModuleType("google")
    google_auth_module = types.ModuleType("google.auth")
    google_transport_module = types.ModuleType("google.auth.transport")
    google_requests_module = types.ModuleType("google.auth.transport.requests")

    google_auth_module.default = lambda scopes=None: (FakeCreds(), "unit-project")
    google_requests_module.AuthorizedSession = FakeAuthorizedSession
    google_requests_module.Request = FakeRequest
    google_auth_module.transport = google_transport_module
    google_transport_module.requests = google_requests_module
    google_module.auth = google_auth_module

    monkeypatch.setitem(sys.modules, "opentelemetry.exporter.otlp.proto.http.trace_exporter", trace_exporter_module)
    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.auth", google_auth_module)
    monkeypatch.setitem(sys.modules, "google.auth.transport", google_transport_module)
    monkeypatch.setitem(sys.modules, "google.auth.transport.requests", google_requests_module)

    monkeypatch.delenv("EZTRACE_OTLP_ENDPOINT", raising=False)
    monkeypatch.setenv("EZTRACE_OTLP_HEADERS", "x-tenant=abc")

    exporter, err = otel._build_exporter("gcp")
    assert err is None
    assert isinstance(exporter, otel._RefreshingGoogleBearerSpanExporter)
    assert isinstance(exporter._inner, FakeOTLPSpanExporter)
    assert captured["endpoint"] == "https://telemetry.googleapis.com/v1/traces"
    assert captured["headers"]["x-tenant"] == "abc"
    assert captured["headers"]["Authorization"] == "Bearer unit-token"


def test_otlp_exporter_google_bearer_fallback_refreshes_token_per_export(monkeypatch):
    export_headers = []
    created = {}

    class FakeOTLPSpanExporter:
        def __init__(self, endpoint=None, headers=None):
            self._headers = dict(headers or {})
            self._session = types.SimpleNamespace(headers=dict(headers or {}))

        def export(self, _spans):
            export_headers.append(self._headers.get("Authorization"))
            return True

    class FakeCreds:
        def __init__(self):
            self.token = None
            self.refresh_calls = 0

        def refresh(self, _request):
            self.refresh_calls += 1
            self.token = f"unit-token-{self.refresh_calls}"

    class FakeAuthorizedSession:
        def __init__(self, credentials):
            self.credentials = credentials

    class FakeRequest:
        pass

    trace_exporter_module = types.ModuleType("opentelemetry.exporter.otlp.proto.http.trace_exporter")
    trace_exporter_module.OTLPSpanExporter = FakeOTLPSpanExporter

    google_module = types.ModuleType("google")
    google_auth_module = types.ModuleType("google.auth")
    google_transport_module = types.ModuleType("google.auth.transport")
    google_requests_module = types.ModuleType("google.auth.transport.requests")

    def fake_default(scopes=None):  # noqa: ARG001
        creds = FakeCreds()
        created["credentials"] = creds
        return creds, "unit-project"

    google_auth_module.default = fake_default
    google_requests_module.AuthorizedSession = FakeAuthorizedSession
    google_requests_module.Request = FakeRequest
    google_auth_module.transport = google_transport_module
    google_transport_module.requests = google_requests_module
    google_module.auth = google_auth_module

    monkeypatch.setitem(sys.modules, "opentelemetry.exporter.otlp.proto.http.trace_exporter", trace_exporter_module)
    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.auth", google_auth_module)
    monkeypatch.setitem(sys.modules, "google.auth.transport", google_transport_module)
    monkeypatch.setitem(sys.modules, "google.auth.transport.requests", google_requests_module)

    exporter, err = otel._build_exporter("gcp")
    assert err is None
    assert isinstance(exporter, otel._RefreshingGoogleBearerSpanExporter)

    exporter.export([])
    exporter.export([])

    assert export_headers == ["Bearer unit-token-2", "Bearer unit-token-3"]
    assert created["credentials"].refresh_calls == 3


def test_otel_init_failure_surfaces_diagnostic_and_status(monkeypatch):
    Setup.initialize("BROKEN_OTEL_APP", show_metrics=False)
    monkeypatch.setenv("EZTRACE_OTEL_ENABLED", "true")
    monkeypatch.setenv("EZTRACE_OTEL_EXPORTER", "otlp")

    def fake_build_exporter(_name):
        return None, "unit-exporter-failure"

    stderr = io.StringIO()
    monkeypatch.setattr(otel, "_build_exporter", fake_build_exporter)
    monkeypatch.setattr(sys, "__stderr__", stderr)

    assert otel.enable_from_env() is False
    status = otel.get_otel_status()
    assert status["enabled"] is False
    assert status["initialized"] is False
    assert status["error"] == "unit-exporter-failure"
    assert "unit-exporter-failure" in stderr.getvalue()


def test_runtime_export_failure_is_surfaced(monkeypatch):
    class FailingExporter:
        def export(self, _spans):
            raise RuntimeError("network-down")

        def shutdown(self):
            return True

    stderr = io.StringIO()
    monkeypatch.setattr(sys, "__stderr__", stderr)

    wrapped = otel._DiagnosticSpanExporter(FailingExporter())
    result = wrapped.export([])

    from opentelemetry.sdk.trace.export import SpanExportResult
    assert result == SpanExportResult.FAILURE
    assert "network-down" in stderr.getvalue()


def test_gcp_resource_has_project_id(monkeypatch):
    Setup.initialize("GCP_APP", show_metrics=False)
    monkeypatch.setenv("EZTRACE_OTEL_ENABLED", "true")
    monkeypatch.setenv("EZTRACE_OTEL_EXPORTER", "gcp")
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "unit-gcp-project")

    from opentelemetry.sdk.trace.export import ConsoleSpanExporter

    def fake_build_exporter(_name):
        return ConsoleSpanExporter(), None

    monkeypatch.setattr(otel, "_build_exporter", fake_build_exporter)

    assert otel.enable_from_env() is True
    attrs = dict(otel._state.tracer_provider.resource.attributes)
    assert attrs.get("gcp.project_id") == "unit-gcp-project"


def test_start_span_propagates_user_exception_without_contextmanager_error(monkeypatch):
    Setup.initialize("SPAN_ERR_APP", show_metrics=False)
    monkeypatch.setenv("EZTRACE_OTEL_ENABLED", "true")
    monkeypatch.setenv("EZTRACE_OTEL_EXPORTER", "console")

    assert otel.enable_from_env() is True
    with pytest.raises(ValueError):
        with otel.start_span("boom-span"):
            raise ValueError("boom")


def test_s3_exporter_writes_span_batch(monkeypatch):
    calls = []

    class FakeSession:
        def __init__(self, region_name=None):
            self.region_name = region_name

        def client(self, name):
            assert name == "s3"
            return FakeClient()

    class FakeClient:
        def put_object(self, Bucket, Key, Body, ContentType):
            calls.append({
                "Bucket": Bucket,
                "Key": Key,
                "Body": Body,
                "ContentType": ContentType,
            })

    session_module = types.ModuleType("boto3.session")
    session_module.Session = FakeSession
    boto3_module = types.ModuleType("boto3")
    boto3_module.session = session_module

    monkeypatch.setitem(sys.modules, "boto3", boto3_module)
    monkeypatch.setitem(sys.modules, "boto3.session", session_module)

    Setup.initialize("S3_APP", show_metrics=False)
    monkeypatch.setenv("EZTRACE_OTEL_ENABLED", "true")
    monkeypatch.setenv("EZTRACE_OTEL_EXPORTER", "s3")
    monkeypatch.setenv("EZTRACE_S3_BUCKET", "unit-bucket")
    monkeypatch.setenv("EZTRACE_COMPRESS", "false")

    assert otel.enable_from_env() is True
    capture_span("test_s3_span")
    assert otel._state.span_processor.force_flush(timeout_millis=5000)

    assert calls, "Expected S3 exporter to upload payload"
    payload = calls[0]["Body"].decode("utf-8").strip().splitlines()
    records = [json.loads(line) for line in payload if line]
    assert any(record["name"] == "test_s3_span" for record in records)
    assert calls[0]["Bucket"] == "unit-bucket"
    assert calls[0]["ContentType"] == "application/json"


def test_azure_exporter_uploads_span_batch(monkeypatch):
    uploads = []

    class FakeContainerClient:
        def __init__(self, name):
            self.name = name

        def create_container(self):
            pass

        def upload_blob(self, name, data, overwrite=False, content_type=None):
            uploads.append({
                "name": name,
                "data": data,
                "content_type": content_type,
            })

    class FakeBlobServiceClient:
        def __init__(self, account_url=None):
            self.account_url = account_url

        @classmethod
        def from_connection_string(cls, _conn_str):
            return cls(account_url="from-connection")

        def get_container_client(self, container):
            return FakeContainerClient(container)

    azure_module = types.ModuleType("azure")
    storage_module = types.ModuleType("azure.storage")
    blob_module = types.ModuleType("azure.storage.blob")
    blob_module.BlobServiceClient = FakeBlobServiceClient
    azure_module.storage = storage_module
    storage_module.blob = blob_module

    monkeypatch.setitem(sys.modules, "azure", azure_module)
    monkeypatch.setitem(sys.modules, "azure.storage", storage_module)
    monkeypatch.setitem(sys.modules, "azure.storage.blob", blob_module)

    Setup.initialize("AZURE_APP", show_metrics=False)
    monkeypatch.setenv("EZTRACE_OTEL_ENABLED", "true")
    monkeypatch.setenv("EZTRACE_OTEL_EXPORTER", "azure")
    monkeypatch.setenv("EZTRACE_AZURE_CONTAINER", "unit-container")
    monkeypatch.setenv("EZTRACE_AZURE_CONNECTION_STRING", "UseDevelopmentStorage=true")
    monkeypatch.setenv("EZTRACE_COMPRESS", "false")

    assert otel.enable_from_env() is True
    capture_span("test_azure_span")
    assert otel._state.span_processor.force_flush(timeout_millis=5000)

    assert uploads, "Azure exporter should upload payload"
    content = uploads[0]["data"].decode("utf-8").strip().splitlines()
    records = [json.loads(line) for line in content if line]
    assert any(record["name"] == "test_azure_span" for record in records)
    assert uploads[0]["content_type"] == "application/json"
