import io
import json
import queue
import sys
import threading
import types
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from pyeztrace import otel
from pyeztrace.setup import Setup
from opentelemetry import trace as ot_trace


@pytest.fixture(autouse=True)
def reset_otel_state(monkeypatch):
    keys = [
        "EZTRACE_OTEL_ENABLED",
        "EZTRACE_OTEL_EXPORTER",
        "EZTRACE_SERVICE_NAME",
        "EZTRACE_OTLP_ENDPOINT",
        "EZTRACE_OTLP_HEADERS",
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
