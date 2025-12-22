import warnings
import pytest
from pyeztrace import setup, tracer


@pytest.fixture(autouse=True)
def reset_setup():
    setup.Setup.reset()


def test_trace_decorator_basic(monkeypatch):
    setup.Setup.initialize("EZTRACER_TEST", show_metrics=True)
    calls = []

    @tracer.trace()
    def foo(x):
        calls.append(x)
        return x * 2

    result = foo(3)
    assert result == 6
    assert calls == [3]


def test_trace_decorator_include_exclude(monkeypatch):
    setup.Setup.initialize("EZTRACER_TEST2", show_metrics=False)
    called = []

    def bar():
        called.append("bar")

    def baz():
        called.append("baz")

    import types
    mod = types.ModuleType("mod")
    mod.bar = bar
    mod.baz = baz

    @tracer.trace(include=["bar"], modules_or_classes=[mod])
    def parent():
        mod.bar()
        mod.baz()

    parent()
    assert "bar" in called
    assert "baz" in called  # baz is not traced, but still called


def test_child_trace_decorator_tracing(monkeypatch):
    setup.Setup.initialize("EZTRACER_TEST3", show_metrics=False)

    @tracer.child_trace_decorator
    def foo():
        return 42

    assert foo() == 42


def test_trace_async(monkeypatch):
    import asyncio

    setup.Setup.initialize("EZTRACER_TEST4", show_metrics=False)

    @tracer.trace()
    async def foo():
        await asyncio.sleep(0.01)
        return "ok"

    result = asyncio.run(foo())
    assert result == "ok"


def test_trace_class_preserves_class_attributes(monkeypatch):
    setup.Setup.initialize("EZTRACER_TEST_CLASS_ATTR", show_metrics=False)

    class Dependency:
        pass

    @tracer.trace()
    class Example:
        helper_type = Dependency

        def __init__(self):
            self.helper = self.helper_type()

    instance = Example()
    assert isinstance(instance.helper, Dependency)
    assert Example.helper_type is Dependency


def test_trace_class_preserves_descriptors(monkeypatch):
    setup.Setup.initialize("EZTRACER_TEST_CLASS_DESCRIPTOR", show_metrics=False)

    calls = []

    @tracer.trace()
    class Example:
        @staticmethod
        def identity(value):
            return value

        @classmethod
        def build(cls, value):
            calls.append(cls)
            return cls.identity(value)

    assert Example.identity(42) == 42
    assert Example.build("ok") == "ok"
    assert calls[-1] is Example
    # Ensure the descriptor types remain intact
    assert isinstance(Example.__dict__["identity"], staticmethod)
    assert isinstance(Example.__dict__["build"], classmethod)


def test_safe_preview_value_redacts_nested_structures():
    redaction = tracer._build_redaction_settings(["password", "token"])
    value = {
        "username": "alice",
        "password": "hunter2",
        "nested": {"token": "abc", "keep": "ok"},
        "items": [
            {"token": "list-secret", "visible": True},
            "safe",
        ],
    }

    preview = tracer._safe_preview_value(value, redaction=redaction)

    assert preview["password"] == "<redacted>"
    assert preview["nested"]["token"] == "<redacted>"
    assert preview["nested"]["keep"] == "ok"
    assert preview["items"][0]["token"] == "<redacted>"
    assert preview["items"][0]["visible"] is True


def test_safe_preview_value_redacts_by_value_pattern():
    redaction = tracer._build_redaction_settings(redact_value_patterns=[r"secret\d+"])
    value = ["public", "secret123", {"other": "secret999"}]

    preview = tracer._safe_preview_value(value, redaction=redaction)

    assert preview[0] == "public"
    assert preview[1] == "<redacted>"
    assert preview[2]["other"] == "<redacted>"


def test_safe_preview_value_uses_presets_for_common_patterns():
    redaction = tracer._build_redaction_settings(redact_value_patterns=["pii"])
    value = {"field1": "alice@example.com", "field2": "123-45-6789", "ok": "fine"}

    preview = tracer._safe_preview_value(value, redaction=redaction)

    assert preview["field1"] == "<redacted>"
    assert preview["field2"] == "<redacted>"
    assert preview["ok"] == "fine"


def test_safe_preview_value_redacts_sets_and_frozensets():
    redaction = tracer._build_redaction_settings(redact_value_patterns=[r"secret\d+"])

    preview = tracer._safe_preview_value({"public", "secret123"}, redaction=redaction)
    assert isinstance(preview, list)
    assert "public" in preview
    assert "<redacted>" in preview

    preview = tracer._safe_preview_value(frozenset(["public", "secret123"]), redaction=redaction)
    assert isinstance(preview, list)
    assert "public" in preview
    assert "<redacted>" in preview


def test_build_redaction_settings_warns_on_invalid_regex():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        settings = tracer._build_redaction_settings(redact_pattern="(")
    assert settings is None or settings.pattern is None
    assert any("invalid redaction pattern" in str(w.message) for w in caught)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        settings = tracer._build_redaction_settings(redact_value_patterns=["["])
    assert settings is None or settings.value_patterns is None
    assert any("invalid redaction value pattern" in str(w.message) for w in caught)


def test_safe_preview_value_redacts_set_tail_elements():
    redaction = tracer._build_redaction_settings(redact_value_patterns=[r"secret\d+"])

    class OrderedSet(set):
        def __init__(self, items):
            self._order = list(items)
            super().__init__(items)

        def __iter__(self):
            return iter(self._order)

    data = OrderedSet(["public1", "public2", "public3", "public4", "public5", "secret123"])

    preview = tracer._safe_preview_value(data, redaction=redaction)

    assert preview.count("<redacted>") >= 1
    assert "…" in preview


def test_trace_applies_environment_redaction(monkeypatch):
    monkeypatch.setenv("EZTRACE_REDACT_KEYS", "secret,token")
    setup.Setup.initialize("EZTRACER_ENV_REDACTION", show_metrics=False)

    logs = []

    def capture_log_info(message, **kwargs):
        logs.append({"message": message, **kwargs})

    monkeypatch.setattr(tracer.logging, "log_info", capture_log_info)
    monkeypatch.setattr(tracer.logging, "record_metric", lambda *args, **kwargs: None)

    @tracer.trace()
    def foo(secret, token, visible):
        return {"secret": secret, "token": token, "visible": visible}

    foo(secret="s", token="t", visible="ok")

    start_log = next(log for log in logs if "kwargs_preview" in log)
    assert start_log["kwargs_preview"]["secret"] == "<redacted>"
    assert start_log["kwargs_preview"]["token"] == "<redacted>"

    result_log = next(log for log in reversed(logs) if "result_preview" in log)
    assert result_log["result_preview"]["secret"] == "<redacted>"
    assert result_log["result_preview"]["token"] == "<redacted>"
    assert result_log["result_preview"]["visible"] == "ok"


def test_trace_applies_value_pattern_environment_redaction(monkeypatch):
    monkeypatch.setenv("EZTRACE_REDACT_VALUE_PATTERNS", r"secret\d+")
    setup.Setup.initialize("EZTRACER_ENV_VALUE_REDACTION", show_metrics=False)

    logs = []

    def capture_log_info(message, **kwargs):
        logs.append({"message": message, **kwargs})

    monkeypatch.setattr(tracer.logging, "log_info", capture_log_info)
    monkeypatch.setattr(tracer.logging, "record_metric", lambda *args, **kwargs: None)

    @tracer.trace()
    def foo(data):
        return {"response": data}

    foo(data="secret123")

    start_log = next(log for log in logs if "kwargs_preview" in log)
    assert start_log["kwargs_preview"]["data"] == "<redacted>"

    result_log = next(log for log in reversed(logs) if "result_preview" in log)
    assert result_log["result_preview"]["response"] == "<redacted>"
