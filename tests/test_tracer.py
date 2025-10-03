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

    def bar(): called.append("bar")
    def baz(): called.append("baz")

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