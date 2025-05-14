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