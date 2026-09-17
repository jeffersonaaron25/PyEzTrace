"""Explicit OpenAI/Anthropic client proxies; SDK imports stay in user code."""
from pyeztrace.llm import CapturePolicy, wrap_create


class _Proxy:
    def __init__(self, target, provider, policy, path=()):
        self._target, self._provider, self._policy, self._path = target, provider, policy, path

    def __getattr__(self, name):
        value = getattr(self._target, name)
        path = self._path + (name,)
        routes = {('chat', 'completions', 'create'), ('responses', 'create')} if self._provider == 'openai' else {('messages', 'create')}
        if path in routes:
            return wrap_create(value, self._provider, '.'.join(path), self._policy)
        if any(route[:len(path)] == path for route in routes):
            return _Proxy(value, self._provider, self._policy, path)
        return value

    def __enter__(self):
        self._target.__enter__()
        return self

    def __exit__(self, *args):
        return self._target.__exit__(*args)

    async def __aenter__(self):
        await self._target.__aenter__()
        return self

    async def __aexit__(self, *args):
        return await self._target.__aexit__(*args)


def trace_openai(client, *, capture_content=True, max_content_bytes=8192, redact=None):
    """Return a proxy tracing Chat Completions/Responses create, including stream=True."""
    return _Proxy(client, 'openai', CapturePolicy(capture_content, max_content_bytes, redact))


def trace_anthropic(client, *, capture_content=True, max_content_bytes=8192, redact=None):
    """Return a proxy tracing Messages create, including stream=True."""
    return _Proxy(client, 'anthropic', CapturePolicy(capture_content, max_content_bytes, redact))
