"""Bounded local LLM capture with no SDK dependencies or automatic networking."""
import inspect
import math
import os
import re
import time
import uuid
from contextlib import contextmanager

_SECRET_KEY = re.compile(r'api.?key|authorization|password|secret|cookie|access.?token|refresh.?token', re.I)
_SECRET_VALUE = re.compile(r'\b(?:sk-[\w-]+|Bearer\s+[\w.\-/+=]+)', re.I)
_FIELDS = ('id', 'type', 'role', 'text', 'content', 'choices', 'message', 'delta',
           'output', 'output_text', 'tool_calls', 'function', 'name', 'arguments',
           'call_id', 'tool_use_id', 'input', 'response', 'stop_reason', 'finish_reason',
           'generation_info', 'generations', 'additional_kwargs')


def field(value, key, default=None):
    return value.get(key, default) if isinstance(value, dict) else getattr(value, key, default)


class CapturePolicy:
    """Local-development content capture; disable explicitly for metadata only."""
    def __init__(self, capture_content=True, max_content_bytes=8192, redact=None):
        if type(max_content_bytes) is not int or not 1 <= max_content_bytes <= 65536:
            raise ValueError('max_content_bytes must be between 1 and 65536')
        self.capture_content = bool(capture_content)
        self.max_content_bytes = max_content_bytes
        self.redact = redact

    def capture(self, value, budget=None):
        if not self.capture_content:
            return None
        import json
        from pyeztrace.tracer import _resolve_redaction
        settings = _resolve_redaction(None)
        nodes = [0]
        clipped = [False]
        limit = self.max_content_bytes if budget is None else max(0, budget)

        def safe(obj, depth=0):
            nodes[0] += 1
            if nodes[0] > 256 or depth > 8:
                clipped[0] = True
                return '[limit]'
            if obj is None or type(obj) in (bool, int):
                return obj
            if isinstance(obj, float):
                return obj if math.isfinite(obj) else None
            if isinstance(obj, str):
                clipped[0] |= len(obj) > limit
                text = obj[:limit]
                text = _SECRET_VALUE.sub('[REDACTED]', text)
                if settings and settings.value_patterns:
                    for pattern in settings.value_patterns:
                        text = pattern.sub('[REDACTED]', text)
                return text
            if isinstance(obj, (list, tuple)):
                clipped[0] |= len(obj) > 64
                return [safe(x, depth + 1) for x in obj[:64]]
            if isinstance(obj, dict):
                clipped[0] |= len(obj) > 64
                from itertools import islice
                items = list(islice(obj, 64))
                return {str(k)[:128]: '[REDACTED]' if _SECRET_KEY.search(str(k)) or
                        (settings and settings.should_redact_key(k)) else safe(obj[k], depth + 1)
                        for k in items}
            # Only known SDK fields, never repr(), headers, request objects or credentials.
            return {key: safe(field(obj, key), depth + 1) for key in _FIELDS
                    if field(obj, key) is not None}

        try:
            value = safe(value)
            if self.redact is not None:
                value = self.redact(value)
            raw = json.dumps(value, ensure_ascii=False, allow_nan=False).encode('utf-8')
            return {'text': raw[:limit].decode('utf-8', errors='ignore'),
                    'truncated': clipped[0] or len(raw) > limit, 'bytes': min(len(raw), limit)}
        except Exception:
            # A failed redactor must never fall back to unredacted data.
            return {'text': '[capture unavailable]', 'truncated': False, 'bytes': 0}


class Operation:
    def __init__(self, name, provider, params, policy, parent_id=None, identity=None, run_id=None):
        from pyeztrace.events import current_run_id
        from pyeztrace.tracer import _call_stack_ids
        self.policy = policy
        self.name, self.provider = name, provider
        self.id = identity or uuid.uuid4().hex
        stack = _call_stack_ids.get()
        self.parent = parent_id if parent_id is not None else (stack[-1] if stack else None)
        self.run = run_id or current_run_id()
        self.started = time.monotonic()
        self.done = False
        self.outcome = 'success'
        self.metadata = {'provider': provider, 'model': str(params.get('model', 'unknown'))[:256]}
        self.chunks = []
        self.remaining = policy.max_content_bytes
        self.chunk_count = 0
        self.stream_text = {}
        self.stream_truncated = False
        content = {k: params[k] for k in ('messages', 'input', 'instructions', 'system', 'tools', 'prompt') if k in params}
        self.emit('start', 'running', input=policy.capture(content))

    @contextmanager
    def scope(self):
        from pyeztrace.tracer import _call_stack_ids
        token = _call_stack_ids.set(_call_stack_ids.get() + (self.id,))
        try:
            yield
        finally:
            _call_stack_ids.reset(token)

    def emit(self, event, status, **values):
        from pyeztrace.custom_logging import Logging
        from pyeztrace.events import _run
        token = _run.set((os.getpid(), self.run))
        try:
            Logging.log_info('LLM ' + event, function=self.name, event=event,
                             call_id=self.id, parent_id=self.parent, kind='llm', status=status,
                             time_epoch=time.time(),
                             duration=None if event == 'start' else time.monotonic() - self.started,
                             llm={**self.metadata, **values},
                             **({'args_preview': values['input']} if event == 'start' else {'result_preview': values}))
        finally:
            _run.reset(token)

    def observe(self, value, streaming=False):
        response = field(value, 'response') or field(value, 'message') or value
        event_type = field(value, 'type')
        response_status = field(response, 'status')
        if event_type in ('error', 'response.failed') or response_status == 'failed':
            self.outcome = 'error'
        elif self.outcome != 'error' and (event_type == 'response.incomplete' or response_status == 'incomplete'):
            self.outcome = 'incomplete'
        model = field(response, 'model')
        if isinstance(model, str):
            self.metadata['model'] = model[:256]
        identity = field(response, 'id')
        if isinstance(identity, str):
            self.metadata['response_id'] = identity[:256]
        usage = field(response, 'usage') or field(value, 'usage')
        if usage:
            counts = {key: field(usage, key) for key in ('input_tokens', 'output_tokens', 'total_tokens',
                                                       'prompt_tokens', 'completion_tokens',
                                                       'cache_read_input_tokens', 'cache_creation_input_tokens')}
            self.metadata.setdefault('usage', {}).update({k: v for k, v in counts.items() if type(v) is int and v >= 0})
        self.observe_tools(value)
        self.chunk_count += 1
        if streaming:
            self.observe_delta(value)
            return
        if self.policy.capture_content and self.remaining > 0 and len(self.chunks) < 64:
            captured = self.policy.capture(value, self.remaining)
            self.remaining -= captured['bytes']
            self.chunks.append(captured)

    def observe_tools(self, value):
        pending, visited = [value], 0
        while pending and visited < 256:
            item = pending.pop()
            visited += 1
            if isinstance(item, (list, tuple)):
                pending.extend(item[:64])
                continue
            kind = field(item, 'type')
            if kind in ('function', 'function_call', 'tool_use'):
                identity = field(item, 'call_id') or field(item, 'id')
                name = field(item, 'name') or field(field(item, 'function'), 'name')
                if isinstance(identity, str):
                    tools = self.metadata.setdefault('tool_calls', [])
                    metadata = {'id': identity[:256], 'name': name[:128] if isinstance(name, str) else None}
                    if metadata not in tools and len(tools) < 64:
                        tools.append(metadata)
            for key in ('choices', 'message', 'delta', 'output', 'content', 'tool_calls', 'content_block', 'item', 'response'):
                child = field(item, key)
                if child is not None and not isinstance(child, (str, bytes, int, float, bool)):
                    pending.append(child)

    def observe_delta(self, value):
        # Join each textual channel before redaction: credentials may span chunks.
        fragments = []
        delta = field(value, 'delta')
        if isinstance(delta, str):
            fragments.append(('text', delta))
        elif delta is not None:
            fragments.extend((key, field(delta, key)) for key in ('text', 'partial_json'))
        for choice in (field(value, 'choices') or [])[:64]:
            prefix = str(field(choice, 'index', 0))[:32]
            delta = field(choice, 'delta')
            fragments.append((prefix + '.text', field(delta, 'content')))
            for tool in (field(delta, 'tool_calls') or [])[:64]:
                key = prefix + '.tool.' + str(field(tool, 'index', 0))[:32]
                fragments.append((key, field(field(tool, 'function'), 'arguments')))
        if not self.policy.capture_content:
            return
        for key, text in fragments:
            if not isinstance(text, str):
                continue
            if key not in self.stream_text and len(self.stream_text) >= 64:
                self.stream_truncated = True
                continue
            raw = text[:self.remaining + 1].encode('utf-8')
            part = raw[:self.remaining].decode('utf-8', errors='ignore')
            self.stream_text[key] = self.stream_text.get(key, '') + part
            self.stream_truncated |= len(raw) > self.remaining
            self.remaining -= len(part.encode('utf-8'))

    def finish(self, status='success', error=None):
        if self.done:
            return
        self.done = True
        if status == 'success':
            status = self.outcome
        if self.stream_text:
            self.chunks = [self.policy.capture(self.stream_text)]
            self.stream_text.clear()
        self.emit('error' if status == 'error' else 'end', status,
                  output=self.chunks if self.policy.capture_content else None,
                  content_truncated=(self.stream_truncated or self.remaining == 0 or any(c['truncated'] for c in self.chunks)) if self.policy.capture_content else False,
                  error_type=type(error).__name__ if error else None)


class Stream:
    """Preserve iterator values and SDK attributes while observing consumption."""
    def __init__(self, stream, operation):
        self._stream, self._operation = stream, operation
        self._iterator = None
        self._aiterator = None

    def __getattr__(self, name):
        return getattr(self._stream, name)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            if self._iterator is None:
                self._iterator = iter(self._stream)
            with self._operation.scope():
                value = next(self._iterator)
            self._operation.observe(value, streaming=True)
            return value
        except StopIteration:
            self._operation.finish()
            raise
        except BaseException as exc:
            self._operation.finish('error' if isinstance(exc, Exception) else 'cancelled', exc)
            raise

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            if self._aiterator is None:
                self._aiterator = self._stream.__aiter__()
            with self._operation.scope():
                value = await self._aiterator.__anext__()
            self._operation.observe(value, streaming=True)
            return value
        except StopAsyncIteration:
            self._operation.finish()
            raise
        except BaseException as exc:
            self._operation.finish('error' if isinstance(exc, Exception) else 'cancelled', exc)
            raise

    def close(self):
        try:
            close = getattr(self._stream, 'close', None)
            return close() if close else None
        finally:
            self._operation.finish('cancelled')

    async def aclose(self):
        try:
            close = getattr(self._stream, 'aclose', None) or getattr(self._stream, 'close', None)
            if close:
                result = close()
                if inspect.isawaitable(result):
                    await result
        finally:
            self._operation.finish('cancelled')

    def __enter__(self):
        if hasattr(self._stream, '__enter__'):
            self._stream.__enter__()
        return self

    def __exit__(self, *args):
        self.close()

    async def __aenter__(self):
        if hasattr(self._stream, '__aenter__'):
            await self._stream.__aenter__()
        return self

    async def __aexit__(self, *args):
        await self.aclose()


def wrap_create(create, provider, api, policy):
    """Wrap an SDK create method without replacing global SDK classes."""
    from functools import wraps

    def result(value, operation, streaming):
        if streaming:
            return Stream(value, operation)
        operation.observe(value)
        operation.finish()
        return value

    @wraps(create)
    def wrapped(*args, **kwargs):
        operation = Operation(provider + '.' + api, provider, kwargs, policy)
        try:
            with operation.scope():
                value = create(*args, **kwargs)
        except BaseException as exc:
            operation.finish('error' if isinstance(exc, Exception) else 'cancelled', exc)
            raise
        if inspect.isawaitable(value):
            async def awaited():
                try:
                    with operation.scope():
                        response = await value
                    return result(response, operation, kwargs.get('stream', False))
                except BaseException as exc:
                    operation.finish('error' if isinstance(exc, Exception) else 'cancelled', exc)
                    raise
            return awaited()
        return result(value, operation, kwargs.get('stream', False))
    return wrapped
