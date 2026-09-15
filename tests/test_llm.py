import asyncio
import json

import pytest

from pyeztrace.llm import CapturePolicy, wrap_create


@pytest.fixture
def records(monkeypatch):
    from pyeztrace.custom_logging import Logging
    rows = []
    monkeypatch.setattr(Logging, 'log_info', lambda message, **kw: rows.append(kw))
    return rows


def test_content_bounds_credentials_and_redaction_fail_closed():
    policy = CapturePolicy(max_content_bytes=100)
    value = policy.capture({'api_key': 'secret', 'content': 'Bearer abc-secret sk-another-secret ' + '☃' * 200})
    assert len(value['text'].encode()) <= 100 and value['truncated']
    assert 'abc-secret' not in value['text'] and 'another-secret' not in value['text']
    assert 'secret' not in value['text']
    def fail(value):
        raise ValueError('redactor failure')
    assert CapturePolicy(redact=fail).capture('private')['text'] == '[capture unavailable]'
    assert CapturePolicy(capture_content=False).capture('private') is None


def test_response_identity_and_metadata_only(records):
    response = {'id': 'r1', 'model': 'm', 'output': 'private', 'usage': {'input_tokens': 2}}
    create = wrap_create(lambda **k: response, 'test', 'create', CapturePolicy(False))
    assert create(model='m', input='private') is response
    assert records[-1]['llm']['usage']['input_tokens'] == 2
    assert 'private' not in json.dumps(records)


def test_stream_finishes_on_exhaustion_and_close(records):
    create = wrap_create(lambda **k: iter([{'text': 'a'}, {'text': 'b'}]), 'test', 'create', CapturePolicy())
    stream = create(stream=True)
    assert len(records) == 1
    assert list(stream) == [{'text': 'a'}, {'text': 'b'}]
    assert records[-1]['status'] == 'success'
    stream.close()
    assert len(records) == 2
    other = create(stream=True)
    next(other)
    other.close()
    assert records[-1]['status'] == 'cancelled'


async def test_async_stream_cancellation_and_parent_context(records):
    from pyeztrace.tracer import _call_stack_ids
    async def chunks():
        yield {'text': 'first'}
        raise asyncio.CancelledError()
    async def create(**kwargs):
        return chunks()
    before = _call_stack_ids.get()
    stream = await wrap_create(create, 'test', 'create', CapturePolicy())(stream=True)
    assert await stream.__anext__() == {'text': 'first'}
    with pytest.raises(asyncio.CancelledError):
        await stream.__anext__()
    assert _call_stack_ids.get() == before
    assert records[-1]['status'] == 'cancelled'


def test_error_text_is_not_captured(records):
    def create(**kwargs):
        raise ValueError('authorization=private-token')
    with pytest.raises(ValueError):
        wrap_create(create, 'test', 'create', CapturePolicy())()
    assert records[-1]['status'] == 'error'
    assert 'private-token' not in json.dumps(records)


@pytest.mark.parametrize('chunks', [
    [{'type': 'response.output_text.delta', 'delta': x} for x in ('sk-', 'secret-value')],
    [{'choices': [{'index': 0, 'delta': {'content': x}}]} for x in ('Bearer ', 'secret-value')],
    [{'type': 'content_block_delta', 'delta': {'text': x}} for x in ('sk-', 'secret-value')],
])
def test_stream_redacts_credentials_across_chunk_boundaries(records, chunks):
    stream = wrap_create(lambda **k: iter(chunks), 'test', 'create', CapturePolicy())(stream=True)
    assert list(stream) == chunks
    assert 'secret-value' not in json.dumps(records)
    assert 'REDACTED' in json.dumps(records)
    assert records[-1]['llm']['content_truncated'] is False


@pytest.mark.parametrize('event,status', [('response.failed', 'error'), ('response.incomplete', 'incomplete')])
def test_stream_protocol_failure_is_not_success(records, event, status):
    stream = wrap_create(lambda **k: iter([{'type': event}]), 'test', 'create', CapturePolicy())(stream=True)
    list(stream)
    assert records[-1]['status'] == status


def test_metadata_only_keeps_tool_relationships(records):
    result = {'output': [{'type': 'function_call', 'call_id': 'tool-1', 'name': 'lookup', 'arguments': 'private'}]}
    wrap_create(lambda **k: result, 'test', 'create', CapturePolicy(False))()
    assert records[-1]['llm']['tool_calls'] == [{'id': 'tool-1', 'name': 'lookup'}]
    assert 'private' not in json.dumps(records)


def test_structural_capture_limits_are_reported():
    captured = CapturePolicy().capture(list(range(100)))
    assert captured['truncated'] is True
    assert len(json.loads(captured['text'])) == 64
