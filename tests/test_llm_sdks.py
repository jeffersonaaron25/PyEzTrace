"""Real optional SDKs against synthetic transports: never live credentials."""
import json
import uuid

import pytest

httpx = pytest.importorskip('httpx2')
openai = pytest.importorskip('openai')
anthropic = pytest.importorskip('anthropic')

from pyeztrace.integrations.sdk import trace_openai, trace_anthropic


@pytest.fixture
def records(monkeypatch):
    from pyeztrace.custom_logging import Logging
    rows = []
    from pyeztrace.events import current_run_id
    monkeypatch.setattr(Logging, 'log_info', lambda message, **kw: rows.append({**kw, '_run_id': current_run_id()}))
    return rows


CHAT = {'id': 'chat1', 'object': 'chat.completion', 'created': 1, 'model': 'test-model',
        'choices': [{'index': 0, 'message': {'role': 'assistant', 'content': 'hello'}, 'finish_reason': 'stop'}],
        'usage': {'prompt_tokens': 2, 'completion_tokens': 1, 'total_tokens': 3}}
RESPONSES = {'id': 'resp1', 'object': 'response', 'created_at': 1, 'model': 'test-model', 'status': 'completed',
             'output': [{'type': 'message', 'id': 'msg1', 'role': 'assistant', 'status': 'completed',
                         'content': [{'type': 'output_text', 'text': 'hello', 'annotations': []}]}],
             'usage': {'input_tokens': 2, 'output_tokens': 1, 'total_tokens': 3}}
MESSAGE = {'id': 'msg1', 'type': 'message', 'role': 'assistant', 'model': 'test-model',
           'content': [{'type': 'text', 'text': 'hello'}], 'stop_reason': 'end_turn', 'stop_sequence': None,
           'usage': {'input_tokens': 2, 'output_tokens': 1}}


@pytest.mark.parametrize('api,body', [('chat', CHAT), ('responses', RESPONSES), ('anthropic', MESSAGE)])
def test_real_sdk_nonstream(api, body, records):
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=body))
    client_type = anthropic.Anthropic if api == 'anthropic' else openai.OpenAI
    with client_type(api_key='synthetic', http_client=httpx.Client(transport=transport)) as original:
        client = trace_anthropic(original) if api == 'anthropic' else trace_openai(original)
        if api == 'responses':
            response = client.responses.create(model='test-model', input='hi')
            assert response.output_text == 'hello'
        elif api == 'chat':
            response = client.chat.completions.create(model='test-model', messages=[{'role': 'user', 'content': 'hi'}])
            assert response.choices[0].message.content == 'hello'
        else:
            response = client.messages.create(model='test-model', max_tokens=10, messages=[{'role': 'user', 'content': 'hi'}])
            assert response.content[0].text == 'hello'
    assert len(records) == 2 and records[-1]['status'] == 'success'
    assert records[-1]['llm']['usage']
    assert 'synthetic' not in json.dumps(records)


@pytest.mark.parametrize('api', ['chat', 'responses', 'anthropic'])
async def test_real_sdk_async_stream(api, records):
    if api == 'chat':
        events = [{'id': 'chat1', 'object': 'chat.completion.chunk', 'created': 1, 'model': 'test-model',
                   'choices': [{'index': 0, 'delta': {'content': 'hello'}, 'finish_reason': 'stop'}]}]
    elif api == 'responses':
        events = [{'type': 'response.output_text.delta', 'delta': 'hello', 'item_id': 'msg1', 'output_index': 0, 'content_index': 0, 'sequence_number': 1},
                  {'type': 'response.completed', 'response': RESPONSES, 'sequence_number': 2}]
    else:
        events = [{'type': 'message_start', 'message': MESSAGE},
                  {'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'text_delta', 'text': 'hello'}},
                  {'type': 'message_stop'}]
    wire = ''.join(('event: ' + e['type'] + '\n' if 'type' in e else '') + 'data: ' + json.dumps(e) + '\n\n' for e in events)
    if api == 'chat':
        wire += 'data: [DONE]\n\n'
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=wire, headers={'content-type': 'text/event-stream'}))
    client_type = anthropic.AsyncAnthropic if api == 'anthropic' else openai.AsyncOpenAI
    async with client_type(api_key='synthetic', http_client=httpx.AsyncClient(transport=transport)) as original:
        client = trace_anthropic(original) if api == 'anthropic' else trace_openai(original)
        if api == 'responses':
            stream = await client.responses.create(model='test-model', input='hi', stream=True)
        elif api == 'chat':
            stream = await client.chat.completions.create(model='test-model', messages=[], stream=True)
        else:
            stream = await client.messages.create(model='test-model', max_tokens=10, messages=[], stream=True)
        assert len(records) == 1
        async with stream:
            received = [event async for event in stream]
        assert len(received) == len(events)
    assert len(records) == 2 and records[-1]['status'] == 'success'
    assert 'hello' in json.dumps(records[-1]['llm']['output'])


def test_langchain_real_callback_protocol(records):
    pytest.importorskip('langchain_core')
    from langchain_core.language_models.fake_chat_models import FakeListChatModel
    from pyeztrace.integrations.langchain import PyEzTraceCallbackHandler
    model = FakeListChatModel(responses=['hello'])
    callback = PyEzTraceCallbackHandler()
    response = model.invoke('hi', config={'callbacks': [callback]})
    assert response.content == 'hello'
    assert records[-1]['status'] == 'success'
    assert not callback._operations
    identifier = uuid.uuid4()
    callback.on_tool_start({'name': 'tool'}, 'hi', run_id=identifier)
    callback.close()
    assert records[-1]['status'] == 'cancelled'


@pytest.mark.parametrize('api,body', [('chat', CHAT), ('responses', RESPONSES), ('anthropic', MESSAGE)])
async def test_real_sdk_async_response(api, body, records):
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=body))
    client_type = anthropic.AsyncAnthropic if api == 'anthropic' else openai.AsyncOpenAI
    async with client_type(api_key='synthetic', http_client=httpx.AsyncClient(transport=transport)) as original:
        client = trace_anthropic(original) if api == 'anthropic' else trace_openai(original)
        if api == 'responses':
            result = await client.responses.create(model='test-model', input='hi')
        elif api == 'chat':
            result = await client.chat.completions.create(model='test-model', messages=[])
        else:
            result = await client.messages.create(model='test-model', max_tokens=10, messages=[])
        assert result.id == body['id']
    assert records[-1]['status'] == 'success'


@pytest.mark.parametrize('api', ['chat', 'responses', 'anthropic'])
def test_real_sdk_sync_stream_close(api, records):
    if api == 'chat':
        event = {'id': 'chat1', 'object': 'chat.completion.chunk', 'created': 1, 'model': 'test-model',
                 'choices': [{'index': 0, 'delta': {'content': 'hello'}, 'finish_reason': None}]}
    elif api == 'responses':
        event = {'type': 'response.output_text.delta', 'delta': 'hello', 'item_id': 'msg1', 'output_index': 0,
                 'content_index': 0, 'sequence_number': 1}
    else:
        event = {'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'text_delta', 'text': 'hello'}}
    wire = ('event: ' + event['type'] + '\n' if 'type' in event else '') + 'data: ' + json.dumps(event) + '\n\n'
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=wire, headers={'content-type': 'text/event-stream'}))
    client_type = anthropic.Anthropic if api == 'anthropic' else openai.OpenAI
    with client_type(api_key='synthetic', http_client=httpx.Client(transport=transport)) as original:
        client = trace_anthropic(original) if api == 'anthropic' else trace_openai(original)
        if api == 'responses':
            stream = client.responses.create(model='test-model', input='hi', stream=True)
        elif api == 'chat':
            stream = client.chat.completions.create(model='test-model', messages=[], stream=True)
        else:
            stream = client.messages.create(model='test-model', max_tokens=10, messages=[], stream=True)
        with stream:
            next(stream)
    assert records[-1]['status'] == 'cancelled'
    assert 'hello' in json.dumps(records[-1]['llm']['output'])


def test_langchain_chain_parent_survives_thread_boundary(records):
    import concurrent.futures
    from pyeztrace import Run
    from pyeztrace.integrations.langchain import PyEzTraceCallbackHandler
    callback = PyEzTraceCallbackHandler(capture_content=False)
    root, child = uuid.uuid4(), uuid.uuid4()
    with Run('chain') as run:
        callback.on_chain_start({'name': 'chain'}, {}, run_id=root)
        with concurrent.futures.ThreadPoolExecutor() as pool:
            pool.submit(callback.on_tool_start, {'name': 'tool'}, 'private', run_id=child, parent_run_id=root).result()
        callback.on_tool_end('private', run_id=child)
        callback.on_chain_end({}, run_id=root)
    calls = [row for row in records if row.get('call_id') == str(child)]
    assert len(calls) == 2 and calls[0]['parent_id'] == str(root)
    assert all(row['_run_id'] == run.id for row in calls)
    assert 'private' not in json.dumps(records)
    assert not callback._operations


def test_langchain_ai_message_usage(records):
    from langchain_core.messages import AIMessage
    from langchain_core.outputs import LLMResult, ChatGeneration
    from pyeztrace.integrations.langchain import PyEzTraceCallbackHandler
    callback = PyEzTraceCallbackHandler(capture_content=False)
    identity = uuid.uuid4()
    callback.on_llm_start({}, ['private'], run_id=identity)
    generation = ChatGeneration(message=AIMessage(content='private', usage_metadata={
        'input_tokens': 3, 'output_tokens': 4, 'total_tokens': 7}))
    callback.on_llm_end(LLMResult(generations=[[generation, generation]]), run_id=identity)
    assert records[-1]['llm']['usage'] == {'input_tokens': 3, 'output_tokens': 4, 'total_tokens': 7}
    assert 'private' not in json.dumps(records)
