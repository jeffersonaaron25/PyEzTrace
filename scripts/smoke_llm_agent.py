"""Installed-package smoke for optional LLM adapters and agent inspection.

Run with an environment containing ``pyeztrace[llm]``. Provider SDKs use
synthetic in-memory transports; this script never reads credentials or sends a
network request. Temporary trace files are removed on completion.
"""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace

import anthropic
import httpx2 as httpx
import openai
from langchain_core.language_models.fake_chat_models import FakeListChatModel

from pyeztrace import Run
from pyeztrace.custom_logging import Logging
from pyeztrace.integrations.langchain import PyEzTraceCallbackHandler
from pyeztrace.integrations.sdk import trace_anthropic, trace_openai
from pyeztrace.setup import Setup
from pyeztrace.viewer import _TraceTreeBuilder


CHAT = {
    'id': 'chat-smoke', 'object': 'chat.completion', 'created': 1,
    'model': 'synthetic-model',
    'choices': [{'index': 0, 'message': {'role': 'assistant', 'content': 'chat ok'},
                 'finish_reason': 'stop'}],
    'usage': {'prompt_tokens': 2, 'completion_tokens': 2, 'total_tokens': 4},
}
RESPONSE = {
    'id': 'response-smoke', 'object': 'response', 'created_at': 1,
    'model': 'synthetic-model', 'status': 'completed',
    'output': [{'type': 'message', 'id': 'message-smoke', 'role': 'assistant',
                'status': 'completed',
                'content': [{'type': 'output_text', 'text': 'response ok',
                             'annotations': []}]}],
    'usage': {'input_tokens': 2, 'output_tokens': 2, 'total_tokens': 4},
}
MESSAGE = {
    'id': 'message-smoke', 'type': 'message', 'role': 'assistant',
    'model': 'synthetic-model', 'content': [{'type': 'text', 'text': 'anthropic ok'}],
    'stop_reason': 'end_turn', 'stop_sequence': None,
    'usage': {'input_tokens': 2, 'output_tokens': 2},
}


def command(root, *arguments):
    result = subprocess.run(
        [sys.executable, '-m', 'pyeztrace.cli', *map(str, arguments)],
        cwd=root, capture_output=True, text=True, timeout=20,
    )
    assert result.returncode == 0, result.stderr
    assert not result.stderr
    return json.loads(result.stdout)


def main():
    with tempfile.TemporaryDirectory(prefix='pyeztrace-llm-smoke-') as directory:
        root = Path(directory)
        trace_file = root / 'llm.jsonl'
        Setup.initialize('llm-smoke', log_dir=root, log_file=trace_file.name,
                         file_format='json', console_format='plain',
                         disable_file_logging=False, buffer_enabled=False)
        Logging()

        openai_transport = httpx.MockTransport(
            lambda request: httpx.Response(
                200, json=CHAT if request.url.path.endswith('/chat/completions')
                else RESPONSE,
            )
        )
        anthropic_transport = httpx.MockTransport(
            lambda request: httpx.Response(200, json=MESSAGE)
        )

        with Run('installed-llm-smoke') as run:
            with openai.OpenAI(
                api_key='smoke-api-key-private',
                http_client=httpx.Client(transport=openai_transport),
            ) as original:
                client = trace_openai(original)
                chat = client.chat.completions.create(
                    model='synthetic-model',
                    messages=[{'role': 'user', 'content': 'chat input'}],
                )
                response = client.responses.create(
                    model='synthetic-model', input='response input',
                )
                assert chat.choices[0].message.content == 'chat ok'
                assert response.output_text == 'response ok'

            with anthropic.Anthropic(
                api_key='smoke-api-key-private',
                http_client=httpx.Client(transport=anthropic_transport),
            ) as original:
                message = trace_anthropic(original).messages.create(
                    model='synthetic-model', max_tokens=10,
                    messages=[{'role': 'user', 'content': 'anthropic input'}],
                )
                assert message.content[0].text == 'anthropic ok'

            callback = PyEzTraceCallbackHandler()
            assert FakeListChatModel(responses=['langchain ok']).invoke(
                'langchain input', config={'callbacks': [callback]},
            ).content == 'langchain ok'
            callback.close()

            events = [
                {'type': 'response.output_text.delta', 'delta': 'Bearer '},
                {'type': 'response.output_text.delta', 'delta': 'stream-secret'},
                {'type': 'response.completed', 'response': RESPONSE},
            ]
            stream_client = trace_openai(SimpleNamespace(
                responses=SimpleNamespace(create=lambda **kwargs: iter(events))))
            assert list(stream_client.responses.create(
                model='synthetic-model', input='stream input', stream=True,
            )) == events

            failing_client = trace_openai(SimpleNamespace(
                responses=SimpleNamespace(
                    create=lambda **kwargs: (_ for _ in ()).throw(
                        ValueError('authorization=must-not-be-logged')))))
            try:
                failing_client.responses.create(
                    model='synthetic-model', input='failure input')
            except ValueError:
                pass
            else:  # pragma: no cover - smoke assertion
                raise AssertionError('provider failure did not propagate')

        Logging.flush_logs()
        raw = trace_file.read_text(encoding='utf-8')
        assert 'smoke-api-key-private' not in raw
        assert 'stream-secret' not in raw
        assert 'must-not-be-logged' not in raw
        assert '[REDACTED]' in raw

        runs = command(root, 'runs', trace_file)
        assert runs['schema_version'] == 1
        assert runs['data'] == [{'run_id': run.id, 'name': 'installed-llm-smoke',
                                  'status': 'success', 'calls': 6}]
        tree = command(root, 'tree', trace_file, '--run', run.id)
        assert len(tree['data']) == 6
        assert {call['kind'] for call in tree['data']} == {'llm'}
        failures = command(root, 'errors', trace_file, '--run', run.id)
        assert len(failures['data']) == 1
        failure = failures['data'][0]
        path = command(root, 'path', trace_file, failure['call_id'], '--run', run.id)
        assert [call['call_id'] for call in path['data']] == [failure['call_id']]
        payload = command(root, 'payload', trace_file, failure['call_id'],
                          '--run', run.id, '--max-bytes', 512)
        assert payload['data']['bytes'] > 0

        viewed = _TraceTreeBuilder(trace_file).build_tree()
        viewer_calls = []
        pending = list(viewed['roots'])
        while pending:
            node = pending.pop()
            viewer_calls.append(node)
            pending.extend(node.get('children', []))
        assert len(viewer_calls) == 6
        assert sum(node.get('status') == 'error' for node in viewer_calls) == 1
        assert all(node.get('run_id') == run.id for node in viewer_calls)
        assert {node.get('llm', {}).get('provider') for node in viewer_calls} == {
            'openai', 'anthropic', 'langchain'
        }

        print('PASS: installed OpenAI Chat/Responses, Anthropic Messages,')
        print('      LangChain, bounded stream redaction, failure propagation,')
        print('      agent CLI investigation, and viewer parsing agree.')


if __name__ == '__main__':
    main()
