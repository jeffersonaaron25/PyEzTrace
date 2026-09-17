"""LangChain callback integration; only this module imports langchain-core."""
import threading

try:
    from langchain_core.callbacks import BaseCallbackHandler
except ImportError as exc:
    raise ImportError('Install pyeztrace[langchain] to use the LangChain integration') from exc

from pyeztrace.llm import CapturePolicy, Operation, field


class PyEzTraceCallbackHandler(BaseCallbackHandler):
    """Capture local model/tool operations; do not combine with SDK wrapping.

    The same handler can be shared by concurrent runs. Pending operations are
    bounded; call close() when abandoning a framework run without end callbacks.
    """
    run_inline = True

    def __init__(self, *, capture_content=True, max_content_bytes=8192, redact=None):
        self.policy = CapturePolicy(capture_content, max_content_bytes, redact)
        self._operations = {}
        self._lock = threading.RLock()

    def _start(self, name, params, run_id, parent_run_id):
        with self._lock:
            if len(self._operations) >= 1024 or str(run_id) in self._operations:
                return
            parent = self._operations.get(str(parent_run_id))
            operation = Operation(name, 'langchain', params, self.policy,
                                  parent_id=str(parent_run_id) if parent_run_id else None,
                                  identity=str(run_id), run_id=parent.run if parent else None)
            self._operations[str(run_id)] = operation

    def on_chat_model_start(self, serialized, messages, *, run_id, parent_run_id=None, **kwargs):
        params = kwargs.get('invocation_params') or {}
        self._start('langchain.chat', {'messages': messages, 'model': params.get('model_name', params.get('model', 'unknown'))}, run_id, parent_run_id)

    def on_llm_start(self, serialized, prompts, *, run_id, parent_run_id=None, **kwargs):
        params = kwargs.get('invocation_params') or {}
        self._start('langchain.llm', {'prompt': prompts, 'model': params.get('model_name', params.get('model', 'unknown'))}, run_id, parent_run_id)

    def on_llm_end(self, response, *, run_id, **kwargs):
        with self._lock:
            operation = self._operations.pop(str(run_id), None)
        if operation:
            generations = field(response, 'generations') or []
            usage = (field(response, 'llm_output') or {}).get('token_usage', {})
            if not usage:
                # Newer chat models attach usage to the returned AIMessage.
                # Count one candidate per batch, not each alternative generation.
                usage = {}
                for batch in generations[:64]:
                    if not batch:
                        continue
                    counts = field(field(batch[0], 'message'), 'usage_metadata') or {}
                    for key in ('input_tokens', 'output_tokens', 'total_tokens'):
                        count = field(counts, key)
                        if type(count) is int and count >= 0:
                            usage[key] = usage.get(key, 0) + count
            operation.observe({'output': field(response, 'generations'),
                               'usage': usage})
            operation.finish()

    def on_llm_error(self, error, *, run_id, **kwargs):
        with self._lock:
            operation = self._operations.pop(str(run_id), None)
        if operation:
            operation.finish('error', error)

    def on_tool_start(self, serialized, input_str, *, run_id, parent_run_id=None, **kwargs):
        self._start('langchain.tool.' + str((serialized or {}).get('name', 'tool'))[:128],
                    {'input': input_str}, run_id, parent_run_id)

    def on_tool_end(self, output, *, run_id, **kwargs):
        with self._lock:
            operation = self._operations.pop(str(run_id), None)
        if operation:
            operation.observe({'output': output})
            operation.finish()

    def on_tool_error(self, error, *, run_id, **kwargs):
        self.on_llm_error(error, run_id=run_id)

    def on_chain_start(self, serialized, inputs, *, run_id, parent_run_id=None, **kwargs):
        name = kwargs.get('name') or (serialized or {}).get('name', 'chain')
        self._start('langchain.chain.' + str(name)[:128], {'input': inputs}, run_id, parent_run_id)

    def on_chain_end(self, outputs, *, run_id, **kwargs):
        self.on_tool_end(outputs, run_id=run_id)

    def on_chain_error(self, error, *, run_id, **kwargs):
        self.on_llm_error(error, run_id=run_id)

    def close(self):
        with self._lock:
            pending, self._operations = self._operations, {}
        for operation in pending.values():
            operation.finish('cancelled')
