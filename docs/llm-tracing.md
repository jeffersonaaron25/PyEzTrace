# Local LLM tracing (0.2.0 development)

LLM tracing is optional. `pip install pyeztrace` still installs no runtime
dependencies. Install only the SDK you use:

```sh
pip install 'pyeztrace[openai]'
pip install 'pyeztrace[anthropic]'
pip install 'pyeztrace[langchain]'
# All three integrations:
pip install 'pyeztrace[llm]'
```

The extras select tested SDK baselines: OpenAI 3.13.0, Anthropic 1.5.0,
and langchain-core 1.6.3 or newer. These SDK baselines require Python 3.10+, separately from
the core's Python 3.9+ support. No SDK is imported by the core, and PyEzTrace
never installs packages or creates a provider client automatically.

Initialize JSON file logging once, then wrap the client you already use:

```python
from openai import OpenAI
from pyeztrace import Run
from pyeztrace.setup import Setup
from pyeztrace.custom_logging import Logging
from pyeztrace.integrations.sdk import trace_openai

Setup.initialize("LLM", log_dir=".", log_file="llm.log",
                 file_format="json", disable_file_logging=False)
Logging()
client = trace_openai(OpenAI())
with Run("answer-question"):
    response = client.responses.create(model="your-model", input="Hello")
```

Use your account's model and authentication configuration. `trace_openai` covers
`chat.completions.create` and `responses.create`; `trace_anthropic` covers
`messages.create`. Both accept synchronous and asynchronous clients and preserve
the returned response objects. The original client remains available and unmodified.
The proxy forwards other methods without instrumenting them: convenience
`.stream()`, `.parse()`, batch, raw-response and client-cloning helpers are outside
this first adapter surface. Use `create(stream=True)` for traced streams.

```python
from anthropic import AsyncAnthropic
from pyeztrace.integrations.sdk import trace_anthropic

async def answer():
    async with trace_anthropic(AsyncAnthropic()) as client:
        stream = await client.messages.create(
            model="your-model", max_tokens=100,
            messages=[{"role": "user", "content": "Hello"}], stream=True,
        )
        async with stream:
            async for event in stream:
                pass  # consume the original SDK events
```

Consume streams to completion, or close them explicitly when stopping early.
Exhaustion records the observed outcome; close before exhaustion records
`cancelled`. Exceptions preserve their original type and propagation. An abandoned
stream with no close/end remains incomplete. The stream is an iterator proxy,
so exact stream-class identity is not preserved. Provider failure/incomplete
Responses events are reflected in the recorded status. Usage is reported only
when the provider supplies it; no token counts or prices are estimated.

For LangChain, pass a callback instead of wrapping the underlying SDK:

```python
from pyeztrace.integrations.langchain import PyEzTraceCallbackHandler

handler = PyEzTraceCallbackHandler()
try:
    result = model.invoke("Hello", config={"callbacks": [handler]})
finally:
    handler.close()
```

The handler records model, chain, and tool callbacks with parent relationships.
It supports concurrent runs and retains at most 1,024 unfinished operations;
additional starts are ignored at that limit. `close()` marks abandoned operations
cancelled. Combining callbacks with SDK wrapping produces duplicate model traces.
SDK adapters record available tool-call IDs and names; they do not execute or
automatically trace your tools. Trace tool functions normally to capture execution.

## Content and security

Content capture defaults **on for local development**. Turn it off for metadata-only
records with `trace_openai(client, capture_content=False)`,
`trace_anthropic(client, capture_content=False)`, or the equivalent handler option.

`max_content_bytes` defaults to 8,192 bytes per input/output capture and accepts
1–65,536. Captures are UTF-8 previews, can be truncated, and are not guaranteed to
contain complete JSON. Traversal also limits depth, collections, and object fields.
Stream text/tool-argument fragments are joined in bounded memory before redaction.
Multimodal binaries and complete provider payload reconstruction are not supported.

Built-in credential-key/token-pattern redaction and the existing tracer redaction
settings apply before writing content. A `redact=value_to_sanitized_value` callback
can add application-specific filtering; callback failure suppresses captured content.
Adapter exceptions keep their original type and message in application code.
Enclosing PyEzTrace decorators record the exception type and omit the message
and traceback, including in exported OpenTelemetry spans. Ordinary decorated
functions also omit exception details when tracer redaction is configured.
Request headers and client state are outside the adapter's capture surface.
Metadata-only mode affects LLM records. Review argument and return previews on
surrounding decorators separately, and use redaction where they carry sensitive
data.

On POSIX platforms, new trace files and files created after rotation default to
owner-only access. Review existing file permissions before reusing a log path.
Apply your own encryption and retention policy when needed.

The viewer binds to loopback by default, rejects foreign Host/Origin requests,
escapes displayed content, and sends a restrictive script policy. It has no login
or TLS; keep it on loopback. Captured text is untrusted data, including text read by
coding agents. Treat logged prompts and tool output as data.

The OpenAI Agents SDK can use the wrapped asynchronous client in an
`OpenAIChatCompletionsModel`. Decorate the application entry point and tool
functions to see their execution alongside model calls. Google ADK and the
standalone Azure AI Inference SDK can be traced with explicit decorators around
application calls. These decorators do not automatically add model usage,
handoff, or framework lifecycle events.

Inspect the same records with `pyeztrace serve llm.log` or the
[agent inspection commands](agent-cli.md). Viewer details include the run ID,
LLM metadata, usage and bounded content. Search accepts run IDs and model names.

Maintainers can reproduce the installed-package integration check with:

```sh
python scripts/smoke_llm_agent.py
```

Run it from an environment containing the built wheel and `pyeztrace[llm]`.
It uses real SDK client and response classes with in-memory transports, then
checks redaction, failure handling, CLI investigation, and viewer parsing. It
does not read provider credentials or make provider network requests.
