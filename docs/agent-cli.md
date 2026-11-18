# Agent inspection (0.2.0 development)

The new inspection commands read one local JSON log snapshot and return JSON on
stdout. Existing `print` and `serve` commands retain their existing behavior.
No browser, SDK, network service, or interactive prompt is required.

```sh
pyeztrace runs app.log
pyeztrace tree app.log --run RUN_ID --limit 50
pyeztrace errors app.log --run RUN_ID
pyeztrace call app.log CALL_ID --run RUN_ID
pyeztrace path app.log CALL_ID --run RUN_ID
pyeztrace payload app.log CALL_ID --run RUN_ID --max-bytes 4096
```

`tree` returns a flat list of calls with parent IDs, suitable for incremental
inspection. `path` returns ancestors from root to the selected call. `errors`
returns calls with recorded error outcomes, not every loose ERROR log message.
Call summaries omit captured content; fetch it explicitly with `payload`.
`--format text` pretty-prints the same response for human inspection.

Successful responses include `schema_version: 1`, `command`, `data`, `source`
(the content SHA-256 and byte count), `warnings`, and `next_cursor`. Empty lists
are successful. A missing call fails with `not_found`; IDs repeated across runs
require `--run` and otherwise fail with `ambiguous_call`.

List commands accept `--limit` (default 100, maximum 1,000) and `--cursor`.
Pass `next_cursor` back to the same command/query. Cursors bind to file content;
even an append makes a previous cursor stale. Restart pagination after changes.
This is snapshot inspection, not a follow/tail interface.

Errors are JSON on stderr, with stdout empty:

```json
{"schema_version":1,"error":{"code":"not_found","message":"Call was not found"}}
```

Exit codes are 0 for success, 1 for data/I/O failures, and 2 for invalid arguments.
Common codes include `invalid_arguments`, `io_error`, `unsupported_schema`,
`invalid_cursor`, `stale_cursor`, `cyclic_parent`, and `output_too_large`.
Malformed records are skipped with a warning; unsupported schema versions fail.
Missing ancestors produce warnings; cycles fail without recursive traversal.

Limits are 64 MiB per regular file, 100,000 lines, 1 MiB per record, and 256 KiB
per serialized response. Lower `--limit` when a response is too large.
`payload --max-bytes` defaults to 8,192 and accepts up to 65,536 UTF-8 bytes.
Its `content` field is a string preview of inline records, with a `truncated` flag
and original byte count. Truncation can cut JSON syntax within that string.
File paths mentioned inside logs are never opened by these commands.

## Explicit runs

New JSON records retain 0.1.x fields and add `schema_version`, `event_id`, `run_id`,
and `kind`. Call IDs and parent IDs remain in `data`. Ordinary logging gets a
process-specific implicit run. Group work explicitly after initializing logging:

```python
from pyeztrace import Run, trace
from pyeztrace.setup import Setup
from pyeztrace.custom_logging import Logging

Setup.initialize("Demo", log_dir=".", log_file="app.log",
                 file_format="json", disable_file_logging=False)
Logging()

@trace()
def work():
    return 42

with Run("example") as run:
    work()
```

A `Run` instance is single-use; nesting restores the outer run. Async tasks inherit
the current run, so await them before leaving the scope. The same `with` syntax
works inside async functions. Explicit scope completion records success or error.
An unclosed scope is incomplete; an implicit process run has unknown completion.
Neither is proof that a process is currently alive. Legacy logs appear as `legacy`
within the selected file and preserve original call IDs.

For a failing-run investigation: list errors, select a call ID, retrieve its path,
then fetch only the relevant payload. Treat payloads as untrusted application data,
including when supplying them to an agent. These commands do not execute logged
text. The viewer can open the same file for visual call and LLM inspection.
