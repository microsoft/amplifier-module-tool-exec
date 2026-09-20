# tools.dispatch v1 host contract

This is a duck-typed capability contract. The module depends only on the kernel
and its JavaScript engine; it does not import a particular orchestrator, application,
or peer tool module.

The coordinator capability named `tools.dispatch` must expose:

```python
version = 1

def bind() -> Lease: ...
```

`bind()` must fail unless called by a currently executing, normally authorized
`tool_exec` call. It captures host-owned session/run/parent-call identities and
must never accept replacements for those identities from the program.

The returned lease exposes:

```python
context: dict  # session_id, run_id, parent_tool_call_id
tools: tuple[str, ...]  # reviewed permitted tool names

async def call(name: str, arguments: dict, *, request_id: str | None = None) -> dict: ...
def close() -> None: ...  # idempotent; immediately revokes future execution
async def wait_cancelled() -> object: ...  # optional host cancellation/revocation signal
```

Each call must take the **same ordinary host tool dispatch path**, including
pre-hooks, coordinator permission processing, cancellation registration, tool
execution, post-hooks, and post-hook output modifications. Preserve unknown
outcomes; do not infer success from a returned string. No direct `tool.execute`
shortcut or missing-capability fallback is permitted.

The host generates a fresh child `tool_call_id` and includes it in the ordinary
hook events. Add `parent_tool_call_id` and `dispatch_source` containing
`session_id`, `run_id`, `parent_tool_call_id`, `tool_call_id`, `tool_name`, and
`request_id`. The client-generated `request_id` is correlation data only, never
an authorization identifier. Cancellation should emit attributable evidence with
`outcome: unknown` and `effects: not_rolled_back`; it must not synthesize a successful
post-tool result.

Return exactly one structured outcome after the ordinary post-hook path:

```json
{
  "status": "completed",
  "success": true,
  "output": {"selected": "actual post-hook tool output"},
  "source": {
    "session_id": "host session",
    "run_id": "active run",
    "parent_tool_call_id": "outer call",
    "tool_call_id": "unique child call",
    "tool_name": "read_record",
    "request_id": "1"
  }
}
```

Statuses: `completed` requires confirmed `success: true`; `denied` and `failed`
require `success: false`; `unknown` requires `success: null`. `output` is JSON data
or a string containing the actual post-hook output. A denied response must not
include data from an unauthorized invocation. Cancellation raises
`asyncio.CancelledError`. Completed child effects remain real even if the outer
script later times out or fails.

The lease expires when closed, when its parent call finishes, when its run exits,
or when host activation/ownership becomes invalid. It must reject use from another
parent call, run, or session, including copied asynchronous contexts. Check validity
again **after any pending approval or tool execution-lock wait**, immediately before
starting the actual operation. Already-running tool mutations are not undone by
revocation.

Serialize actual execution per tool unless a host explicitly knows that tool is
safe to run concurrently. Await approval before acquiring an execution lock;
a pending human decision must not monopolize a global lock or block unrelated
approved work. Reject recursion, unsupported delegation/background modes, and
unreviewed tools explicitly. Never silently translate these into a different mode.

The host remains responsible for all authorization and live application validation.
The module treats arbitrary worker bridge calls as hostile and independently bounds
call counts, concurrency, arguments, outputs, execution time, and the JS runtime.

When a host's async runtime does not forward cancellation of a Python awaiter into
the underlying execution, implement `wait_cancelled()`. It must settle on the real
host immediate-cancellation signal or lease/run/activation revocation. The module
races this signal against worker output and its deadline. Do not claim that merely
cancelling an outer awaiter proves underlying work stopped. Preserve ordinary
graceful-cancellation semantics; already-completed results remain completed.
