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

## Optional structured media result carrier

When a media-aware host returns refs, `output` must be the actual effective
post-hook `ToolResult.output` mapping, with the direct reserved member
`"experimental.media.v1": {"refs": [SessionBlobRefV1, ...]}`. Explicit null,
replacement without refs and in-place redaction must not fall back to the original
producer output. Never return original refs beside a redacted effective output.
Text containing this spelling is still text, not a carrier.

Tool-exec aggregates only attributed `completed` / `success: true` responses
successfully delivered within the existing result byte limits. It snapshots the
bounded wire data outside JavaScript, preserving ref order, inner provenance and
original expiry. Failed, denied, unknown, oversized and undeliverable responses
cannot contribute refs, even if the script prints success. Later program failure
does not erase refs from completed delivered siblings or change their statuses.

The closed v1 ref has exactly `v`, `session_id`, `sha256`, `mime`, `bytes`, `width`,
`height`, and `provenance`. `v` is integer 1, the owner is the lease session, the
digest is 64 lowercase hex characters, MIME is PNG/JPEG, and size/dimensions are
positive non-boolean integers. Provenance has exactly `kind`, `session_id`, `id`,
`created_at`, `expires_at`; both session IDs use the session-store identifier shape,
kind is tool/upload/host/migration, the origin ID is nonempty, timestamps are
timezone-qualified, and expiry is null or no earlier than creation. Unknown fields
(including pixel payload fields), versions and malformed envelopes are refused,
not silently sanitized. This structural check is not storage authorization.

The host remains responsible for validating staged/accepted identity, policy,
ownership and expiry through its session-bound media capability. In particular,
the final **outer** post-hook/publication path must validate surviving nested
origins without rebinding them to the outer call or extending their expiry; it may
remove refs. Tool-exec adds no media capability imports, storage, resolution,
inference, dispatch authority or checkpoint API. A host must qualify its nested
dispatch and outer publication pairing before enabling media producers. The
module's synthetic lease fixtures alone are not that cross-repository proof.
