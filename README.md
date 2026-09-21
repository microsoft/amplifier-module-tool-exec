# Amplifier programmatic tool orchestration

`tool-exec` mounts the `tool_exec` tool. It runs a fresh, bounded JavaScript program
that calls already-mounted tools through the host's **approved dispatch lease**.
Only explicit `text()` output and an authoritative call receipt list return to the
model. This is useful when a task needs selected fields from several large tool
results.

```javascript
const results = await Promise.all([
  tools.call("read_record", {id: "a"}),
  tools.call("read_record", {id: "b"})
]);
text(results.map(result => result.output.title));
```

`tools.NAME(arguments)` is equivalent to `tools.call(name, arguments)`;
`tools.available` lists permitted names. Each call resolves to
`{status, success, output, source}`. Await every call. The program runs as an async
function body and its return value is ignored; call `text(value)` to publish a
string or JSON value. Each invocation starts with fresh state.

The host must implement [`tools.dispatch` version 1](docs/dispatch-contract.md).
Without an authorized active-parent lease the tool fails closed. There is no
fallback that calls a mounted tool's `execute()` directly. Every nested operation
retains the host's ordinary pre/post hooks, approval process, cancellation tracking,
and tool identity. Denied, failed, or unknown calls make the overall ToolResult a
failure even when the script prints success-looking text or catches an exception.
Printed content is selected by the program; receipts are produced by the host.

## Runtime boundary

The engine is the [`quickjs-ng` Python distribution](https://github.com/genotrance/quickjs-ng)
(import name `quickjs`), running in a disposable Python process with a fresh runtime,
heap and stack limits. JavaScript receives no Node, filesystem, network, Python,
process-environment, console, or persistent-state APIs. **This describes JavaScript
ambient globals, not the authority of delegated tools:** an approved tool can
perform its normal filesystem, network, or other actions. Those actions are not
rolled back if the program is interrupted.

The parent owns the wall-clock deadline and kills the JS worker on timeout or
cancellation. It validates every RPC request, tool name, argument shape, byte/call
budget, and outcome. Hiding bridge globals is not the security boundary. A native
engine vulnerability is outside this language-level isolation; this is not an OS
sandbox for hostile native code. The engine heap/stack limits apply on supported
platforms; POSIX CPU limits add protection, and Linux workers also have a 512 MiB
address-space ceiling. These limits do not bound memory already used inside a host
tool before its result crosses the bridge.

Recursive `tool_exec`, recognized delegation tools (`delegate`, `task`, `live_job`),
and background `async: true` calls are explicitly unsupported in this first
version. Hosts should use a reviewed allowlist when other mounted tools expose
long-lived execution or delegation under different names.

## Configuration and results

All limits are host configuration, not script arguments. The wall deadline includes
approval waits; hosts can raise it within the documented maximum of 300 seconds
when human review requires more time. Defaults:

| Setting | Default |
| --- | ---: |
| `max_code_bytes` | 65,536 |
| `max_calls` | 16 |
| `max_argument_bytes` | 65,536 |
| `max_output_bytes` | 32,768 |
| `max_output_items` | 128 |
| `max_result_bytes` | 262,144 |
| `max_total_result_bytes` | 1,048,576 |
| `max_concurrency` | 4 |
| `max_active_runs` | 2 |
| `timeout_seconds` | 30 |
| `memory_bytes` | 33,554,432 |
| `stack_bytes` | 524,288 |

`max_concurrency` bounds in-flight host calls; the host may serialize individual
tools after approval. Result limits apply to the complete dispatch envelope before
it enters JavaScript. An oversized result is omitted and the program fails with a
receipt saying the tool completed but its result was not delivered; it is never
silently clipped into misleading data. Output and call budgets include failed
attempts that reached dispatch. Invalid/unsupported attempts fail before dispatch.

Successful output contains `execution_id`, parent `source`, `status`, selected
`output` strings, `calls` receipts, byte counts, and applied `limits`. Failure output
also includes a stable `error.code` and message. Examples include
`dispatch_unavailable`, `tool_not_allowed`, `invalid_arguments`, `call_limit`,
`argument_limit`, `result_limit`, `output_limit`, `unawaited_calls`, `timeout`,
`javascript_error`, and `nested_call_failed`.

Caller cancellation delivered to this tool propagates. An optional host lease
`wait_cancelled()` signal also stops execution when the real host immediate-cancel
token or lease ownership changes; cancellation of only a foreign-runtime awaiter
is not evidence that underlying work stopped. Timeout/error cleanup requests nested cancellation,
revokes the lease, and gives cooperative calls up to one second to settle. A tool
that suppresses cancellation may continue under its existing host authority;
its receipt stays `status: unknown`, `success: null`, `cancellation: unconfirmed`,
`effects: not_rolled_back`. This module cannot safely claim that an external mutation
stopped or was reversed. Partial selected output and completed call receipts remain
visible on program failure. No operation is replayed automatically.

## Development

```sh
uv sync --frozen --group dev
uv run --frozen --no-sync pytest -q
uv run --frozen --no-sync ruff check --isolated --select E4,E7,E9,F .
uv run --with tiktoken scripts/measure_output.py
```

The comparison uses twelve verbose fixture records and computes the same sum from
the same tool data in both paths. It reports UTF-8 bytes and `cl100k_base` tokens for
direct input/output and the programmatic input/output including all audit receipts.
It does not measure model latency, provider billing, universal savings, or a model
speedup. Shared schemas/prompts and the added tool schema overhead are excluded.

This module's tests cover isolation, adversarial transport frames, resource bounds,
unknown/denied outcomes, and cancellation. They do not establish any application's
UI acceptance or live model behavior. Hosts must separately prove lease expiration,
real approvals, nested attribution, and cancellation through their actual dispatch
implementation before enabling it for users.
## Contributing

> [!NOTE]
> This project is not currently accepting external contributions, but we're actively working toward opening this up. We value community input and look forward to collaborating in the future. For now, feel free to fork and experiment!

Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit [Contributor License Agreements](https://cla.opensource.microsoft.com).

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft
trademarks or logos is subject to and must follow
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
