import asyncio
import copy
import json
import time
from types import SimpleNamespace

import pytest

from amplifier_module_tool_exec import ProgrammaticTool
from amplifier_module_tool_exec.runner import Limits, MEDIA_NAMESPACE


class Lease:
    tools = ("read", "other", "tool_exec", "delegate", "task")
    context = {"session_id": "s1", "run_id": "r1", "parent_tool_call_id": "p1"}

    def __init__(self, handler=None):
        self.handler = handler
        self.calls = []
        self.closed = False
        self.active = self.peak = 0

    async def call(self, name, arguments, *, request_id=None):
        assert not self.closed
        self.calls.append((name, arguments))
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            if self.handler:
                result = await self.handler(name, arguments)
                result["source"] = {
                    **self.context,
                    "tool_call_id": f"child-{len(self.calls)}",
                    "tool_name": name,
                    "request_id": request_id,
                    **result.get("source", {}),
                }
                return result
            return {
                "status": "completed",
                "success": True,
                "output": arguments,
                "source": {
                    **self.context,
                    "tool_call_id": f"child-{len(self.calls)}",
                    "tool_name": name,
                    "request_id": request_id,
                },
            }
        finally:
            self.active -= 1

    def close(self):
        self.closed = True


class Dispatch:
    version = 1

    def __init__(self, lease):
        self.lease = lease

    def bind(self):
        return self.lease


def tool(config=None, lease=None, capability=True):
    lease = lease or Lease()
    coordinator = SimpleNamespace(
        get_capability=lambda _: Dispatch(lease) if capability else None
    )
    return ProgrammaticTool(coordinator, config), lease


async def test_no_dispatch_fails_closed_before_starting_worker(monkeypatch):
    instance, lease = tool(capability=False)

    async def forbidden(*args, **kwargs):
        raise AssertionError("Worker must not start")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", forbidden)
    result = await instance.execute({"code": 'text("hello")'})
    assert not result.success
    assert result.error["code"] == "dispatch_unavailable"
    assert lease.calls == []


async def test_selective_output_and_parent_child_attribution():
    instance, lease = tool()
    result = await instance.execute(
        {
            "code": 'const r=await tools.read({keep:42, noise:"x".repeat(10000)}); text(r.output.keep);'
        }
    )
    assert result.success
    assert result.output["output"] == ["42"]
    assert len(lease.calls) == 1
    receipt = result.output["calls"][0]
    assert receipt["source"]["parent_tool_call_id"] == "p1"
    assert receipt["source"]["tool_call_id"] == "child-1"
    assert receipt["source"]["request_id"] == "1"
    assert "noise" not in str(result.output)
    assert lease.closed


async def test_fresh_context_and_no_node_filesystem_network_or_environment():
    instance, _ = tool()
    first = await instance.execute(
        {
            "code": "globalThis.secret=42; text([typeof process,typeof require,typeof fetch,typeof std,typeof os,typeof console]);"
        }
    )
    assert first.success
    assert first.output["output"] == [
        '["undefined","undefined","undefined","undefined","undefined","undefined"]'
    ]
    second, _ = tool()
    result = await second.execute({"code": "text(typeof secret)"})
    assert result.success
    assert result.output["output"] == ["undefined"]


@pytest.mark.parametrize(
    "code",
    [
        "while(true){}",
        "await Promise.resolve().then(function loop(){return Promise.resolve().then(loop)})",
    ],
)
async def test_infinite_loop_is_killed_without_blocking_parent(code):
    instance, lease = tool({"timeout_seconds": 0.2})
    ticks = 0

    async def heartbeat():
        nonlocal ticks
        for _ in range(5):
            await asyncio.sleep(0.02)
            ticks += 1

    start = time.monotonic()
    result, _ = await asyncio.gather(instance.execute({"code": code}), heartbeat())
    assert not result.success
    assert result.error["code"] in {"timeout", "javascript_error"}
    assert time.monotonic() - start < 2
    assert ticks == 5
    assert lease.closed


async def test_memory_limit_stops_heap_growth():
    instance, _ = tool({"memory_bytes": 2 * 1024 * 1024, "timeout_seconds": 5})
    result = await instance.execute(
        {
            "code": "let x=[]; let limited=false; try {for(let i=0;i<1000;i++) x.push(new Array(10000).fill(i));} catch(e) {limited=true;} x=null; text(limited);"
        }
    )
    assert result.success
    assert result.output["output"] == ["true"]


@pytest.mark.parametrize("name", ["tool_exec", "delegate", "task", "unknown"])
async def test_recursive_delegation_and_unavailable_calls_are_explicitly_blocked(name):
    instance, lease = tool()
    result = await instance.execute({"code": f'await tools.call("{name}", {{}});'})
    assert not result.success
    assert result.error["code"] == "tool_not_allowed"
    assert lease.calls == []


@pytest.mark.parametrize(
    "code,config,error",
    [
        ("await tools.read({async:true})", {}, "invalid_arguments"),
        ('text("x".repeat(50))', {"max_output_bytes": 20}, "output_limit"),
        ("for(let i=0;i<3;i++) text(i)", {"max_output_items": 2}, "output_limit"),
        ("for(let i=0;i<3;i++) await tools.read({i})", {"max_calls": 2}, "call_limit"),
        (
            'await tools.read({x:"a".repeat(1000)})',
            {"max_argument_bytes": 50},
            "argument_limit",
        ),
        (
            'await tools.read({x:"a".repeat(1000)})',
            {"max_result_bytes": 50},
            "result_limit",
        ),
    ],
)
async def test_limits_are_parent_enforced(code, config, error):
    instance, _ = tool(config)
    result = await instance.execute({"code": code})
    assert not result.success
    assert result.output["error"]["code"] == error
    assert result.output["output_bytes"] <= instance.limits.max_output_bytes


async def test_denied_call_cannot_be_rewritten_to_success_by_script():
    async def denied(*_):
        return {
            "status": "denied",
            "success": False,
            "output": "denied",
            "source": Lease.context,
        }

    instance, _ = tool(lease=Lease(denied))
    result = await instance.execute(
        {"code": 'await tools.read({}); text("All succeeded")'}
    )
    assert not result.success
    assert result.output["calls"][0]["status"] == "denied"
    assert result.error["code"] == "nested_call_failed"


async def test_unknown_outcome_stays_unknown():
    async def unknown(*_):
        return {
            "status": "unknown",
            "success": None,
            "output": "unconfirmed",
            "source": Lease.context,
        }

    instance, _ = tool(lease=Lease(unknown))
    result = await instance.execute({"code": "text(await tools.read({}))"})
    assert not result.success
    assert result.output["calls"][0]["success"] is None


async def test_print_overflow_does_not_turn_confirmed_mutation_into_unknown():
    effects = []

    async def mutate(*_):
        effects.append("saved")
        return {"status": "completed", "success": True,
                "output": {"id": "saved-result", "details": "x" * 33000}}

    instance, lease = tool(lease=Lease(mutate))
    result = await instance.execute({"code": "text(await tools.other({}))"})
    assert not result.success and result.error["code"] == "output_limit"
    assert effects == ["saved"] and len(lease.calls) == 1
    assert result.output["output_delivery"] == {"status": "incomplete", "reason": "output_limit"}
    receipt, = result.output["calls"]
    assert receipt["status"] == "completed" and receipt["success"] is True
    assert receipt["result_delivery"] == "delivered"
    assert "do not replay mutations" in result.error["message"]


async def test_serialization_failure_preserves_confirmed_child_outcome():
    async def non_json(*_):
        return {"status": "completed", "success": True, "output": {"value": object()}}

    instance, _ = tool(lease=Lease(non_json))
    result = await instance.execute({"code": "text(await tools.other({}))"})
    assert not result.success and result.error["code"] == "result_delivery_failed"
    receipt, = result.output["calls"]
    assert receipt["status"] == "completed" and receipt["success"] is True
    assert receipt["result_delivery"] == "failed"


async def test_print_overflow_does_not_promote_unknown_mutation():
    async def uncertain(*_):
        return {"status": "unknown", "success": None, "output": "x" * 33000}

    instance, _ = tool(lease=Lease(uncertain))
    result = await instance.execute({"code": "text(await tools.other({}))"})
    assert not result.success and result.error["code"] == "output_limit"
    receipt, = result.output["calls"]
    assert receipt["status"] == "unknown" and receipt["success"] is None


async def test_complete_printed_output_is_distinct_from_denied_call():
    async def denied(*_):
        return {"status": "denied", "success": False, "output": "denied"}

    instance, _ = tool(lease=Lease(denied))
    result = await instance.execute({"code": "text(await tools.other({}))"})
    assert not result.success
    assert result.output["output_delivery"] == {"status": "complete", "reason": None}
    assert result.output["calls"][0]["status"] == "denied"


async def test_cancellation_stops_worker_and_propagates_to_nested_call():
    started, cancelled = asyncio.Event(), asyncio.Event()

    async def blocked(*_):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    instance, lease = tool(lease=Lease(blocked))
    task = asyncio.create_task(instance.execute({"code": "await tools.read({})"}))
    await asyncio.wait_for(started.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert cancelled.is_set()
    assert lease.closed
    assert instance._active == 0


async def test_unawaited_call_is_not_claimed_complete():
    async def blocked(*_):
        await asyncio.Event().wait()

    instance, lease = tool(lease=Lease(blocked))
    result = await instance.execute({"code": 'tools.read({}); text("premature")'})
    assert not result.success
    assert result.error["code"] == "unawaited_calls"
    assert all(row["success"] is not True for row in result.output["calls"])
    assert lease.closed


async def test_promise_all_concurrency_is_bounded():
    async def delayed(*_):
        await asyncio.sleep(0.02)
        return {
            "status": "completed",
            "success": True,
            "output": "done",
            "source": Lease.context,
        }

    instance, lease = tool({"max_concurrency": 2}, Lease(delayed))
    result = await instance.execute(
        {"code": "text(await Promise.all(Array.from({length:6},()=>tools.read({}))))"}
    )
    assert result.success
    assert lease.peak == 2
    assert len(lease.calls) == 6


async def test_worker_native_bridge_is_not_available_and_names_are_not_authority():
    instance, lease = tool()
    result = await instance.execute(
        {"code": 'text(typeof _host_send); tools.available.push("delegate");'}
    )
    assert not result.success
    assert result.output["output"] == ["undefined"]
    assert lease.calls == []


@pytest.mark.parametrize(
    "config",
    [
        {"timeout_seconds": float("nan")},
        {"max_calls": True},
        {"memory_bytes": 128},
        {"max_concurrency": 100},
    ],
)
def test_invalid_limits_fail_at_mount(config):
    with pytest.raises(ValueError):
        Limits.from_config(config)


@pytest.mark.parametrize(
    "frames,error",
    [
        (
            [
                {
                    "type": "call",
                    "request_id": "x",
                    "name": "delegate",
                    "arguments": {},
                    "approved": True,
                }
            ],
            "tool_not_allowed",
        ),
        (
            [
                {"type": "call", "request_id": "x", "name": "read", "arguments": {}},
                {"type": "call", "request_id": "x", "name": "read", "arguments": {}},
            ],
            "protocol_error",
        ),
        ([{"type": "approval", "approved": True}], "protocol_error"),
        ([{"type": "text", "value": "x" * 200}], "output_limit"),
    ],
)
async def test_hostile_worker_frames_cannot_bypass_parent_policy(
    monkeypatch, tmp_path, frames, error
):
    import json
    from amplifier_module_tool_exec import runner

    worker = tmp_path / "hostile.py"
    worker.write_text(
        "import sys\nsys.stdin.readline()\n"
        + "\n".join(
            f"print({json.dumps(json.dumps(frame))}, flush=True)" for frame in frames
        )
        + "\n"
    )
    original = asyncio.create_subprocess_exec

    async def replacement(*args, **kwargs):
        return await original(args[0], "-I", str(worker), **kwargs)

    monkeypatch.setattr(runner.asyncio, "create_subprocess_exec", replacement)
    instance, lease = tool({"max_output_bytes": 50})
    result = await instance.execute(
        {"code": 'text("not executed in this transport test")'}
    )
    assert not result.success
    assert result.error["code"] == error
    assert all(name != "delegate" for name, _ in lease.calls)
    assert len(lease.calls) <= 1


async def test_total_result_budget_is_enforced_across_successful_calls():
    instance, _ = tool({"max_total_result_bytes": 400})
    result = await instance.execute(
        {
            "code": 'await tools.read({value:"x".repeat(100)}); await tools.read({value:"x".repeat(100)})'
        }
    )
    assert not result.success
    assert result.error["code"] == "result_limit"
    assert result.output["nested_result_bytes"] <= 400
    assert result.output["calls"][-1]["result_delivery"] == "omitted_limit"


async def test_noncooperative_nested_cancellation_is_bounded_and_unconfirmed():
    started, release = asyncio.Event(), asyncio.Event()

    async def ignores_cancel(*_):
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()
        return {
            "status": "completed",
            "success": True,
            "output": "late",
            "source": Lease.context,
        }

    instance, lease = tool({"timeout_seconds": 1}, Lease(ignores_cancel))
    try:
        start = time.monotonic()
        result = await instance.execute({"code": "await tools.read({})"})
        assert started.is_set()
        assert time.monotonic() - start < 3
        assert not result.success
        assert result.output["calls"][0]["status"] == "unknown"
        assert result.output["calls"][0]["cancellation"] == "unconfirmed"
        assert result.output["calls"][0]["success"] is None
        assert lease.closed
    finally:
        release.set()
        await asyncio.sleep(0)


async def test_capacity_bound_rejects_extra_execution_before_binding():
    instance, _ = tool({"max_active_runs": 1})
    instance._active = 1
    result = await instance.execute({"code": 'text("extra")'})
    assert not result.success
    assert result.error["code"] == "capacity_exceeded"


@pytest.mark.parametrize(
    "response",
    [
        {"status": "denied", "success": True, "output": "misleading"},
        {"status": "unknown", "success": False, "output": "misleading"},
        {
            "status": "completed",
            "success": True,
            "output": "wrong session",
            "source": {"session_id": "other-session"},
        },
    ],
)
async def test_invalid_host_outcomes_fail_closed(response):
    async def invalid(*_):
        return dict(response)

    instance, _ = tool(lease=Lease(invalid))
    result = await instance.execute({"code": "text(await tools.read({}))"})
    assert not result.success
    assert result.error["code"] == "dispatch_contract"
    assert result.output["calls"][0]["status"] == "unknown"


async def test_host_cancellation_signal_stops_active_nested_work():
    started, host_cancel, stopped = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def blocked(*_):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    lease = Lease(blocked)
    lease.wait_cancelled = host_cancel.wait
    instance, _ = tool(lease=lease)
    task = asyncio.create_task(instance.execute({"code": "await tools.read({})"}))
    await asyncio.wait_for(started.wait(), 2)
    host_cancel.set()
    result = await asyncio.wait_for(task, 2)
    assert not result.success
    assert result.error["code"] == "host_cancelled"
    assert result.output["calls"][0]["status"] == "unknown"
    assert result.output["calls"][0]["cancellation"] == "requested"
    assert stopped.is_set()
    assert lease.closed


def media_ref(digest="a", origin="capture-1"):
    return {
        "v": 1,
        "session_id": "s1",
        "sha256": digest * 64,
        "mime": "image/png",
        "bytes": 300,
        "width": 12,
        "height": 10,
        "provenance": {
            "kind": "tool",
            "session_id": "s1",
            "id": origin,
            "created_at": "2026-01-01T00:00:00+00:00",
            "expires_at": "2026-01-01T00:05:00+00:00",
        },
    }


def media_output(*refs, **extra):
    return {**extra, MEDIA_NAMESPACE: {"refs": list(refs)}}


async def test_media_aggregates_without_printing_and_in_call_order():
    first, second = media_ref(), media_ref("b", "capture-2")
    completed = []

    async def returned(name, _):
        if name == "read":
            await asyncio.sleep(0.05)
        completed.append(name)
        refs = [first, first] if name == "read" else [second]
        return {
            "status": "completed", "success": True,
            "output": media_output(*refs, noise="not copied"),
        }

    instance, lease = tool(lease=Lease(returned))
    result = await instance.execute({
        "code": 'await Promise.all([tools.read({}), tools.other({})]); text("two observations");'
    })
    assert result.success
    assert completed == ["other", "read"]
    assert result.output[MEDIA_NAMESPACE] == {"refs": [first, first, second]}
    assert result.output["output"] == ["two observations"]
    assert "not copied" not in str(result.output)
    assert len(lease.calls) == 2
    assert all(row["result_delivery"] == "delivered" for row in result.output["calls"])


async def test_media_snapshot_cannot_be_rewritten_by_script_or_host_alias():
    ref = media_ref()
    nested = media_output(ref)

    async def returned(*_):
        return {"status": "completed", "success": True, "output": nested}

    instance, _ = tool(lease=Lease(returned))
    result = await instance.execute({
        "code": f'const r=await tools.read({{}}); r.output["{MEDIA_NAMESPACE}"].refs[0].sha256="f".repeat(64);'
    })
    nested[MEDIA_NAMESPACE]["refs"][0]["sha256"] = "e" * 64
    assert result.success
    assert result.output[MEDIA_NAMESPACE]["refs"][0]["sha256"] == "a" * 64


async def test_jpeg_with_original_foreign_provenance_and_no_expiry_is_unchanged():
    ref = media_ref()
    ref["mime"] = "image/jpeg"
    ref["provenance"].update(
        kind="upload", session_id="original-session",
        created_at="2026-01-01T02:00:00+02:00", expires_at=None,
    )

    async def returned(*_):
        return {"status": "completed", "success": True, "output": media_output(ref)}

    instance, _ = tool(lease=Lease(returned))
    result = await instance.execute({"code": "await tools.read({})"})
    assert result.success
    assert result.output[MEDIA_NAMESPACE] == {"refs": [ref]}


@pytest.mark.parametrize("kind", ["string", "nested", "list", "null", "empty"])
async def test_only_direct_structured_effective_output_is_media_source(kind):
    carrier = media_output(media_ref())
    outputs = {
        "string": json.dumps(carrier),
        "nested": {"output": carrier},
        "list": [carrier],
        "null": None,
        "empty": {},
    }

    async def posthook_returned(*_):
        # The lease exposes only the final effective hook value, never the
        # producer's original carrier after a hook replaces/removes it.
        return {"status": "completed", "success": True, "output": outputs[kind]}

    instance, _ = tool(lease=Lease(posthook_returned))
    result = await instance.execute({
        "code": f'await tools.read({{}}); text({json.dumps(carrier)});'
    })
    assert result.success
    assert MEDIA_NAMESPACE not in result.output
    assert MEDIA_NAMESPACE in result.output["output"][0]  # Text stays text.


async def test_effective_hook_subset_is_not_merged_with_original_refs():
    kept = media_ref("b", "kept")

    async def posthook_returned(*_):
        return {"status": "completed", "success": True, "output": media_output(kept)}

    instance, _ = tool(lease=Lease(posthook_returned))
    result = await instance.execute({"code": "await tools.read({})"})
    assert result.success
    assert result.output[MEDIA_NAMESPACE] == {"refs": [kept]}


@pytest.mark.parametrize("status,success", [
    ("denied", False), ("failed", False), ("unknown", None),
])
async def test_ineligible_status_cannot_promote_media_or_be_rewritten(status, success):
    good = media_ref("b", "completed")

    async def returned(name, _):
        return {
            "status": "completed" if name == "read" else status,
            "success": True if name == "read" else success,
            "output": media_output(good if name == "read" else media_ref()),
        }

    instance, _ = tool(lease=Lease(returned))
    result = await instance.execute({
        "code": 'await tools.read({}); await tools.other({}); text("all succeeded");'
    })
    assert not result.success
    assert result.error["code"] == "nested_call_failed"
    assert result.output[MEDIA_NAMESPACE] == {"refs": [good]}
    assert result.output["output_delivery"] == {"status": "complete", "reason": None}
    receipt = result.output["calls"][1]
    assert receipt["status"] == status and receipt["success"] is success
    assert receipt["result_delivery"] == "delivered"


@pytest.mark.parametrize("tail,error", [
    ('text("x".repeat(100))', "output_limit"),
    ('throw new Error("later failure")', "javascript_error"),
])
async def test_later_program_failure_preserves_returned_refs_and_child_status(tail, error):
    ref = media_ref()

    async def returned(*_):
        return {"status": "completed", "success": True, "output": media_output(ref)}

    instance, _ = tool({"max_output_bytes": 10}, Lease(returned))
    result = await instance.execute({"code": f"await tools.read({{}}); {tail}"})
    assert not result.success
    assert result.error["code"] == error
    assert result.output[MEDIA_NAMESPACE] == {"refs": [ref]}
    assert result.output["output_delivery"] == {"status": "incomplete", "reason": error}
    receipt, = result.output["calls"]
    assert receipt["status"] == "completed" and receipt["success"] is True
    assert receipt["result_delivery"] == "delivered"


@pytest.mark.parametrize("limit", ["max_result_bytes", "max_total_result_bytes"])
async def test_media_cannot_bypass_nested_result_byte_limits(limit):
    async def returned(*_):
        return {"status": "completed", "success": True, "output": media_output(media_ref())}

    instance, _ = tool({limit: 100}, Lease(returned))
    result = await instance.execute({"code": "await tools.read({})"})
    assert not result.success and result.error["code"] == "result_limit"
    assert MEDIA_NAMESPACE not in result.output
    receipt, = result.output["calls"]
    assert receipt["status"] == "completed" and receipt["success"] is True
    assert receipt["result_delivery"] == "omitted_limit"
    assert result.output["nested_result_bytes"] == 0


async def test_total_budget_preserves_only_previously_delivered_media():
    ref = media_ref()

    async def returned(name, _):
        return {
            "status": "completed", "success": True,
            "output": media_output(ref if name == "read" else media_ref("b"), noise="x" * 300),
        }

    instance, _ = tool({"max_total_result_bytes": 1500}, Lease(returned))
    result = await instance.execute({"code": "await tools.read({}); await tools.other({})"})
    assert not result.success and result.error["code"] == "result_limit"
    assert result.output[MEDIA_NAMESPACE] == {"refs": [ref]}
    assert [row["result_delivery"] for row in result.output["calls"]] == [
        "delivered", "omitted_limit",
    ]


async def test_failed_worker_delivery_cannot_promote_returned_refs(monkeypatch):
    from amplifier_module_tool_exec import runner

    original = asyncio.create_subprocess_exec

    class BrokenDelivery:
        def __init__(self, stream):
            self.stream = stream
            self.writes = 0

        def write(self, value):
            self.writes += 1
            if self.writes > 1:
                raise BrokenPipeError("synthetic delivery failure")
            self.stream.write(value)

        async def drain(self):
            await self.stream.drain()

    async def replacement(*args, **kwargs):
        process = await original(*args, **kwargs)
        process.stdin = BrokenDelivery(process.stdin)
        return process

    async def returned(*_):
        return {"status": "completed", "success": True, "output": media_output(media_ref())}

    monkeypatch.setattr(runner.asyncio, "create_subprocess_exec", replacement)
    instance, _ = tool(lease=Lease(returned))
    result = await instance.execute({"code": "await tools.read({})"})
    assert not result.success and result.error["code"] == "result_delivery_failed"
    assert MEDIA_NAMESPACE not in result.output
    receipt, = result.output["calls"]
    assert receipt["status"] == "completed" and receipt["success"] is True
    assert receipt["result_delivery"] == "failed"


@pytest.mark.parametrize("terminal", ["failed", "completed", "eof"])
async def test_receive_side_failure_after_pipe_read_does_not_promote_refs(
    monkeypatch, tmp_path, terminal
):
    from amplifier_module_tool_exec import runner

    # The parent's pipe write/drain succeeds, but the receiver cannot ingest the
    # reply into JavaScript. This models parse/heap failure before promise resolve.
    worker = tmp_path / "receive_failure.py"
    worker.write_text(
        "import json, sys\n"
        "json.loads(sys.stdin.readline())\n"
        "print(json.dumps({'type':'call','request_id':'1','name':'read','arguments':{}}), flush=True)\n"
        "json.loads(sys.stdin.readline())\n"
        + (
            f"print(json.dumps({{'type':'done','status':{terminal!r},'error':'receiver parse failed'}}), flush=True)\n"
            if terminal != "eof" else ""
        )
    )
    original = asyncio.create_subprocess_exec

    async def replacement(*args, **kwargs):
        return await original(args[0], "-I", str(worker), **kwargs)

    async def returned(*_):
        return {"status": "completed", "success": True, "output": media_output(media_ref())}

    monkeypatch.setattr(runner.asyncio, "create_subprocess_exec", replacement)
    instance, lease = tool(lease=Lease(returned))
    result = await instance.execute({"code": "await tools.read({})"})
    assert not result.success
    receipt, = result.output["calls"]
    assert receipt["status"] == "completed" and receipt["success"] is True
    assert receipt["result_delivery"] == "failed"
    assert MEDIA_NAMESPACE not in result.output
    assert result.error["code"] == "result_delivery_failed"
    assert len(lease.calls) == 1 and lease.closed


async def test_real_quickjs_receive_heap_failure_never_acknowledges_or_promotes_refs():
    async def returned(*_):
        return {
            "status": "completed", "success": True,
            "output": media_output(media_ref(), records=[{"value": 0}] * 15000),
        }

    instance, lease = tool({"memory_bytes": 2 * 1024 * 1024}, Lease(returned))
    result = await instance.execute({"code": "await tools.read({})"})
    assert not result.success
    assert result.error["code"] == "result_delivery_failed"
    assert "JSException" in result.error.get("message", "") or (
        "acknowledging" in result.error["message"]
    )
    receipt, = result.output["calls"]
    assert receipt["status"] == "completed" and receipt["success"] is True
    assert receipt["result_delivery"] == "failed"
    assert MEDIA_NAMESPACE not in result.output
    assert len(lease.calls) == 1 and lease.closed


@pytest.mark.parametrize("stop", ["timeout", "host_cancelled"])
async def test_missing_ack_on_timeout_or_host_cancel_does_not_promote_refs(
    monkeypatch, tmp_path, stop
):
    from amplifier_module_tool_exec import runner

    worker = tmp_path / "stalled_receive.py"
    worker.write_text(
        "import json, sys, time\n"
        "json.loads(sys.stdin.readline())\n"
        "print(json.dumps({'type':'call','request_id':'1','name':'read','arguments':{}}), flush=True)\n"
        "json.loads(sys.stdin.readline())\n"
        "print(json.dumps({'type':'text','value':'receiver stalled before ingestion'}), flush=True)\n"
        "time.sleep(30)\n"
    )
    original = asyncio.create_subprocess_exec
    received, cancel = asyncio.Event(), asyncio.Event()

    class ObserveRead:
        def __init__(self, stream):
            self.stream = stream

        async def readline(self):
            line = await self.stream.readline()
            if b"receiver stalled" in line:
                received.set()
            return line

    async def replacement(*args, **kwargs):
        process = await original(args[0], "-I", str(worker), **kwargs)
        process.stdout = ObserveRead(process.stdout)
        return process

    async def returned(*_):
        return {"status": "completed", "success": True, "output": media_output(media_ref())}

    monkeypatch.setattr(runner.asyncio, "create_subprocess_exec", replacement)
    lease = Lease(returned)
    lease.wait_cancelled = cancel.wait
    instance, _ = tool({"timeout_seconds": 0.5 if stop == "timeout" else 5}, lease)
    task = asyncio.create_task(instance.execute({"code": "await tools.read({})"}))
    await asyncio.wait_for(received.wait(), 2)
    if stop == "host_cancelled":
        cancel.set()
    result = await asyncio.wait_for(task, 2)
    assert not result.success and result.error["code"] == stop
    receipt, = result.output["calls"]
    assert receipt["status"] == "completed" and receipt["success"] is True
    assert receipt["result_delivery"] == "failed"
    assert MEDIA_NAMESPACE not in result.output
    assert len(lease.calls) == 1 and lease.closed


@pytest.mark.parametrize("ack_id", ["other", None, [], 1])
async def test_acknowledgement_requires_matching_pending_request(monkeypatch, tmp_path, ack_id):
    from amplifier_module_tool_exec import runner

    worker = tmp_path / "invalid_ack.py"
    worker.write_text(
        "import json, sys\n"
        "json.loads(sys.stdin.readline())\n"
        f"print({json.dumps(json.dumps({'type': 'result_received', 'request_id': ack_id}))}, flush=True)\n"
    )
    original = asyncio.create_subprocess_exec

    async def replacement(*args, **kwargs):
        return await original(args[0], "-I", str(worker), **kwargs)

    monkeypatch.setattr(runner.asyncio, "create_subprocess_exec", replacement)
    instance, lease = tool()
    result = await instance.execute({"code": 'text("not run")'})
    assert not result.success and result.error["code"] == "protocol_error"
    assert result.output["calls"] == [] and lease.calls == []
    assert MEDIA_NAMESPACE not in result.output and lease.closed


async def test_duplicate_ack_fails_without_erasing_already_acknowledged_delivery(
    monkeypatch, tmp_path
):
    from amplifier_module_tool_exec import runner

    worker = tmp_path / "duplicate_ack.py"
    worker.write_text(
        "import json, sys\n"
        "json.loads(sys.stdin.readline())\n"
        "print(json.dumps({'type':'call','request_id':'1','name':'read','arguments':{}}), flush=True)\n"
        "json.loads(sys.stdin.readline())\n"
        "for _ in range(2):\n"
        "    print(json.dumps({'type':'result_received','request_id':'1'}), flush=True)\n"
    )
    original = asyncio.create_subprocess_exec

    async def replacement(*args, **kwargs):
        return await original(args[0], "-I", str(worker), **kwargs)

    async def returned(*_):
        return {"status": "completed", "success": True, "output": media_output(media_ref())}

    monkeypatch.setattr(runner.asyncio, "create_subprocess_exec", replacement)
    instance, lease = tool(lease=Lease(returned))
    result = await instance.execute({"code": "await tools.read({})"})
    assert not result.success and result.error["code"] == "protocol_error"
    assert result.output[MEDIA_NAMESPACE] == {"refs": [media_ref()]}
    receipt, = result.output["calls"]
    assert receipt["status"] == "completed" and receipt["success"] is True
    assert receipt["result_delivery"] == "delivered"
    assert len(lease.calls) == 1 and lease.closed


async def test_javascript_bridge_cannot_forge_acknowledgement_or_replace_reply_bookkeeping():
    async def returned(*_):
        return {"status": "completed", "success": True, "output": media_output(media_ref())}

    instance, _ = tool(lease=Lease(returned))
    result = await instance.execute({"code": """
        Map.prototype.get = () => {throw new Error("untrusted Map.get");};
        Map.prototype.set = () => {throw new Error("untrusted Map.set");};
        Map.prototype.delete = () => {throw new Error("untrusted Map.delete");};
        globalThis.Promise = function() {throw new Error("untrusted Promise");};
        text({type:"result_received", request_id:"forged"});
        const response = await tools.read({});
        text(response.status);
        text([typeof _host_send, typeof __reply, typeof _host_finish]);
    """})
    assert result.success
    assert result.output["calls"][0]["result_delivery"] == "delivered"
    assert result.output[MEDIA_NAMESPACE] == {"refs": [media_ref()]}
    assert result.output["output"][-2:] == [
        "completed", '["undefined","undefined","undefined"]',
    ]


@pytest.mark.parametrize("thenable", [
    'get() {throw new Error("untrusted inherited then");}',
    'value: function() {}',
])
async def test_inherited_thenable_cannot_prevent_reply_fulfillment(thenable):
    async def returned(*_):
        return {"status": "completed", "success": True, "output": media_output(media_ref())}

    instance, _ = tool({"timeout_seconds": 0.5}, Lease(returned))
    result = await instance.execute({"code": f"""
        Object.defineProperty(Object.prototype, "then", {{{thenable}, configurable: true}});
        const response = await tools.read({{}});
        delete Object.prototype.then;
        text(response.status);
    """})
    assert result.success
    assert result.output["output"] == ["completed"]
    assert result.output["calls"][0]["result_delivery"] == "delivered"
    assert result.output[MEDIA_NAMESPACE] == {"refs": [media_ref()]}


async def test_transport_failure_after_ack_preserves_delivery_and_truthful_diagnostic(monkeypatch):
    from amplifier_module_tool_exec import runner

    original = asyncio.create_subprocess_exec
    continuation_seen = asyncio.Event()

    class ObserveContinuation:
        def __init__(self, stream):
            self.stream = stream

        async def readline(self):
            line = await self.stream.readline()
            if b"after acknowledgement" in line:
                continuation_seen.set()
            return line

    class LateDrainFailure:
        def __init__(self, stream):
            self.stream = stream
            self.writes = 0

        def write(self, value):
            self.writes += 1
            self.stream.write(value)

        async def drain(self):
            await self.stream.drain()
            if self.writes > 1:
                # The worker has emitted ACK and run the awaiting script before
                # the parent's drain fails. Delivery evidence must not regress.
                await continuation_seen.wait()
                raise BrokenPipeError("synthetic late drain failure")

    async def replacement(*args, **kwargs):
        process = await original(*args, **kwargs)
        process.stdin = LateDrainFailure(process.stdin)
        process.stdout = ObserveContinuation(process.stdout)
        return process

    async def returned(*_):
        return {"status": "completed", "success": True, "output": media_output(media_ref())}

    monkeypatch.setattr(runner.asyncio, "create_subprocess_exec", replacement)
    instance, lease = tool(lease=Lease(returned))
    result = await instance.execute({
        "code": 'await tools.read({}); text("after acknowledgement");'
    })
    assert not result.success and result.error["code"] == "worker_transport_failed"
    assert "after acknowledged" in result.error["message"]
    assert result.output[MEDIA_NAMESPACE] == {"refs": [media_ref()]}
    receipt, = result.output["calls"]
    assert receipt["status"] == "completed" and receipt["success"] is True
    assert receipt["result_delivery"] == "delivered"
    assert len(lease.calls) == 1 and lease.closed


@pytest.mark.parametrize("change", [
    {"data": "PIXEL_SENTINEL"},
    {"v": True}, {"v": 2}, {"bytes": True}, {"height": 0},
    {"sha256": "A" * 64}, {"session_id": "other"}, {"mime": "image/gif"},
    {"provenance": {"base64": "PIXEL_SENTINEL"}},
])
async def test_malformed_refs_fail_delivery_without_changing_confirmed_status(change):
    ref = {**media_ref(), **copy.deepcopy(change)}

    async def returned(*_):
        return {"status": "completed", "success": True, "output": media_output(ref)}

    instance, _ = tool(lease=Lease(returned))
    result = await instance.execute({"code": "await tools.read({})"})
    assert not result.success and result.error["code"] == "invalid_media_result"
    assert MEDIA_NAMESPACE not in result.output
    assert "PIXEL_SENTINEL" not in str(result.output)
    receipt, = result.output["calls"]
    assert receipt["status"] == "completed" and receipt["success"] is True
    assert receipt["result_delivery"] == "failed"


@pytest.mark.parametrize("envelope", [
    None, [], {"refs": None}, {"refs": {}}, {"refs": [], "data": "PIXEL_SENTINEL"},
])
async def test_malformed_reserved_envelope_is_not_silently_ignored(envelope):
    async def returned(*_):
        return {"status": "completed", "success": True, "output": {MEDIA_NAMESPACE: envelope}}

    instance, _ = tool(lease=Lease(returned))
    result = await instance.execute({"code": "await tools.read({})"})
    assert not result.success and result.error["code"] == "invalid_media_result"
    assert MEDIA_NAMESPACE not in result.output
    assert "PIXEL_SENTINEL" not in str(result.output)


@pytest.mark.parametrize("origin_change", [
    {"data": "PIXEL_SENTINEL"}, {"id": {}}, {"session_id": "../other"},
    {"created_at": "2026-01-01T00:00:00"}, {"created_at": True},
    {"expires_at": "2025-01-01T00:00:00Z"},
    {"expires_at": "2026-01-01T00:05:00"},
])
async def test_malformed_provenance_is_not_promoted(origin_change):
    ref = media_ref()
    ref["provenance"].update(origin_change)

    async def returned(*_):
        return {"status": "completed", "success": True, "output": media_output(ref)}

    instance, _ = tool(lease=Lease(returned))
    result = await instance.execute({"code": "await tools.read({})"})
    assert not result.success and result.error["code"] == "invalid_media_result"
    assert MEDIA_NAMESPACE not in result.output
    assert "PIXEL_SENTINEL" not in str(result.output)


async def test_host_cancel_retains_completed_media_but_not_unknown_sibling():
    first_done, second_started, host_cancel = asyncio.Event(), asyncio.Event(), asyncio.Event()
    ref = media_ref()

    async def returned(name, _):
        if name == "read":
            first_done.set()
            return {"status": "completed", "success": True, "output": media_output(ref)}
        second_started.set()
        await asyncio.Event().wait()

    lease = Lease(returned)
    lease.wait_cancelled = host_cancel.wait
    instance, _ = tool(lease=lease)
    task = asyncio.create_task(instance.execute({
        "code": "await tools.read({}); await tools.other({})"
    }))
    await asyncio.wait_for(first_done.wait(), 2)
    await asyncio.wait_for(second_started.wait(), 2)
    host_cancel.set()
    result = await asyncio.wait_for(task, 2)
    assert not result.success and result.error["code"] == "host_cancelled"
    assert result.output[MEDIA_NAMESPACE] == {"refs": [ref]}
    assert result.output["calls"][0]["status"] == "completed"
    assert result.output["calls"][1]["status"] == "unknown"
    assert result.output["calls"][1]["success"] is None
    assert lease.closed
