import asyncio
import time
from types import SimpleNamespace

import pytest

from amplifier_module_tool_exec import ProgrammaticTool
from amplifier_module_tool_exec.runner import Limits


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
