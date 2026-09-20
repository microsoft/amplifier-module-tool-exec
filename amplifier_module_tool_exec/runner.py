"""Authority and lifecycle remain in the parent, never in the JS worker."""

import asyncio
import json
import sys
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any
from uuid import uuid4

from amplifier_core import ToolResult

MAX_WIRE_BYTES = 2 * 1024 * 1024
FORBIDDEN_TOOLS = {"tool_exec", "delegate", "task", "live_job"}


@dataclass(frozen=True)
class Limits:
    max_code_bytes: int = 65536
    max_calls: int = 16
    max_argument_bytes: int = 65536
    max_output_bytes: int = 32768
    max_output_items: int = 128
    max_result_bytes: int = 262144
    max_total_result_bytes: int = 1048576
    max_concurrency: int = 4
    max_active_runs: int = 2
    timeout_seconds: float = 30
    memory_bytes: int = 32 * 1024 * 1024
    stack_bytes: int = 512 * 1024

    @classmethod
    def from_config(cls, config):
        values = {
            field.name: config[field.name]
            for field in fields(cls)
            if field.name in config
        }
        limits = cls(**values)
        maxima = {
            "max_code_bytes": 262144,
            "max_calls": 128,
            "max_argument_bytes": 262144,
            "max_output_bytes": 1048576,
            "max_output_items": 1024,
            "max_result_bytes": 1048576,
            "max_total_result_bytes": 16 * 1048576,
            "max_concurrency": 8,
            "max_active_runs": 8,
            "timeout_seconds": 300,
            "memory_bytes": 128 * 1048576,
            "stack_bytes": 2 * 1048576,
        }
        for name, maximum in maxima.items():
            value = getattr(limits, name)
            types = (int, float) if name == "timeout_seconds" else (int,)
            if (
                isinstance(value, bool)
                or not isinstance(value, types)
                or not 0 < value <= maximum
            ):
                raise ValueError(f"{name} must be positive and at most {maximum}")
        if limits.memory_bytes < 1024 * 1024 or limits.stack_bytes < 64 * 1024:
            raise ValueError(
                "memory_bytes must be at least 1 MiB and stack_bytes at least 64 KiB"
            )
        return limits


class ExecutionFailure(Exception):
    def __init__(self, code, message):
        self.code, self.message = code, message
        super().__init__(message)


class ProgrammaticTool:
    name = "tool_exec"
    description = (
        "Run bounded JavaScript over approved tools. Use await tools.NAME(arguments) "
        "or tools.call(name, arguments), Promise.all for independent calls, and text(value) "
        "to return selected output. tools.available lists names. Each call returns "
        "{status, success, output, source}. No Node, filesystem, network, or persistent state. "
        "Nested calls keep normal approvals. Await every call; failed/denied calls remain "
        "visible in authoritative receipts. output retains the delegated tool's structured "
        "type: inspect its fields rather than assuming it is a string. Calls can succeed "
        "before later JavaScript fails; inspect receipts before retrying. "
        "Delegation, background work, and recursion are unsupported."
    )

    def __init__(self, coordinator, config=None):
        self.coordinator = coordinator
        self.limits = Limits.from_config(config or {})
        self._active = 0

    @property
    def input_schema(self):
        return {
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "JavaScript body; top-level await is supported",
                }
            },
            "required": ["code"],
            "additionalProperties": False,
        }

    async def execute(self, input: dict[str, Any]):
        code = input.get("code")
        if (
            not isinstance(code, str)
            or not code.strip()
            or len(code.encode("utf-8")) > self.limits.max_code_bytes
        ):
            return self._error(
                "invalid_input",
                "code must be a non-empty string within the configured byte limit",
            )
        dispatch = self.coordinator.get_capability("tools.dispatch")
        if (
            dispatch is None
            or getattr(dispatch, "version", None) != 1
            or not callable(getattr(dispatch, "bind", None))
        ):
            return self._error(
                "dispatch_unavailable", "Host-approved tools.dispatch v1 is required"
            )
        if self._active >= self.limits.max_active_runs:
            return self._error(
                "capacity_exceeded", "Programmatic execution capacity is full"
            )
        try:
            lease = dispatch.bind()
        except Exception:
            return self._error(
                "dispatch_unavailable", "No active authorized parent tool call"
            )
        self._active += 1
        try:
            return await self._run(code, lease)
        finally:
            self._active -= 1
            lease.close()

    @staticmethod
    def _error(code, message):
        return ToolResult(
            success=False,
            output={"status": "failed", "error": {"code": code, "message": message}},
            error={"code": code, "message": message},
        )

    async def _run(self, code, lease):
        limits = self.limits
        execution_id = str(uuid4())
        available = sorted(set(lease.tools) - FORBIDDEN_TOOLS)
        process = None
        cancellation = None
        calls = {}
        receipts = []
        output = []
        output_bytes = result_bytes = 0
        fatal = asyncio.get_running_loop().create_future()
        lock = asyncio.Lock()
        slots = asyncio.Semaphore(limits.max_concurrency)
        finished = False
        failure = None
        seen_ids = set()
        closed = False

        async def send(frame):
            encoded = (
                json.dumps(frame, ensure_ascii=True, separators=(",", ":")).encode()
                + b"\n"
            )
            if len(encoded) > MAX_WIRE_BYTES:
                raise ExecutionFailure(
                    "result_limit", "Nested tool result exceeds the transport limit"
                )
            async with lock:
                process.stdin.write(encoded)
                await process.stdin.drain()

        async def invoke(frame, receipt):
            nonlocal result_bytes
            try:
                async with slots:
                    receipt["status"] = "dispatching"
                    response = await lease.call(
                        frame["name"],
                        frame["arguments"],
                        request_id=frame["request_id"],
                    )
                    if closed:
                        raise asyncio.CancelledError
                    if not isinstance(response, dict) or response.get("status") not in {
                        "completed",
                        "denied",
                        "failed",
                        "unknown",
                    }:
                        raise ExecutionFailure(
                            "dispatch_contract",
                            "Host returned an unsupported dispatch outcome",
                        )
                    expected = {
                        "completed": True,
                        "denied": False,
                        "failed": False,
                        "unknown": None,
                    }
                    if response.get("success") is not expected[response["status"]]:
                        raise ExecutionFailure(
                            "dispatch_contract",
                            "Host outcome did not match its confirmed success state",
                        )
                    source = response.get("source")
                    if (
                        not isinstance(source, dict)
                        or any(
                            source.get(key) != value
                            for key, value in lease.context.items()
                        )
                        or source.get("tool_name") != frame["name"]
                        or source.get("request_id") != frame["request_id"]
                        or not isinstance(source.get("tool_call_id"), str)
                        or not 0 < len(source["tool_call_id"]) <= 128
                    ):
                        raise ExecutionFailure(
                            "dispatch_contract",
                            "Host result lacks matching call attribution",
                        )
                    encoded = json.dumps(response, ensure_ascii=False).encode("utf-8")
                    size = len(encoded)
                    receipt.update(
                        {
                            "status": response["status"],
                            "success": response.get("success"),
                            "source": response.get("source"),
                            "result_bytes": size,
                        }
                    )
                    if (
                        size > limits.max_result_bytes
                        or result_bytes + size > limits.max_total_result_bytes
                    ):
                        receipt["result_delivery"] = "omitted_limit"
                        raise ExecutionFailure(
                            "result_limit",
                            "Nested result exceeds configured limits; narrow the tool request",
                        )
                    result_bytes += size
                    await send({"request_id": frame["request_id"], "result": response})
            except asyncio.CancelledError:
                # The underlying tool may have already made effects. Do not
                # relabel interruption as denied, success, or rolled back.
                if receipt["status"] in {"queued", "dispatching"}:
                    receipt.update(
                        status="unknown",
                        success=None,
                        cancellation="requested",
                        effects="not_rolled_back",
                    )
                raise
            except ExecutionFailure as exc:
                if receipt["status"] in {"queued", "dispatching"}:
                    receipt.update(
                        status="unknown", success=None, effects="not_rolled_back"
                    )
                if not fatal.done():
                    fatal.set_result(exc)
            except Exception:
                receipt.update(
                    status="unknown", success=None, effects="not_rolled_back"
                )
                if not fatal.done():
                    fatal.set_result(
                        ExecutionFailure(
                            "dispatch_failed",
                            "Nested dispatch failed without a confirmed outcome",
                        )
                    )

        async def consume():
            nonlocal output_bytes, finished
            while True:
                read = asyncio.create_task(process.stdout.readline())
                try:
                    waiters = {read, fatal}
                    if cancellation is not None:
                        waiters.add(cancellation)
                    done, _ = await asyncio.wait(
                        waiters, return_when=asyncio.FIRST_COMPLETED
                    )
                    if cancellation is not None and cancellation in done:
                        cancellation.result()
                        raise ExecutionFailure(
                            "host_cancelled",
                            "Host cancelled execution; nested effects are not rolled back",
                        )
                    if fatal in done:
                        raise fatal.result()
                    line = read.result()
                finally:
                    if not read.done():
                        read.cancel()
                        await asyncio.gather(read, return_exceptions=True)
                if not line:
                    raise ExecutionFailure(
                        "worker_failed",
                        "JavaScript worker exited without a complete result",
                    )
                if len(line) > MAX_WIRE_BYTES:
                    raise ExecutionFailure(
                        "protocol_error", "Worker frame exceeds transport limit"
                    )
                try:
                    frame = json.loads(line)
                except (ValueError, UnicodeError):
                    raise ExecutionFailure("protocol_error", "Invalid worker frame")
                if not isinstance(frame, dict):
                    raise ExecutionFailure("protocol_error", "Invalid worker frame")
                kind = frame.get("type")
                if kind == "call":
                    identity, name, arguments = (
                        frame.get("request_id"),
                        frame.get("name"),
                        frame.get("arguments"),
                    )
                    if (
                        not isinstance(identity, str)
                        or not identity
                        or len(identity) > 64
                        or identity in seen_ids
                    ):
                        raise ExecutionFailure(
                            "protocol_error", "Invalid or reused request identity"
                        )
                    seen_ids.add(identity)
                    if len(receipts) >= limits.max_calls:
                        raise ExecutionFailure(
                            "call_limit", "Maximum nested tool calls exceeded"
                        )
                    if (
                        not isinstance(name, str)
                        or name not in available
                        or name in FORBIDDEN_TOOLS
                    ):
                        raise ExecutionFailure(
                            "tool_not_allowed",
                            "Requested tool is unavailable for programmatic execution",
                        )
                    if (
                        not isinstance(arguments, dict)
                        or arguments.get("async") is True
                    ):
                        raise ExecutionFailure(
                            "invalid_arguments",
                            "Tool arguments must be an object; background execution is unsupported",
                        )
                    if len(json.dumps(arguments).encode()) > limits.max_argument_bytes:
                        raise ExecutionFailure(
                            "argument_limit",
                            "Nested tool arguments exceed configured limits",
                        )
                    receipt = {
                        "request_id": identity,
                        "name": name,
                        "status": "queued",
                        "success": None,
                        "source": {**lease.context, "request_id": identity},
                    }
                    receipts.append(receipt)
                    calls[identity] = asyncio.create_task(invoke(frame, receipt))
                elif kind == "text":
                    value = frame.get("value")
                    if not isinstance(value, str):
                        raise ExecutionFailure(
                            "protocol_error", "text output must serialize to a string"
                        )
                    output_bytes += len(value.encode("utf-8"))
                    if (
                        output_bytes > limits.max_output_bytes
                        or len(output) >= limits.max_output_items
                    ):
                        raise ExecutionFailure(
                            "output_limit", "Selected output exceeds configured limits"
                        )
                    output.append(value)
                elif kind == "done":
                    # A hostile bridge cannot bypass authority by claiming
                    # completion while a real request remains outstanding.
                    if any(
                        receipt["status"] in {"queued", "dispatching"}
                        for receipt in receipts
                    ):
                        raise ExecutionFailure(
                            "unawaited_calls",
                            "Program returned while nested calls were still active",
                        )
                    if calls:
                        await asyncio.gather(*calls.values(), return_exceptions=True)
                    if fatal.done():
                        raise fatal.result()
                    if frame.get("status") != "completed":
                        raise ExecutionFailure(
                            frame.get("code", "javascript_error"),
                            str(frame.get("error", "JavaScript failed"))[:2000],
                        )
                    finished = True
                    return
                else:
                    raise ExecutionFailure(
                        "protocol_error", "Unsupported worker message"
                    )

        try:
            async with asyncio.timeout(limits.timeout_seconds):
                if callable(getattr(lease, "wait_cancelled", None)):
                    cancellation = asyncio.create_task(lease.wait_cancelled())
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-I",
                    str(Path(__file__).with_name("_worker.py")),
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                    limit=MAX_WIRE_BYTES,
                    env={"LANG": "C.UTF-8"},
                )
                await send({"code": code, "tools": available, "limits": asdict(limits)})
                await consume()
        except TimeoutError:
            failure = ExecutionFailure(
                "timeout", "Programmatic execution reached its wall-clock deadline"
            )
        except asyncio.CancelledError:
            raise
        except ExecutionFailure as exc:
            failure = exc
        except Exception:
            failure = ExecutionFailure("worker_failed", "Programmatic worker failed")
        finally:
            closed = True
            if cancellation is not None:
                cancellation.cancel()
                await asyncio.gather(cancellation, return_exceptions=True)
            for call in calls.values():
                if not call.done():
                    call.cancel()
            # Cooperative dispatch cancellation is required by the host lease.
            # Do not abandon cleanup or allow an expired lease to initiate work.
            lease.close()
            if process is not None:
                if process.returncode is None:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                await process.wait()
            if calls:
                _, pending = await asyncio.wait(calls.values(), timeout=1)
                for identity, call in calls.items():
                    if call in pending:
                        for receipt in receipts:
                            if receipt["request_id"] == identity:
                                receipt.update(
                                    status="unknown",
                                    cancellation="unconfirmed",
                                    success=None,
                                    effects="not_rolled_back",
                                )
                        # A non-cooperative host tool cannot be forcibly killed
                        # here. Revoke authority, retain an honest receipt, and
                        # consume its eventual exception without blocking forever.
                        call.add_done_callback(
                            lambda done: None if done.cancelled() else done.exception()
                        )
                    elif not call.cancelled():
                        call.exception()
        success = (
            finished
            and failure is None
            and all(receipt["success"] is True for receipt in receipts)
        )
        result = {
            "execution_id": execution_id,
            "source": lease.context,
            "status": "completed" if success else "failed",
            "output": output,
            "calls": receipts,
            "output_bytes": sum(len(item.encode()) for item in output),
            "nested_result_bytes": result_bytes,
            "effects": "not_rolled_back",
            "limits": asdict(limits),
        }
        if not success:
            failure = failure or ExecutionFailure(
                "nested_call_failed", "One or more nested calls did not confirm success"
            )
            result["error"] = {"code": failure.code, "message": failure.message}
            return ToolResult(success=False, output=result, error=result["error"])
        return ToolResult(success=True, output=result)


async def mount(coordinator, config=None):
    tool = ProgrammaticTool(coordinator, config)
    await coordinator.mount("tools", tool, name=tool.name)
