"""Disposable QuickJS-NG worker. No application imports or host authority.

Only trusted Python writes terminal frames. The JS bridge accepts two hostile
operations: call and text. The parent revalidates all calls and owns all limits.
"""

import json
import math
import os
import sys

import quickjs

MAX_WIRE_BYTES = 2 * 1024 * 1024

BOOTSTRAP = r"""
(() => {
  const send = _host_send;
  const finish = _host_finish;
  const stringify = JSON.stringify.bind(JSON);
  const parse = JSON.parse.bind(JSON);
  const AsyncFunction = Object.getPrototypeOf(async function(){}).constructor;
  const pending = new Map();
  let nextId = 0;
  const call = (name, args = {}) => new Promise((resolve, reject) => {
    const id = String(++nextId);
    pending.set(id, {resolve, reject});
    send(stringify({type: "call", request_id: id, name, arguments: args}));
  });
  const text = value => send(stringify({type: "text", value: typeof value === "string" ? value : stringify(value)}));
  let tools;
  globalThis.__configure = names => {
    const namesList = parse(names);
    tools = Object.create(null);
    for (const name of namesList) {
      if (!["call", "available"].includes(name)) tools[name] = args => call(name, args);
    }
    tools.call = call;
    tools.available = Object.freeze(namesList);
    Object.freeze(tools);
  };
  globalThis.__reply = encoded => {
    const reply = parse(encoded);
    const item = pending.get(reply.request_id);
    if (!item) return;
    pending.delete(reply.request_id);
    item.resolve(reply.result);
  };
  globalThis.__run = code => {
    Promise.resolve().then(() => new AsyncFunction("tools", "text", code)(tools, text)).then(
      () => finish("completed", ""),
      error => finish("failed", String(error).slice(0, 2000)));
  };
})();
"""


def emit(frame):
    encoded = json.dumps(frame, ensure_ascii=True, separators=(",", ":"))
    if len(encoded) > MAX_WIRE_BYTES:
        raise ValueError("Worker frame exceeds transport limit")
    sys.stdout.write(encoded + "\n")
    sys.stdout.flush()


def main():
    line = sys.stdin.buffer.readline(MAX_WIRE_BYTES + 1)
    if len(line) > MAX_WIRE_BYTES:
        return 2
    request = json.loads(line)
    limits = request["limits"]
    # A separate process is the wall-clock interrupt boundary. Engine limits
    # also bound JS heap and stack even while it is between bridge calls.
    if os.name == "posix":
        import resource

        cpu = math.ceil(limits["timeout_seconds"]) + 2
        resource.setrlimit(resource.RLIMIT_CPU, (cpu, cpu))
        if sys.platform.startswith("linux"):
            # Includes Python, native libraries, and temporary JSON copies.
            address_limit = 512 * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (address_limit, address_limit))
    context = quickjs.Context()
    context.set_memory_limit(limits["memory_bytes"])
    context.set_max_stack_size(limits["stack_bytes"])
    state = {"terminal": None, "fatal": None}
    outstanding = set()

    def bridge(encoded):
        try:
            if (
                not isinstance(encoded, str)
                or len(encoded.encode("utf-8")) > MAX_WIRE_BYTES
            ):
                raise ValueError("Bridge frame exceeds transport limit")
            frame = json.loads(encoded)
            if not isinstance(frame, dict) or frame.get("type") not in {"call", "text"}:
                raise ValueError("Unsupported bridge operation")
            if frame["type"] == "call":
                identity = frame.get("request_id")
                if not isinstance(identity, str) or not identity or len(identity) > 64:
                    raise ValueError("Invalid request identity")
                if identity in outstanding:
                    raise ValueError("Duplicate request identity")
                outstanding.add(identity)
            emit(frame)
        except Exception:
            state["fatal"] = "Invalid or oversized JavaScript bridge frame"
            raise

    def finish(status, error):
        # Completion is not evidence that nested work succeeded. The parent
        # owns every call receipt and rejects completion with pending work.
        state["terminal"] = {
            "type": "done",
            "status": status,
            "error": str(error)[:2000],
        }

    context.add_callable("_host_send", bridge)
    context.add_callable("_host_finish", finish)
    context.eval(BOOTSTRAP)
    configure, reply, run = (
        context.get(name) for name in ("__configure", "__reply", "__run")
    )
    # Removing globals is ergonomic isolation, not an authority boundary:
    # forged bridge requests are still untrusted at both ends of the pipe.
    context.eval(
        "delete globalThis._host_send; delete globalThis._host_finish; delete globalThis.__configure; delete globalThis.__reply; delete globalThis.__run;"
    )
    configure(json.dumps(request["tools"]))
    run(request["code"])
    while True:
        while context.execute_pending_job():
            if state["fatal"]:
                break
        if state["fatal"]:
            emit({"type": "done", "status": "failed", "error": state["fatal"]})
            return 1
        if state["terminal"]:
            if outstanding:
                emit(
                    {
                        "type": "done",
                        "status": "failed",
                        "error": "Unawaited tool calls remain",
                        "code": "unawaited_calls",
                    }
                )
                return 1
            emit(state["terminal"])
            return 0
        line = sys.stdin.buffer.readline(MAX_WIRE_BYTES + 1)
        if not line or len(line) > MAX_WIRE_BYTES:
            return 2
        response = json.loads(line)
        outstanding.discard(response.get("request_id"))
        reply(json.dumps(response))


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        # Syntax/runtime/memory failures are bounded; no Python stack or host
        # environment is exposed to JavaScript or model-facing output.
        emit(
            {
                "type": "done",
                "status": "failed",
                "error": f"JavaScript worker failed: {type(exc).__name__}",
            }
        )
        raise SystemExit(1)
