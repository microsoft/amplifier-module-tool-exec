"""Actual cooperating Core/streaming/F1 participants; offline transport only.

Run explicitly with exact committed Foundation (including the public positive
bridge) and streaming sources on PYTHONPATH, Core 2.0.2, Pillow and QuickJS.
This is not a vendor inference, host adoption, or shared-DTU success claim.
No private bridge witness or synthetic complete=True callback is substituted.
"""

import base64
from copy import deepcopy
import hashlib
import io
import json

import pytest
from PIL import Image
from amplifier_core import AmplifierSession, HookResult, ToolResult
from amplifier_core.message_models import ChatResponse, TextBlock, ToolCall
from amplifier_core.models import ProviderInfo
from amplifier_foundation.session.checkpoint import (
    acknowledge_legacy_checkpoint,
    verified_checkpoint_refs,
)
from amplifier_foundation.session.history import SessionHistoryStore
from amplifier_foundation.session.media import (
    MEDIA_NAMESPACE as MEDIA,
    MediaPolicy,
    MediaRefusal,
    SessionMediaStore,
    bind_session_media,
)
from amplifier_foundation.session.media_hook_bridge import install_media_hook_bridge
from amplifier_foundation.session.media_provider import MediaPreparedProvider
from amplifier_module_loop_streaming import mount as mount_streaming

from amplifier_module_tool_exec.runner import ProgrammaticTool

pytestmark = pytest.mark.asyncio


def refs(value):
    """Only the canonical direct carrier, never tool text."""
    if not isinstance(value, dict):
        return []
    output = value.get("output")
    return output.get(MEDIA, {}).get("refs", []) if isinstance(output, dict) else []


class MemoryContext:
    """Explicit test participant, not a claim about another context module."""

    def __init__(self):
        self.rows = []

    async def add_message(self, message):
        self.rows.append(deepcopy(message))

    async def get_messages(self):
        return deepcopy(self.rows)

    async def get_messages_for_request(self, provider=None):
        return deepcopy(self.rows)


class Images:
    name, description = "read", "Offline PNG/JPEG producer"
    input_schema = {"type": "object", "properties": {"index": {"type": "integer"}}}

    def __init__(self, image_format, *, heavy_reply=False):
        self.calls, self.images = [], []
        self.heavy_reply = heavy_reply
        self.mime = f"image/{'jpeg' if image_format == 'JPEG' else 'png'}"
        for color in ("red", "blue"):
            stream = io.BytesIO()
            Image.new("RGB", (3, 2), color).save(stream, format=image_format)
            self.images.append(stream.getvalue())

    async def execute(self, arguments):
        index = arguments["index"]
        self.calls.append(index)
        return ToolResult(
            success=True, output={
                "index": index,
                **({"records": [{"value": 0}] * 15000} if (
                    self.heavy_reply is True or self.heavy_reply == f"index-{index}"
                ) else {}),
            },
            content=[{"type": "image", "source": {
                "type": "base64", "media_type": self.mime,
                "data": base64.b64encode(self.images[index]).decode(),
            }}],
        )


class OfflineTransport:
    """Actual F1 count/send endpoint, no network or inference implementation."""

    name = default_model = "offline-nested-vision"
    config = {}

    def __init__(self, code):
        self.code = code
        self.counted, self.sent, self.pixel_digests = [], [], []

    def get_info(self):
        return ProviderInfo(
            id=self.name, display_name="Offline nested image boundary",
            credential_env_vars=[], defaults={"model": self.name},
            capabilities=["request_budget:provider_count"],
        )

    def get_media_request_support(self, *, model, request_options=None):
        return {"v": 1, "positions": ["tool"], "mime_types": ["image/png", "image/jpeg"]}

    def request_budget(self, request, *, context_estimate, request_options=None):
        self.counted.append(request)
        return {
            "estimated_input_tokens": 20, "input_limit_tokens": 10000,
            "context_token_budget": 10000,
            "measurement": {"kind": "provider_count", "source": "offline-fixture", "input_tokens": 20},
        }

    async def complete(self, request, **kwargs):
        assert request is self.counted[-1], "count and send must share the private object"
        self.sent.append(request)
        blocks = [
            block for message in request.messages if isinstance(message.content, list)
            for block in message.content if getattr(block, "type", None) == "image"
        ]
        self.pixel_digests.append([
            hashlib.sha256(base64.b64decode(block.source["data"], validate=True)).hexdigest()
            for block in blocks
        ])
        if len(self.sent) == 1:
            return ChatResponse(content=[], tool_calls=[
                ToolCall(id="outer", name="tool_exec", arguments={"code": self.code}),
            ])
        return ChatResponse(content=[TextBlock(text="offline done")])

    def parse_tool_calls(self, response):
        return response.tool_calls or []


class ObservedPreparedProvider(MediaPreparedProvider):
    """Observe only public count/send arguments, never private adapter state."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.canonicals = []
        self.before_send = None

    async def _request_budget(self, request, **kwargs):
        original = request.model_dump_json()
        self.canonicals.append((request, original))
        decision = await super()._request_budget(request, **kwargs)
        assert request.model_dump_json() == original
        return decision

    async def complete(self, request, **kwargs):
        assert any(request is canonical for canonical, _ in self.canonicals)
        if self.before_send and any(
            (message.metadata or {}).get(MEDIA) for message in request.messages
        ):
            self.before_send()
        return await super().complete(request, **kwargs)


class ParticipantLoader:
    """Real module mounts through the public Core loader callback contract."""

    def __init__(self, context, images):
        self.context, self.images = context, images
        self.queue = []

    async def load(self, module_id, config, *, source_hint=None, coordinator=None):
        async def mount(current):
            if module_id == "loop-streaming":
                await mount_streaming(current, config)
            elif module_id == "test-memory-context":
                await current.mount("context", self.context)
            elif module_id == "tool-exec":
                await current.mount("tools", ProgrammaticTool(current), name="tool_exec")
            elif module_id == "test-images":
                await current.mount("tools", self.images, name="read")
            else:
                raise AssertionError(f"Unexpected participating module: {module_id}")
        return mount

    def enqueue_on_session_ready(self, name, callback):
        self.queue.append((name, callback))

    def get_on_session_ready_queue(self):
        return self.queue

    def clear_on_session_ready_queue(self):
        self.queue.clear()


async def execute_graph(tmp_path, *, image_format="PNG", outer="retain", child="completed",
                        exec_config=None, tamper=None):
    context, images = MemoryContext(), Images(
        image_format, heavy_reply=(
            "index-1" if child == "mixed-not-delivered" else child == "not-delivered"
        ),
    )
    code = """
        await tools.read({index:0});
        await tools.read({index:1});
        text("two bounded observations");
    """
    raw = OfflineTransport(code)
    session = AmplifierSession({
        "session": {
            "orchestrator": {"module": "loop-streaming", "config": {
                "programmatic_dispatch": True, "programmatic_allowed_tools": ["read"],
                "min_delay_between_calls_ms": 0, "max_iterations": 3,
            }},
            "context": "test-memory-context",
        },
        "providers": [], "tools": [{"module": "tool-exec"}, {"module": "test-images"}],
    }, session_id="nested-actual-graph")
    coordinator = session.coordinator
    coordinator.loader = ParticipantLoader(context, images)
    bridge = install_media_hook_bridge(coordinator)
    graph = bridge.begin_participating_graph(
        session, owner="tool-exec.offline.actual-participants",
        participants=("loop-streaming", "test-memory-context", "tool-exec",
                      "test-images", "host.offline"),
    )
    store = SessionMediaStore(
        tmp_path / "media", session.session_id,
        policy=MediaPolicy(1024 * 1024, 10000, 8, ttl_seconds=300),
        checkpoint_verifier=verified_checkpoint_refs,
    )
    cap = bind_session_media(coordinator, store=store, enabled=True)
    provider = ObservedPreparedProvider(
        raw, cap, provider_identity={"provider": raw.name, "account": "offline", "generation": 1},
        model=raw.name,
    )
    captured, final, checkpoints = [], [], []
    tampered = []

    def between_count_and_send():
        # Trigger after a successful completed budget through the actual public
        # wrapper's send seam, not inside the raw count callback.
        counted = raw.counted[-1]
        assert len(raw.sent) == 1 and len(raw.counted) >= 2
        blocks = [block for message in counted.messages if isinstance(message.content, list)
                  for block in message.content if getattr(block, "type", None) == "image"]
        assert [hashlib.sha256(base64.b64decode(block.source["data"])).hexdigest()
                for block in blocks] == [hashlib.sha256(data).hexdigest() for data in images.images]
        tampered.append(tamper)
        if tamper == "epoch":
            store.revoke()
        else:
            blocks[0].source["data"] = base64.b64encode(b"uncounted mutation").decode()
    if tamper:
        provider.before_send = between_count_and_send

    def before_policy(event, data):
        captured.append(deepcopy(data))
        return HookResult()

    def policy(event, data):
        if data["tool_name"] == "read":
            outcome = child.removeprefix("mixed-") if (
                not child.startswith("mixed-") or data["tool_input"]["index"] == 1
            ) else "completed"
            if outcome == "failed":
                value = deepcopy(data["result"])
                value["success"] = False
                return HookResult(action="modify", data={"result": value})
            if outcome == "unknown":
                return HookResult(action="modify", data={"result": "unknown child result"})
            return HookResult()
        if outer == "one":
            value = deepcopy(data["result"])
            value["output"][MEDIA]["refs"] = refs(value)[1:]
            return HookResult(action="modify", data={"result": value})
        if outer == "remove":
            value = deepcopy(data["result"])
            value["output"].pop(MEDIA)
            return HookResult(action="modify", data={"result": value})
        if outer == "inplace":
            data["result"]["output"].pop(MEDIA)
        if outer == "null":
            return HookResult(action="modify", data={"result": None})
        if outer.startswith("continue-null"):
            return HookResult(data={"result": None})
        if outer == "string":
            # Deliberately JSON-looking, never a source of media refs.
            return HookResult(action="modify", data={"result": json.dumps(data["result"])})
        if outer == "error":
            raise RuntimeError("offline outer redaction handler failed")
        if outer == "deny":
            return HookResult(action="deny", reason="offline outer denial")
        return HookResult()

    def after_policy(event, data):
        final.append(deepcopy(data))
        if outer.startswith("continue-null"):
            # Continue.data is authoritative even though Core natively chains
            # only Modify. This ordinary echo must never revive pre-null refs.
            return HookResult(action="modify", data=data)
        return HookResult()

    def special_policy(event, data):
        if outer == "continue-null-inject" and data.get("tool_name") != "read":
            return HookResult(action="inject_context", context_injection="offline retained notice")
        return HookResult()

    def pre_policy(event, data):
        return HookResult(action="deny", reason="offline child denial") if (
            data["tool_name"] == "read" and (
                child == "denied" or (child == "mixed-denied" and data["tool_input"]["index"] == 1)
            )
        ) else HookResult()

    async def checkpoint():
        rows = await context.get_messages()
        path = tmp_path / "session"
        SessionHistoryStore(path).save(
            rows, {"session_id": session.session_id}, preserve_system=True,
        )
        saved = SessionHistoryStore(path).load_messages()
        assert saved == rows
        receipt = acknowledge_legacy_checkpoint(
            path, session_id=session.session_id, expected_messages=rows,
        )
        assert list(receipt.iter_refs()) == [
            ref for row in rows for ref in row.get("metadata", {}).get(MEDIA, {}).get("refs", [])
        ]
        store.acknowledge_checkpoint(receipt)
        checkpoints.append(saved)

    try:
        await session.initialize()
        host = graph.participant("host.offline")
        if child.endswith("not-delivered"):
            exec_config = {"memory_bytes": 2 * 1024 * 1024}
        if exec_config:
            # Replace through the declared participant before sealing the graph.
            await host.mount("tools", ProgrammaticTool(host, exec_config), name="tool_exec")
        await host.mount("providers", provider, name="offline")
        host.hooks.register("tool:pre", pre_policy, name="offline-child-admission")
        host.hooks.register("tool:post", before_policy, priority=0, name="offline-capture-input")
        host.hooks.register("tool:post", policy, priority=10, name="offline-effective-policy")
        host.hooks.register("tool:post", special_policy, priority=15, name="offline-special-action")
        host.hooks.register("tool:post", after_policy, priority=20, name="offline-capture-final")
        coordinator.register_capability("session.durable_checkpoint", checkpoint)
        construction = graph.seal()
        assert bridge.validate_receipt(construction)
        loop = coordinator.get("orchestrator")
        async def run():
            return await loop.execute(
                "Inspect the two offline observations", context, {"offline": provider},
                coordinator.get("tools"), coordinator.hooks, coordinator,
            )
        if tamper is not None:
            reason = "media preparation is released" if tamper == "epoch" else "private media projection changed"
            with pytest.raises(MediaRefusal, match=reason):
                await run()
            assert len(raw.sent) == 1 and checkpoints
            assert tampered == [tamper]
            assert bridge.validate_receipt(construction)
            assert images.calls == [0, 1], "tamper refusal cannot replay image tools"
            assert all(request.model_dump_json() == before for request, before in provider.canonicals)
            assert all(request.messages == [] for request in raw.counted)
            assert context.rows[:len(checkpoints[-1])] == checkpoints[-1]
            return
        result = await run()
        assert result == "offline done"
        assert bridge.validate_receipt(construction), "no late graph repair/reseal"
        assert len(raw.sent) == 2 and checkpoints
        assert all(counted is not canonical for counted in raw.counted
                   for canonical, _ in provider.canonicals)
        assert all(request.model_dump_json() == before for request, before in provider.canonicals)
        rows = await context.get_messages()
        selected = [
            ref for row in rows if row["role"] == "tool"
            for ref in row.get("metadata", {}).get(MEDIA, {}).get("refs", [])
        ]
        assert raw.pixel_digests == [[], [ref["sha256"] for ref in selected]]
        for ref in selected:
            assert ref["provenance"]["id"] != "outer"
            assert ref["provenance"]["expires_at"] is not None
            with cap.resolve(ref, purpose="model") as source:
                assert hashlib.sha256(source.read()).hexdigest() == ref["sha256"]
        for ref in [
            ref for event in captured if event["tool_name"] == "read"
            for ref in refs(event["result"])
        ]:
            if ref not in selected:
                with pytest.raises(MediaRefusal):
                    cap.resolve(ref, purpose="model")
        # Same canonical ref set at final append, source-sealed ACK and send.
        assert selected == [
            ref for row in checkpoints[-1] if row["role"] == "tool"
            for ref in row.get("metadata", {}).get(MEDIA, {}).get("refs", [])
        ]
        for image in images.images:
            encoded = base64.b64encode(image).decode()
            assert encoded not in json.dumps(rows)
            for path in tmp_path.rglob("*"):
                if path.is_file() and (tmp_path / "media" / "assets") not in path.parents:
                    assert encoded.encode() not in path.read_bytes()
        assert all(request.messages == [] for request in raw.sent), "private buffers released"
        return {
            "rows": rows, "captured": captured, "final": final, "selected": selected,
            "images": images, "dispatch": loop.approved_dispatch,
        }
    finally:
        provider.close()
        await session.cleanup()
        store.close()


@pytest.mark.parametrize("image_format", ["PNG", "JPEG"])
@pytest.mark.parametrize("outer", [
    "retain", "one", "remove", "null", "string", "inplace", "deny", "error",
    "continue-null-echo", "continue-null-inject",
])
async def test_actual_nested_ack_final_posthook_checkpoint_private_count_send(tmp_path, image_format, outer):
    observed = await execute_graph(tmp_path, image_format=image_format, outer=outer)
    raw_outer = next(event for event in observed["captured"] if event["tool_name"] == "tool_exec")
    returned = raw_outer["result"]["output"]
    inner = [
        ref for event in observed["captured"] if event["tool_name"] == "read"
        for ref in refs(event["result"])
    ]
    assert len(inner) == 2 and observed["images"].calls == [0, 1]
    child_events = [event for event in observed["captured"] if event["tool_name"] == "read"]
    for index, (ref, event) in enumerate(zip(inner, child_events)):
        assert ref["sha256"] == hashlib.sha256(observed["images"].images[index]).hexdigest()
        assert ref["mime"] == observed["images"].mime
        assert (ref["width"], ref["height"]) == (3, 2)
        assert ref["session_id"] == ref["provenance"]["session_id"] == "nested-actual-graph"
        assert ref["provenance"]["kind"] == "tool"
        assert ref["provenance"]["id"] == event["tool_call_id"]
    assert returned[MEDIA]["refs"] == inner
    assert all(call["status"] == "completed" and call["success"] is True
               and call["result_delivery"] == "delivered" for call in returned["calls"])
    assert [call["source"]["tool_call_id"] for call in returned["calls"]] == [
        ref["provenance"]["id"] for ref in inner
    ]
    selected = inner if outer == "retain" else inner[1:] if outer == "one" else []
    assert observed["selected"] == selected
    assert returned["output"] == ["two bounded observations"]
    # Captures are actual native registered callbacks, not another bridge emit.
    assert len(observed["captured"]) == 3
    if outer not in {"deny", "error"}:
        # Core Modify replaces the complete callback data mapping. The outer
        # chain is last after both sequential nested calls; policy may lawfully
        # remove tool_name rather than preserving diagnostic envelope fields.
        final_outer = observed["final"][-1]
        assert refs(final_outer.get("result")) == selected
    # Closing the real parent call revokes even previously returned references.
    assert all(not observed["dispatch"].media_refs({
        "kind": "tool", "session_id": "nested-actual-graph", "id": "outer",
    }, ref) for ref in inner)


@pytest.mark.parametrize("child", ["denied", "failed", "unknown"])
async def test_actual_nested_ineligible_children_never_promote(tmp_path, child):
    observed = await execute_graph(tmp_path, child=child)
    raw_outer = next(event for event in observed["captured"] if event["tool_name"] == "tool_exec")
    output = raw_outer["result"]["output"]
    assert not raw_outer["result"]["success"]
    assert MEDIA not in output and observed["selected"] == []
    assert [call["status"] for call in output["calls"]] == [child, child]
    assert all(call["result_delivery"] == "delivered" for call in output["calls"])
    assert observed["images"].calls == ([] if child == "denied" else [0, 1])


async def test_actual_nested_quickjs_receive_failure_excludes_completed_child(tmp_path):
    observed = await execute_graph(tmp_path, child="not-delivered")
    raw_outer = next(event for event in observed["captured"] if event["tool_name"] == "tool_exec")
    output = raw_outer["result"]["output"]
    assert not raw_outer["result"]["success"]
    assert MEDIA not in output and observed["selected"] == []
    assert output["error"]["code"] == "result_delivery_failed"
    call, = output["calls"]
    assert call["status"] == "completed" and call["success"] is True
    assert call["result_delivery"] == "failed" and observed["images"].calls == [0]


@pytest.mark.parametrize("child", ["failed", "unknown", "denied", "not-delivered"])
async def test_actual_nested_mixed_outcomes_only_promote_successful_acknowledged_child(tmp_path, child):
    observed = await execute_graph(tmp_path, child=f"mixed-{child}")
    raw_outer = next(event for event in observed["captured"] if event["tool_name"] == "tool_exec")
    output = raw_outer["result"]["output"]
    first, second = output["calls"]
    assert not raw_outer["result"]["success"]
    assert first["status"] == "completed" and first["success"] is True
    assert first["result_delivery"] == "delivered"
    assert second["status"] == ("completed" if child == "not-delivered" else child)
    assert second["result_delivery"] == ("failed" if child == "not-delivered" else "delivered")
    selected, = output[MEDIA]["refs"]
    assert observed["selected"] == [selected]
    assert selected["provenance"]["id"] == first["source"]["tool_call_id"]
    assert selected["sha256"] == hashlib.sha256(observed["images"].images[0]).hexdigest()


@pytest.mark.parametrize("tamper", ["epoch", "private"])
async def test_actual_nested_mutation_between_private_count_and_send_refuses(tmp_path, tamper):
    await execute_graph(tmp_path, tamper=tamper)