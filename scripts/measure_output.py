"""Reproducible context-volume comparison; no model speed claim.

Run: uv run --with tiktoken scripts/measure_output.py
Uses a deterministic fixture dispatch implementation, with identical tool outputs
for direct and programmatic paths. Output includes the programmatic audit receipts.
"""

import asyncio
import json
import time
from types import SimpleNamespace

import tiktoken
from amplifier_core import ToolResult
from amplifier_module_tool_exec import ProgrammaticTool


class FixtureLease:
    tools = ("read_record",)
    context = {
        "session_id": "measurement",
        "run_id": "comparison",
        "parent_tool_call_id": "parent",
    }
    calls = 0

    def record(self, index):
        return {
            "index": index,
            "value": index * 7,
            "details": "Supplementary record detail that is not needed for the requested sum. "
            * 256,
        }

    async def call(self, name, arguments, *, request_id=None):
        self.calls += 1
        return {
            "status": "completed",
            "success": True,
            "output": self.record(arguments["index"]),
            "source": {
                **self.context,
                "tool_call_id": f"record-{arguments['index']}",
                "tool_name": name,
                "request_id": request_id,
            },
        }

    def close(self):
        pass


async def main():
    lease = FixtureLease()
    code = "const results = await Promise.all(Array.from({length:12}, (_,index)=>tools.read_record({index}))); text({sum:results.reduce((sum,r)=>sum+r.output.value,0)});"
    direct_inputs = json.dumps(
        [{"name": "read_record", "arguments": {"index": index}} for index in range(12)]
    )
    direct_output = "\n".join(
        ToolResult(success=True, output=lease.record(index)).get_serialized_output()
        for index in range(12)
    )
    dispatch = SimpleNamespace(version=1, bind=lambda: lease)
    tool = ProgrammaticTool(
        SimpleNamespace(get_capability=lambda _: dispatch), {"max_calls": 12}
    )
    started = time.monotonic()
    result = await tool.execute({"code": code})
    elapsed = time.monotonic() - started
    assert result.success, result
    assert json.loads(result.output["output"][0]) == {
        "sum": sum(index * 7 for index in range(12))
    }
    assert lease.calls == 12
    programmatic_input = json.dumps({"name": "tool_exec", "arguments": {"code": code}})
    programmatic_output = result.get_serialized_output()
    encoding = tiktoken.get_encoding("cl100k_base")

    def size(text):
        return {
            "utf8_bytes": len(text.encode()),
            "cl100k_base_tokens": len(encoding.encode(text)),
        }

    print(
        json.dumps(
            {
                "fixture": "12 records; exact same record data and computed sum in both paths",
                "underlying_tool_calls_each": 12,
                "selected_result": {"sum": 462},
                "direct_input": size(direct_inputs),
                "direct_output": size(direct_output),
                "programmatic_input": size(programmatic_input),
                "programmatic_output_including_receipts": size(programmatic_output),
                "programmatic_fixture_wall_seconds": round(elapsed, 4),
                "measurement_limits": "No model or network latency measured. Tokenizer is an explicit comparison proxy, not provider billing. Shared tool schemas/system prompts excluded; new tool_exec schema overhead also excluded. Fixture native outputs are deliberately verbose. No universal speedup or savings claim.",
            },
            indent=2,
        )
    )


asyncio.run(main())
