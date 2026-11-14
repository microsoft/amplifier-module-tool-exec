"""Verify imported candidate bytes against Git objects, then run offline pairing.

Use existing Python 3.13/Core2.0.2/Pillow/pytest/QuickJS dependencies. This command
does not fetch, install, launch DTUs, or edit any source repository.
"""

import argparse
import hashlib
import importlib
import importlib.metadata
import json
from pathlib import Path
import subprocess
import sys
import tempfile


EXPECTED_CASES = {
    *(f"test_actual_nested_ack_final_posthook_checkpoint_private_count_send[{outer}-{image}]"
      for outer in ("retain", "one", "remove", "null", "string", "inplace", "deny", "error",
                    "continue-null-echo", "continue-null-inject")
      for image in ("PNG", "JPEG")),
    *(f"test_actual_nested_ineligible_children_never_promote[{child}]"
      for child in ("denied", "failed", "unknown")),
    "test_actual_nested_quickjs_receive_failure_excludes_completed_child",
    *(f"test_actual_nested_mixed_outcomes_only_promote_successful_acknowledged_child[{child}]"
      for child in ("failed", "unknown", "denied", "not-delivered")),
    *(f"test_actual_nested_mutation_between_private_count_and_send_refuses[{tamper}]"
      for tamper in ("epoch", "private")),
}


def git(repo, *arguments):
    return subprocess.check_output(["git", "-C", str(repo), *arguments])


def verify_source(repo, commit, source, package):
    resolved = git(repo, "rev-parse", f"{commit}^{{commit}}").decode().strip()
    tracked = {}
    for entry in git(repo, "ls-tree", "-rz", resolved, "--", package).split(b"\0"):
        if not entry:
            continue
        header, name = entry.split(b"\t", 1)
        mode, kind, blob = header.decode().split()
        relative = name.decode()
        if kind != "blob" or not relative.endswith(".py"):
            continue
        path = source / relative
        raw = path.read_bytes()
        identity = hashlib.sha1(f"blob {len(raw)}\0".encode() + raw).hexdigest()
        if identity != blob or mode not in {"100644", "100755"}:
            raise ValueError(f"Source does not match exact Git object: {relative}")
        tracked[path.resolve()] = hashlib.sha256(raw).hexdigest()
    if not tracked:
        raise ValueError(f"No Python inputs verified for {package}")
    return {"commit": resolved, "repo": str(repo), "source": str(source),
            "files": {str(path): digest for path, digest in tracked.items()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for part in ("foundation", "streaming", "bridge"):
        parser.add_argument(f"--{part}-repo", type=Path, required=True)
        parser.add_argument(f"--{part}-commit", required=True)
        parser.add_argument(f"--{part}-source", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    arguments = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    test_path = root / "integration/test_nested_media_graph.py"
    qualification_files = {
        str(path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (test_path, Path(__file__).resolve())
    }
    if any(name.startswith(("amplifier_foundation", "amplifier_module_loop_streaming",
                            "amplifier_module_tool_exec")) for name in sys.modules):
        raise ValueError("Run in a fresh process before participant imports")
    # New cache namespace prevents stale or foreign pyc from supplying code
    # while __file__ still points at the verified Python source.
    sys.pycache_prefix = tempfile.mkdtemp(
        prefix="nested-graph-pycache-", dir=arguments.receipt.resolve().parent,
    )
    records = {}
    packages = {
        "foundation": "amplifier_foundation",
        "streaming": "amplifier_module_loop_streaming",
        "bridge": "amplifier_foundation/session/media_hook_bridge.py",
    }
    for name, package in packages.items():
        records[name] = verify_source(
            getattr(arguments, f"{name}_repo").resolve(),
            getattr(arguments, f"{name}_commit"),
            getattr(arguments, f"{name}_source").resolve(), package,
        )
    records["tool_exec"] = verify_source(
        root, "HEAD", root, "amplifier_module_tool_exec",
    )
    # Existing test/runtime environments may contain other installed versions.
    # Exact source roots take precedence; the narrow bridge overlay supplies
    # only that owned module, not unrelated moving Foundation files.
    sys.path[:0] = [
        str(Path(records["foundation"]["source"])),
        str(Path(records["streaming"]["source"])), str(root),
    ]
    import amplifier_foundation.session
    amplifier_foundation.session.__path__.insert(
        0, str(Path(records["bridge"]["source"]) / "amplifier_foundation/session"),
    )
    versions = {
        package: importlib.metadata.version(package)
        for package in ("amplifier-core", "Pillow", "quickjs-ng", "pytest", "pytest-asyncio")
    }
    if versions["amplifier-core"] != "2.0.2" or sys.version_info < (3, 13):
        raise ValueError("The participating candidate requires Python>=3.13/Core2.0.2")

    def verify_imports():
        imports = {}
        for name, module in tuple(sys.modules.items()):
            path = getattr(module, "__file__", None)
            if not path or not name.startswith((
                "amplifier_foundation", "amplifier_module_loop_streaming",
                "amplifier_module_tool_exec",
            )):
                continue
            path = Path(path).resolve()
            owner = (
                "bridge" if name == "amplifier_foundation.session.media_hook_bridge"
                else "foundation" if name.startswith("amplifier_foundation")
                else "streaming" if name.startswith("amplifier_module_loop_streaming")
                else "tool_exec"
            )
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if records[owner]["files"].get(str(path)) != digest:
                raise ValueError(f"Imported bytes do not match selected graph: {name}")
            imports[name] = {"path": str(path), "sha256": digest}
        return imports

    import pytest
    imported_before = verify_imports()
    outcomes = []

    class Outcomes:
        def pytest_collection_modifyitems(self, items):
            actual = [item.nodeid.split("::")[-1] for item in items]
            if set(actual) != EXPECTED_CASES or len(actual) != len(EXPECTED_CASES):
                raise pytest.UsageError("Qualification requires the exact complete nested case set")

        def pytest_runtest_logreport(self, report):
            if report.when == "call" or report.failed or report.skipped:
                outcomes.append({"nodeid": report.nodeid, "phase": report.when,
                                 "outcome": report.outcome})

    worker_versions = json.loads(subprocess.check_output([
        sys.executable, "-I", "-c",
        "import importlib.metadata as m,json,sys;"
        "print(json.dumps({'python':sys.version,'quickjs-ng':m.version('quickjs-ng')}))",
    ]))
    status = int(pytest.main(["-q", str(test_path)], plugins=[Outcomes()]))
    passed = [item["nodeid"].split("::")[-1] for item in outcomes
              if item["phase"] == "call" and item["outcome"] == "passed"]
    if (set(passed) != EXPECTED_CASES or len(passed) != len(EXPECTED_CASES)
            or any(item["outcome"] != "passed" for item in outcomes)):
        status = status or 1
    if qualification_files != {
        path: hashlib.sha256(Path(path).read_bytes()).hexdigest() for path in qualification_files
    }:
        raise ValueError("Qualification source changed during execution")
    receipt = {
        "kind": "offline-actual-participant-graph", "status": "passed" if status == 0 else "failed",
        "pytest_returncode": status, "python": sys.version, "versions": versions,
        "isolated_worker_versions": worker_versions, "outcomes": outcomes,
        "qualification_files": qualification_files, "pycache_prefix": sys.pycache_prefix,
        "sources": records, "imports_before": imported_before,
        "imports_after": verify_imports(),
        "limitations": [
            "Candidate graph, not accepted union or installed DTU",
            "Declared offline context/image/transport participants; no inference or vendor wire",
            "No live host adoption, conversion, merge or release",
        ],
    }
    arguments.receipt.write_text(json.dumps(receipt, indent=2) + "\n")
    return status


if __name__ == "__main__":
    raise SystemExit(main())