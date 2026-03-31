from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import argparse
import random
from typing import Any

from contracts.utils import now_utc_iso, parse_iso8601, stable_uuid_v4, to_iso8601_z, write_jsonl


def _src_path(cloned_root: str) -> str:
    return os.path.join(cloned_root, "Week 1", "outputs", "week1", "intent_records.jsonl")


def migrate_or_synthesize(cloned_root: str, out_path: str, min_n: int = 80, seed: int = 3) -> None:
    rnd = random.Random(seed)

    rows: list[dict[str, Any]] = []
    src = _src_path(cloned_root)
    if os.path.exists(src):
        import json

        with open(src, "r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if not isinstance(obj, dict):
                    continue

                ts = obj.get("timestamp")
                dt = parse_iso8601(ts)
                created_at = to_iso8601_z(dt) if dt else now_utc_iso()
                target = obj.get("target")
                tool = obj.get("tool")
                status = obj.get("status")

                intent_type = "PLAN"
                if tool == "read_file":
                    intent_type = "READ"
                elif tool == "write_file":
                    intent_type = "WRITE"
                elif tool == "execute_command":
                    intent_type = "RUN"

                intent_key = str(obj.get("intent_id") or f"INTENT-{line_no:04d}")
                trace_key = str(obj.get("trace_id") or f"TRACE-{line_no:04d}")

                rows.append(
                    {
                        "intent_id": stable_uuid_v4(f"week1.intent::{intent_key}"),
                        "trace_id": stable_uuid_v4(f"week1.trace::{trace_key}"),
                        "created_at": created_at,
                        "actor": {"agent_id": "agent_week1", "agent_role": "orchestrator"},
                        "intent": {
                            "type": intent_type,
                            "description": f"{tool} {target}",
                            "code_refs": [
                                {
                                    "file": target if isinstance(target, str) else "src/unknown.py",
                                    "start_line": 1,
                                    "end_line": 1,
                                }
                            ],
                        },
                        "tool": {"name": tool or "unknown", "args": {"target": target}},
                        "outcome": {
                            "status": "SUCCESS" if status == "SUCCESS" else "FAILED",
                            "error_type": obj.get("error_type"),
                            "error_message": None,
                        },
                        "mutation_class": obj.get("mutation_class") or "UNKNOWN",
                        "metadata": {"source": "week1_migration", "source_intent_id": intent_key, "line_no": line_no},
                    }
                )

    # Ensure minimum size with synthesized realistic additions.
    base_files = [
        "packages/core/src/tools/concurrency_test.ts",
        "packages/core/src/hooks/hook_engine.ts",
        "src/module_1.py",
        "src/module_2.py",
        "src/pipeline/run.py",
    ]
    while len(rows) < min_n:
        i = len(rows)
        tool = rnd.choice(["read_file", "write_file", "execute_command", "plan"])
        target = rnd.choice(base_files)
        intent_type = {"read_file": "READ", "write_file": "WRITE", "execute_command": "RUN", "plan": "PLAN"}[tool]
        status = "SUCCESS" if rnd.random() < 0.88 else "FAILED"
        rows.append(
            {
                "intent_id": stable_uuid_v4(f"week1.intent::synth::{i}"),
                "trace_id": stable_uuid_v4(f"week1.trace::synth::{i%200}"),
                "created_at": now_utc_iso(),
                "actor": {"agent_id": f"agent_{(i%5)+1}", "agent_role": "fde"},
                "intent": {
                    "type": intent_type,
                    "description": f"{tool} {target}",
                    "code_refs": [{"file": target, "start_line": rnd.randint(1, 250), "end_line": rnd.randint(1, 250)}],
                },
                "tool": {"name": tool, "args": {"target": target}},
                "outcome": {
                    "status": status,
                    "error_type": None if status == "SUCCESS" else "PermissionError",
                    "error_message": None if status == "SUCCESS" else "permission denied",
                },
                "mutation_class": rnd.choice(["SAFE_READ", "AST_REFACTOR", "CONFIG_CHANGE"]),
                "metadata": {"source": "week1_synth"},
            }
        )

    write_jsonl(out_path, rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cloned-root", default=r"C:\\Users\\Eyor.G\\Documents\\Cloned")
    parser.add_argument("--out", default=os.path.join("outputs", "week1", "intent_records.jsonl"))
    args = parser.parse_args()
    migrate_or_synthesize(args.cloned_root, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
