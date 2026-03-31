from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import random
from datetime import timedelta
from typing import Any

from contracts.utils import now_utc_iso, parse_iso8601, stable_uuid_v4, write_jsonl


def generate(out_path: str, n: int, seed: int = 11) -> None:
    rnd = random.Random(seed)
    rows: list[dict[str, Any]] = []
    for i in range(n):
        start = now_utc_iso()
        latency_ms = rnd.randint(50, 2000)
        start_dt = parse_iso8601(start)
        end_dt = (start_dt + timedelta(milliseconds=latency_ms)) if start_dt else None
        end = (end_dt.isoformat().replace("+00:00", "Z") if end_dt else now_utc_iso())

        pt = rnd.randint(50, 500)
        ct = rnd.randint(10, 400)
        tt = pt + ct
        cost = round(tt * (0.000002 if rnd.random() < 0.7 else 0.000003), 6)

        row = {
            "run_id": stable_uuid_v4(f"run::{i}"),
            "trace_id": stable_uuid_v4(f"trace::{i%200}"),
            "provider": "langsmith",
            "project": "tenx-week7",
            "name": f"agent_step_{i%12}",
            "start_time": start,
            "end_time": end,
            "latency_ms": float(latency_ms),
            "prompt_tokens": pt,
            "completion_tokens": ct,
            "total_tokens": tt,
            "cost_usd": float(cost),
            "status": "success" if rnd.random() < 0.92 else "error",
            "error": None,
            "inputs": {"prompt": "summarize", "context_bytes": rnd.randint(1000, 20000)},
            "outputs": {"completion": "ok", "tool_calls": rnd.randint(0, 3)},
        }
        if row["status"] == "error":
            row["error"] = {"message": "upstream timeout", "type": "TimeoutError"}
        rows.append(row)

    if rows:
        rows[0]["total_tokens"] = rows[0]["total_tokens"] + 5  # token math violation
    write_jsonl(out_path, rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=os.path.join("outputs", "traces", "runs.jsonl"))
    parser.add_argument("--n", type=int, default=120)
    args = parser.parse_args()
    generate(args.out, args.n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
