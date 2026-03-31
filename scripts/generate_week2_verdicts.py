from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import random
from typing import Any

from contracts.utils import now_utc_iso, read_jsonl, stable_uuid_v4, write_jsonl


def generate(intents_path: str, out_path: str, n: int, seed: int = 7) -> None:
    rnd = random.Random(seed)
    intents = [r for r in read_jsonl(intents_path) if "_parse_error" not in r]

    rows: list[dict[str, Any]] = []
    for i in range(n):
        intent = intents[i % len(intents)] if intents else {}
        intent_id = str(intent.get("intent_id") or stable_uuid_v4(f"intent::{i}"))
        trace_id = str(intent.get("trace_id") or stable_uuid_v4(f"trace::{i}"))

        file = None
        intent_obj = intent.get("intent")
        if isinstance(intent_obj, dict) and isinstance(intent_obj.get("code_refs"), list) and intent_obj["code_refs"]:
            cref = intent_obj["code_refs"][rnd.randrange(0, len(intent_obj["code_refs"]))]
            if isinstance(cref, dict):
                file = cref.get("file")
        if not isinstance(file, str) or not file:
            file = f"src/module_{(i%5)+1}.py"

        start_line = rnd.randint(1, 200)
        end_line = start_line + rnd.randint(0, 60)
        provider = "openai" if rnd.random() < 0.7 else "anthropic"
        model = "gpt-4.1-mini" if provider == "openai" else "claude-3-5-sonnet"

        correctness = rnd.randint(1, 5)
        safety = rnd.randint(1, 5)
        style = rnd.randint(1, 5)
        weights = {"correctness": 0.5, "safety": 0.3, "style": 0.2}
        weighted = (
            weights["correctness"] * correctness + weights["safety"] * safety + weights["style"] * style
        ) / (weights["correctness"] + weights["safety"] + weights["style"])

        label = "APPROVE" if weighted >= 4.0 else ("NEEDS_WORK" if weighted >= 2.5 else "REJECT")
        confidence = max(0.0, min(1.0, rnd.random() * 0.6 + (weighted / 5.0) * 0.4))

        rows.append(
            {
                "verdict_id": stable_uuid_v4(f"verdict::{i}"),
                "trace_id": trace_id,
                "intent_id": intent_id,
                "created_at": now_utc_iso(),
                "target_ref": {"file": file, "span": {"start_line": start_line, "end_line": end_line}},
                "model": {"provider": provider, "name": model},
                "scores": {
                    "correctness": correctness,
                    "safety": safety,
                    "style": style,
                    "weights": weights,
                    "weighted_score": weighted,
                },
                "verdict": {
                    "label": label,
                    "rationale": f"Auto-verdict for {file} with weighted={weighted:.2f}",
                    "confidence": confidence,
                },
            }
        )

    # Inject contract violations (for testing)
    if rows:
        rows[0]["scores"]["weighted_score"] = rows[0]["scores"]["weighted_score"] + 0.5  # weighted math violation
        rows[1]["scores"]["correctness"] = 7  # out-of-range
        rows[2]["target_ref"]["file"] = "nonexistent/file/path.py"  # week1->week2 mismatch

    write_jsonl(out_path, rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--intents", default=os.path.join("outputs", "week1", "intent_records.jsonl"))
    parser.add_argument("--out", default=os.path.join("outputs", "week2", "verdicts.jsonl"))
    parser.add_argument("--n", type=int, default=80)
    args = parser.parse_args()
    generate(args.intents, args.out, args.n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
