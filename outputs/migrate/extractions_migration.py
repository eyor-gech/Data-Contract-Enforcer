from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import argparse
import json
import random
from typing import Any

from contracts.utils import now_utc_iso, stable_uuid_v4, write_jsonl


def _src_path(cloned_root: str) -> str:
    return os.path.join(cloned_root, "Week 3", ".refinery", "extraction_ledger.jsonl")


def migrate_or_synthesize(cloned_root: str, out_path: str, min_n: int = 200, seed: int = 5) -> None:
    rnd = random.Random(seed)
    src = _src_path(cloned_root)

    rows: list[dict[str, Any]] = []
    if os.path.exists(src):
        with open(src, "r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                if len(rows) >= min_n:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if not isinstance(obj, dict):
                    continue

                doc_key = str(obj.get("document_id") or "unknown-doc")
                page = int(obj.get("page_number") or 1)
                strategy = str(obj.get("strategy_used") or "vision")
                confidence = float(obj.get("confidence_score") or 0.0)
                threshold = float(obj.get("threshold") or 0.7)
                processing = float(obj.get("processing_time") or 0.0)
                cost = float(obj.get("cost_estimate") or 0.0)
                escalated = bool(obj.get("escalation_occurred") or False)
                flagged = bool(obj.get("flagged_for_review") or False)

                doc_id = stable_uuid_v4(f"week3.doc::{doc_key}")
                extraction_id = stable_uuid_v4(f"week3.extraction::{doc_key}:{page}")
                trace_id = stable_uuid_v4(f"week3.trace::{line_no%200}")

                rows.append(
                    {
                        "extraction_id": extraction_id,
                        "trace_id": trace_id,
                        "created_at": now_utc_iso(),
                        "doc_id": doc_id,
                        "doc_key": doc_key,
                        "page_number": page,
                        "strategy": {"name": strategy if strategy in ("vision", "ocr", "hybrid") else "vision"},
                        "confidence": confidence,
                        "threshold": threshold,
                        "processing_time_ms": processing,
                        "cost_usd": cost,
                        "flags": {"escalated": escalated, "flagged_for_review": flagged},
                        "text": None,
                        "labels": [],
                        "metadata": {"source": "week3_migration", "line_no": line_no},
                    }
                )

    # If missing, synthesize minimal.
    while len(rows) < 60:
        i = len(rows)
        doc_key = f"doc_{i%10:03d}"
        page = (i % 15) + 1
        rows.append(
            {
                "extraction_id": stable_uuid_v4(f"week3.extraction::synth::{i}"),
                "trace_id": stable_uuid_v4(f"week3.trace::synth::{i%200}"),
                "created_at": now_utc_iso(),
                "doc_id": stable_uuid_v4(f"week3.doc::synth::{doc_key}"),
                "doc_key": doc_key,
                "page_number": page,
                "strategy": {"name": rnd.choice(["vision", "ocr", "hybrid"])},
                "confidence": max(0.0, min(1.0, rnd.random())),
                "threshold": 0.7,
                "processing_time_ms": float(rnd.randint(20, 20000)),
                "cost_usd": float(round(rnd.random() * 0.02, 6)),
                "flags": {"escalated": rnd.random() < 0.05, "flagged_for_review": rnd.random() < 0.03},
                "text": None,
                "labels": [],
                "metadata": {"source": "week3_synth"},
            }
        )

    # Inject contract violations
    if rows:
        rows[0]["confidence"] = 1.2  # out of range
        # Keep structural validity (UUIDv4) but allow referential violation against lineage.
        rows[1]["doc_id"] = stable_uuid_v4("week3.doc::violation::not_in_lineage")
        rows[2]["page_number"] = 0  # minimum violation

    write_jsonl(out_path, rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cloned-root", default=r"C:\\Users\\Eyor.G\\Documents\\Cloned")
    parser.add_argument("--out", default=os.path.join("outputs", "week3", "extractions.jsonl"))
    args = parser.parse_args()
    migrate_or_synthesize(args.cloned_root, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
