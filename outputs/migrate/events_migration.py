from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import argparse
import json
import random
from typing import Any

from contracts.utils import now_utc_iso, parse_iso8601, stable_uuid_v4, to_iso8601_z, write_jsonl


def _src_path(cloned_root: str) -> str:
    return os.path.join(cloned_root, "Week 5", "outputs", "week5", "events.jsonl")


def _coerce_recorded_at(v: Any) -> str:
    dt = parse_iso8601(v)
    if dt is None:
        return now_utc_iso()
    return to_iso8601_z(dt)


def migrate_or_synthesize(cloned_root: str, out_path: str, min_n: int = 200, seed: int = 17) -> None:
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
                payload = obj.get("payload")
                if not isinstance(payload, dict):
                    payload = {}
                # Explicit coercions (no silent drift): normalize numeric fields.
                if "requested_amount_usd" in payload and isinstance(payload["requested_amount_usd"], str):
                    try:
                        payload["requested_amount_usd"] = float(payload["requested_amount_usd"])
                    except Exception:
                        pass

                rows.append(
                    {
                        "event_id": stable_uuid_v4(f"week5.event::{line_no}"),
                        "trace_id": stable_uuid_v4(f"week5.trace::{line_no%200}"),
                        "global_position": int(obj.get("global_position") or line_no),
                        "stream_id": str(obj.get("stream_id") or "unknown"),
                        "event_type": str(obj.get("event_type") or "UnknownEvent"),
                        "event_version": int(obj.get("event_version") or 1),
                        "payload": payload,
                        "metadata": obj.get("metadata") if isinstance(obj.get("metadata"), dict) else {"generated_by": "unknown"},
                        "recorded_at": _coerce_recorded_at(obj.get("recorded_at")),
                    }
                )

    while len(rows) < 60:
        i = len(rows) + 1
        rows.append(
            {
                "event_id": stable_uuid_v4(f"week5.event::synth::{i}"),
                "trace_id": stable_uuid_v4(f"week5.trace::synth::{i%200}"),
                "global_position": i,
                "stream_id": f"loan-SYNTH-{i%10:04d}",
                "event_type": "ApplicationSubmitted",
                "event_version": 1,
                "payload": {
                    "application_id": f"SYNTH-{i%10:04d}",
                    "applicant_id": f"COMP-{i%99:03d}",
                    "requested_amount_usd": float(rnd.randint(1000, 900000)),
                    "submitted_at": now_utc_iso(),
                },
                "metadata": {"correlation_id": f"corr-SYNTH-{i%10:04d}", "causation_id": None, "generated_by": "week5_synth"},
                "recorded_at": now_utc_iso(),
            }
        )

    # Inject event contract violations
    if len(rows) >= 3:
        # Monotonicity break for same stream
        rows[1]["stream_id"] = rows[2]["stream_id"]
        rows[1]["global_position"] = rows[2]["global_position"] - 10
        # payload schema missing required field for event_type
        rows[2]["event_type"] = "ApplicationSubmitted"
        if isinstance(rows[2].get("payload"), dict):
            rows[2]["payload"].pop("application_id", None)

    write_jsonl(out_path, rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cloned-root", default=r"C:\\Users\\Eyor.G\\Documents\\Cloned")
    parser.add_argument("--out", default=os.path.join("outputs", "week5", "events.jsonl"))
    args = parser.parse_args()
    migrate_or_synthesize(args.cloned_root, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
