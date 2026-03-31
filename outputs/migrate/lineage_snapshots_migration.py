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
    return os.path.join(cloned_root, "Week 4", "outputs", "week4", "lineage_snapshots.jsonl")


def migrate_or_synthesize(
    cloned_root: str, out_path: str, extractions_path: str, min_n: int = 60, seed: int = 13
) -> None:
    rnd = random.Random(seed)
    rows: list[dict[str, Any]] = []

    # Load a sample of doc_ids from canonical week3 extractions (if present).
    doc_ids: list[str] = []
    if os.path.exists(extractions_path):
        with open(extractions_path, "r", encoding="utf-8") as f:
            for line in f:
                if len(doc_ids) >= 200:
                    break
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if isinstance(obj, dict) and isinstance(obj.get("doc_id"), str):
                    doc_ids.append(obj["doc_id"])

    src = _src_path(cloned_root)
    seed_edges: list[dict[str, Any]] = []
    if os.path.exists(src):
        with open(src, "r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                if line_no > 200:
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
                seed_edges.append(
                    {
                        "from_dataset": "week4_seed",
                        "to_dataset": "week4_seed",
                        "edge_type": "TRANSFORMS",
                        "evidence": {"dataset": "week4_lineage_snapshots", "field": "source_datasets"},
                        "raw": obj,
                    }
                )

    # Create multiple snapshots, each with small graph slice.
    excluded_doc_id = doc_ids[0] if doc_ids else None
    for i in range(max(min_n, 1)):
        snapshot_id = stable_uuid_v4(f"week4.snapshot::{i}")
        nodes: list[dict[str, Any]] = []
        edges: list[dict[str, Any]] = []

        # Dataset nodes
        datasets = [
            "week1_intent_records",
            "week2_verdicts",
            "week3_extractions",
            "week4_lineage_snapshots",
            "week5_events",
            "traces_runs",
        ]
        for ds in datasets:
            nodes.append({"node_id": stable_uuid_v4(f"node::{ds}"), "node_type": "dataset", "name": ds, "ref": {}})

        # Document nodes (cover doc_id referential integrity)
        doc_batch: list[str] = []
        if doc_ids:
            for j in range(5):
                cand = doc_ids[(i * 5 + j) % len(doc_ids)]
                if excluded_doc_id and cand == excluded_doc_id:
                    cand = doc_ids[(i * 5 + j + 1) % len(doc_ids)]
                doc_batch.append(cand)
        else:
            for j in range(5):
                doc_batch.append(stable_uuid_v4(f"doc::synth::{(i*5+j)%50}"))
        for did in doc_batch:
            nodes.append(
                {
                    "node_id": stable_uuid_v4(f"node::doc::{did}"),
                    "node_type": "document",
                    "name": "doc",
                    "ref": {"doc_id": did},
                }
            )

        # Edge: week3 -> week4 evidence
        edges.append(
            {
                "from_node_id": stable_uuid_v4("node::week3_extractions"),
                "to_node_id": stable_uuid_v4("node::week4_lineage_snapshots"),
                "edge_type": "DERIVES",
                "from_dataset": "week3_extractions",
                "to_dataset": "week4_lineage_snapshots",
                "evidence": {"dataset": "week3_extractions", "field": "doc_id"},
            }
        )

        # Connect verdicts to intents
        edges.append(
            {
                "from_node_id": stable_uuid_v4("node::week1_intent_records"),
                "to_node_id": stable_uuid_v4("node::week2_verdicts"),
                "edge_type": "EVALUATES",
                "from_dataset": "week1_intent_records",
                "to_dataset": "week2_verdicts",
                "evidence": {"dataset": "week2_verdicts", "field": "intent_id"},
            }
        )

        # Controlled semantic violation: one snapshot corrupts one doc_id reference.
        if i == 0 and nodes and excluded_doc_id:
            nodes[-1]["ref"] = {"doc_id": "00000000-0000-4000-8000-000000000000"}

        rows.append(
            {
                "snapshot_id": snapshot_id,
                "recorded_at": now_utc_iso(),
                "source": {"system": "week4_cartographer", "version": "1.0.0"},
                "nodes": nodes,
                "edges": edges,
                "metadata": {"seed_edges": seed_edges[:3]},
            }
        )

    write_jsonl(out_path, rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cloned-root", default=r"C:\\Users\\Eyor.G\\Documents\\Cloned")
    parser.add_argument("--out", default=os.path.join("outputs", "week4", "lineage_snapshots.jsonl"))
    parser.add_argument("--extractions", default=os.path.join("outputs", "week3", "extractions.jsonl"))
    args = parser.parse_args()
    migrate_or_synthesize(args.cloned_root, args.out, args.extractions)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
