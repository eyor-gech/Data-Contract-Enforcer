from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
from typing import Any

from contracts.canonical import canonical_specs
from contracts.runner import run_validation
from contracts.utils import Violation, parse_iso8601, read_jsonl, safe_float, safe_int, safe_mkdir


def _dataset_paths() -> dict[str, str]:
    return {
        "week1_intent_records": os.path.join("outputs", "week1", "intent_records.jsonl"),
        "week2_verdicts": os.path.join("outputs", "week2", "verdicts.jsonl"),
        "week3_extractions": os.path.join("outputs", "week3", "extractions.jsonl"),
        "week4_lineage_snapshots": os.path.join("outputs", "week4", "lineage_snapshots.jsonl"),
        "week5_events": os.path.join("outputs", "week5", "events.jsonl"),
        "traces_runs": os.path.join("outputs", "traces", "runs.jsonl"),
    }


def _get_path(obj: Any, path: str) -> Any:
    cur = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _type_ok(expected: str, v: Any) -> bool:
    if v is None:
        return False
    if expected == "string":
        return isinstance(v, str)
    if expected == "integer":
        return isinstance(v, int) and not isinstance(v, bool)
    if expected == "number":
        return isinstance(v, (int, float)) and not isinstance(v, bool)
    if expected == "boolean":
        return isinstance(v, bool)
    if expected == "object":
        return isinstance(v, dict)
    if expected == "array":
        return isinstance(v, list)
    if expected == "uuid":
        return isinstance(v, str) and len(v) == 36 and v.count("-") == 4
    if expected == "datetime":
        return isinstance(v, str) and parse_iso8601(v) is not None
    return True


def _structural_validate(dataset: str, path: str) -> list[Violation]:
    spec = canonical_specs().get(dataset)
    if spec is None or not os.path.exists(path):
        return []
    vios: list[Violation] = []
    bad = 0
    total = 0
    for r in read_jsonl(path):
        if "_parse_error" in r:
            bad += 1
            continue
        total += 1
        for f in spec.fields:
            if not f.required:
                continue
            v = _get_path(r, f.path)
            if not _type_ok(f.logical_type, v):
                bad += 1
                break
    if total < spec.min_records:
        vios.append(
            Violation(
                vtype="SCHEMA",
                field="<dataset>",
                severity="CRITICAL",
                count=max(0, spec.min_records - total),
                root_cause=dataset,
                lineage_path=[dataset],
            )
        )
    if bad:
        vios.append(
            Violation(
                vtype="SCHEMA",
                field="<required_fields>",
                severity="CRITICAL",
                count=bad,
                root_cause=dataset,
                lineage_path=[dataset],
            )
        )
    return vios


def _cross_dataset_validate(paths: dict[str, str]) -> dict[str, list[Violation]]:
    v: dict[str, list[Violation]] = {k: [] for k in paths.keys()}

    # Week1 -> Week2 (intent code_refs.file must exist in verdict target_ref)
    if os.path.exists(paths["week1_intent_records"]) and os.path.exists(paths["week2_verdicts"]):
        intent_files: set[str] = set()
        for r in read_jsonl(paths["week1_intent_records"]):
            code_refs = _get_path(r, "intent.code_refs")
            if isinstance(code_refs, list):
                for cref in code_refs:
                    if isinstance(cref, dict) and isinstance(cref.get("file"), str):
                        intent_files.add(cref["file"])
        missing = 0
        for r in read_jsonl(paths["week2_verdicts"]):
            f = _get_path(r, "target_ref.file")
            if isinstance(f, str) and f not in intent_files:
                missing += 1
        if missing:
            v["week2_verdicts"].append(
                Violation(
                    vtype="SEMANTIC",
                    field="target_ref.file",
                    severity="CRITICAL",
                    count=missing,
                    root_cause="week1_intent_records",
                    lineage_path=["week1_intent_records", "week2_verdicts"],
                )
            )

    # Week3 -> Week4 (extraction.doc_id appears as lineage node)
    if os.path.exists(paths["week3_extractions"]) and os.path.exists(paths["week4_lineage_snapshots"]):
        lineage_doc_ids: set[str] = set()
        for lr in read_jsonl(paths["week4_lineage_snapshots"]):
            nodes = lr.get("nodes")
            if isinstance(nodes, list):
                for n in nodes:
                    if isinstance(n, dict):
                        ref = n.get("ref")
                        if isinstance(ref, dict) and isinstance(ref.get("doc_id"), str):
                            lineage_doc_ids.add(ref["doc_id"])
        missing = 0
        for r in read_jsonl(paths["week3_extractions"]):
            doc_id = _get_path(r, "doc_id")
            if isinstance(doc_id, str) and doc_id not in lineage_doc_ids:
                missing += 1
        if missing:
            v["week3_extractions"].append(
                Violation(
                    vtype="SEMANTIC",
                    field="doc_id",
                    severity="HIGH",
                    count=missing,
                    root_cause="week4_lineage_snapshots",
                    lineage_path=["week3_extractions", "week4_lineage_snapshots"],
                )
            )

    # Week5 payload schema (basic)
    if os.path.exists(paths["week5_events"]):
        missing_payload = 0
        for r in read_jsonl(paths["week5_events"]):
            et = _get_path(r, "event_type")
            payload = _get_path(r, "payload")
            if not isinstance(et, str) or not isinstance(payload, dict):
                continue
            if et == "ApplicationSubmitted":
                req = ["application_id", "applicant_id", "requested_amount_usd", "submitted_at"]
            elif et == "DocumentUploadRequested":
                req = ["application_id", "required_document_types", "deadline", "requested_by"]
            elif et == "PackageCreated":
                req = ["package_id", "application_id", "required_documents", "created_at"]
            else:
                continue
            if any(k not in payload for k in req):
                missing_payload += 1
        if missing_payload:
            v["week5_events"].append(
                Violation(
                    vtype="SEMANTIC",
                    field="payload",
                    severity="CRITICAL",
                    count=missing_payload,
                    root_cause="week5_events",
                    lineage_path=["week5_events"],
                )
            )

    # Trace token math
    if os.path.exists(paths["traces_runs"]):
        bad = 0
        for r in read_jsonl(paths["traces_runs"]):
            pt = safe_int(_get_path(r, "prompt_tokens"))
            ct = safe_int(_get_path(r, "completion_tokens"))
            tt = safe_int(_get_path(r, "total_tokens"))
            if pt is None or ct is None or tt is None:
                continue
            if tt != pt + ct:
                bad += 1
        if bad:
            v["traces_runs"].append(
                Violation(
                    vtype="SEMANTIC",
                    field="total_tokens",
                    severity="CRITICAL",
                    count=bad,
                    root_cause="traces_runs",
                    lineage_path=["traces_runs"],
                )
            )

    return v


def _write_report(dataset: str, total: int, violations: list[Violation], out_path: str) -> None:
    worst = max([vv.count for vv in violations], default=0)
    failed = min(total, worst)
    status = "PASS" if not violations else "FAIL"
    report = {
        "status": status,
        "violations": [
            {
                "type": vv.vtype,
                "field": vv.field,
                "severity": vv.severity,
                "count": vv.count,
                "root_cause": vv.root_cause,
                "lineage_path": vv.lineage_path,
            }
            for vv in violations
        ],
        "summary": {"total_records": total, "failed_records": failed, "pass_rate": ((total - failed) / total) if total else 0.0},
    }
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)


def main() -> int:
    safe_mkdir("validation_reports")
    paths = _dataset_paths()
    cross = _cross_dataset_validate(paths)

    for dataset, path in paths.items():
        rows = [r for r in read_jsonl(path)] if os.path.exists(path) else []
        total = len([r for r in rows if isinstance(r, dict) and "_parse_error" not in r])
        vios = []
        vios.extend(_structural_validate(dataset, path))
        vios.extend(cross.get(dataset, []))
        _write_report(dataset, total, vios, os.path.join("validation_reports", f"phase0_{dataset}.json"))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
