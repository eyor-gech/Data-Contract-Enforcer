from __future__ import annotations

import os
import sys

# Allow `python contracts/runner.py ...` execution (repo root on sys.path).
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import json
from collections import defaultdict
from typing import Any

import yaml

from contracts.utils import Violation, is_uuid_v4, parse_iso8601, read_jsonl, safe_float, safe_int, safe_mkdir


def _get_path(obj: Any, path: str) -> Any:
    cur = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _contract_dataset_name(contract: dict[str, Any]) -> str:
    schema = contract.get("schema")
    if isinstance(schema, list) and schema and isinstance(schema[0], dict):
        n = schema[0].get("name")
        if isinstance(n, str) and n:
            return n
    return str(contract.get("name") or "unknown_dataset")


def _quality_rules(contract: dict[str, Any]) -> list[dict[str, Any]]:
    q = contract.get("quality")
    if not isinstance(q, list) or not q:
        return []
    impl = q[0].get("implementation")
    if isinstance(impl, dict) and isinstance(impl.get("rules"), list):
        return [r for r in impl["rules"] if isinstance(r, dict)]
    return []


def _severity_for_rule(rule_type: str) -> str:
    if rule_type in ("uuid_v4", "datetime_iso8601"):
        return "HIGH"
    if rule_type in ("not_null", "enum", "range", "row_count_min"):
        return "CRITICAL"
    if rule_type in ("zscore_drift",):
        return "MEDIUM"
    return "LOW"


def _validate_rule(rule: dict[str, Any], row: dict[str, Any]) -> bool:
    rtype = rule.get("type")
    if not isinstance(rtype, str):
        return True
    if rtype == "row_count_min":
        return True
    field = rule.get("field")
    if not isinstance(field, str) or not field:
        return True

    v = _get_path(row, field)

    if rtype == "type":
        expected = rule.get("expected")
        if not isinstance(expected, str):
            return True
        if v is None:
            return True
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
            return isinstance(v, str) and is_uuid_v4(v)
        if expected == "datetime":
            return isinstance(v, str) and (parse_iso8601(v) is not None)
        return True

    if rtype == "not_null":
        return v is not None
    if rtype == "uuid_v4":
        return v is None or is_uuid_v4(v)
    if rtype == "datetime_iso8601":
        return v is None or (parse_iso8601(v) is not None)
    if rtype == "enum":
        vals = rule.get("values")
        return v is None or (isinstance(vals, list) and v in vals)
    if rtype == "range":
        if v is None:
            return True
        fv = safe_float(v)
        if fv is None:
            return False
        mn = rule.get("min")
        mx = rule.get("max")
        if mn is not None and fv < float(mn):
            return False
        if mx is not None and fv > float(mx):
            return False
        return True
    if rtype == "zscore_drift":
        if v is None:
            return True
        fv = safe_float(v)
        if fv is None:
            return False
        mean = safe_float(rule.get("mean"))
        stdev = safe_float(rule.get("stdev"))
        max_z = safe_float(rule.get("max_z")) or 3.5
        if mean is None or stdev is None or stdev <= 0:
            return True
        z = abs((fv - mean) / stdev)
        return z <= max_z
    return True


def _validate_structural(dataset: str, rows: list[dict[str, Any]], rules: list[dict[str, Any]]) -> list[Violation]:
    failures: dict[tuple[str, str], int] = defaultdict(int)
    for row in rows:
        for rule in rules:
            rtype = str(rule.get("type") or "")
            field = str(rule.get("field") or "")
            try:
                ok = _validate_rule(rule, row)
            except Exception:
                ok = False
            if not ok and rtype != "row_count_min":
                failures[(rtype, field)] += 1

    vios: list[Violation] = []
    for (rtype, field), cnt in sorted(failures.items(), key=lambda x: (-x[1], x[0][0], x[0][1])):
        vios.append(
            Violation(
                vtype="SCHEMA",
                field=field or "<unknown>",
                severity=_severity_for_rule(rtype),
                count=int(cnt),
                root_cause=dataset,
                lineage_path=_blame_chain(dataset),
            )
        )
    return vios


def _load_aux_paths() -> dict[str, str]:
    mapping = {
        "week1_intent_records": os.path.join("outputs", "week1", "intent_records.jsonl"),
        "week2_verdicts": os.path.join("outputs", "week2", "verdicts.jsonl"),
        "week3_extractions": os.path.join("outputs", "week3", "extractions.jsonl"),
        "week4_lineage_snapshots": os.path.join("outputs", "week4", "lineage_snapshots.jsonl"),
        "week5_events": os.path.join("outputs", "week5", "events.jsonl"),
        "traces_runs": os.path.join("outputs", "traces", "runs.jsonl"),
    }
    return {k: v for k, v in mapping.items() if os.path.exists(v)}


def _load_lineage_graph() -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """
    Returns (parents, children) graph maps for dataset-level edges found in week4 lineage snapshots.
    """
    parents: dict[str, set[str]] = defaultdict(set)
    children: dict[str, set[str]] = defaultdict(set)
    path = os.path.join("outputs", "week4", "lineage_snapshots.jsonl")
    if not os.path.exists(path):
        return parents, children
    for row in read_jsonl(path):
        if "_parse_error" in row:
            continue
        edges = row.get("edges")
        if not isinstance(edges, list):
            continue
        for e in edges:
            if not isinstance(e, dict):
                continue
            frm = e.get("from_dataset")
            to = e.get("to_dataset")
            if isinstance(frm, str) and isinstance(to, str) and frm and to:
                parents[to].add(frm)
                children[frm].add(to)
    return parents, children


def _blame_chain(dataset: str) -> list[str]:
    """
    Reverse-traverse lineage graph to an upstream root; returns ordered chain root..dataset.
    Deterministic: chooses lexicographically smallest parent at each step.
    """
    parents, _ = _load_lineage_graph()
    chain = [dataset]
    seen = {dataset}
    cur = dataset
    for _ in range(8):
        ps = sorted(p for p in parents.get(cur, set()) if p not in seen)
        if not ps:
            break
        cur = ps[0]
        chain.append(cur)
        seen.add(cur)
    return list(reversed(chain))


def _semantic_checks(dataset: str, rows: list[dict[str, Any]]) -> list[Violation]:
    aux = _load_aux_paths()
    vios: list[Violation] = []

    # Trace token math (Phase 4)
    if dataset == "traces_runs":
        bad = 0
        for r in rows:
            pt = safe_int(_get_path(r, "prompt_tokens"))
            ct = safe_int(_get_path(r, "completion_tokens"))
            tt = safe_int(_get_path(r, "total_tokens"))
            if pt is None or ct is None or tt is None:
                continue
            if tt != pt + ct:
                bad += 1
        if bad:
            vios.append(
                Violation(
                    vtype="SEMANTIC",
                    field="total_tokens",
                    severity="CRITICAL",
                    count=bad,
                    root_cause="traces_runs",
                    lineage_path=_blame_chain("traces_runs"),
                )
            )

    # Week2 weighted score accuracy (Phase 4)
    if dataset == "week2_verdicts":
        bad = 0
        for r in rows:
            w = _get_path(r, "scores.weights")
            if not isinstance(w, dict):
                continue
            wc = safe_float(w.get("correctness"))
            ws = safe_float(w.get("safety"))
            wst = safe_float(w.get("style"))
            if wc is None or ws is None or wst is None:
                continue
            denom = wc + ws + wst
            if denom <= 0:
                continue
            c = safe_float(_get_path(r, "scores.correctness"))
            s = safe_float(_get_path(r, "scores.safety"))
            st = safe_float(_get_path(r, "scores.style"))
            wscore = safe_float(_get_path(r, "scores.weighted_score"))
            if c is None or s is None or st is None or wscore is None:
                continue
            expected = (wc * c + ws * s + wst * st) / denom
            if abs(expected - wscore) > 1e-6:
                bad += 1
        if bad:
            vios.append(
                Violation(
                    vtype="SEMANTIC",
                    field="scores.weighted_score",
                    severity="HIGH",
                    count=bad,
                    root_cause="week2_verdicts",
                    lineage_path=_blame_chain("week2_verdicts"),
                )
            )

    # Week1 -> Week2 cross contract (mandatory)
    if dataset == "week2_verdicts" and "week1_intent_records" in aux:
        intents = [r for r in read_jsonl(aux["week1_intent_records"]) if "_parse_error" not in r]
        intent_files: set[str] = set()
        for r in intents:
            code_refs = _get_path(r, "intent.code_refs")
            if isinstance(code_refs, list):
                for cref in code_refs:
                    if isinstance(cref, dict) and isinstance(cref.get("file"), str):
                        intent_files.add(cref["file"])
        missing = 0
        for r in rows:
            f = _get_path(r, "target_ref.file")
            if isinstance(f, str) and f not in intent_files:
                missing += 1
        if missing:
            vios.append(
                Violation(
                    vtype="SEMANTIC",
                    field="target_ref.file",
                    severity="CRITICAL",
                    count=missing,
                    root_cause="week1_intent_records",
                    lineage_path=_blame_chain("week2_verdicts"),
                )
            )

    # Week3 -> Week4 cross contract (mandatory)
    if dataset == "week3_extractions" and "week4_lineage_snapshots" in aux:
        lineage_rows = [r for r in read_jsonl(aux["week4_lineage_snapshots"]) if "_parse_error" not in r]
        lineage_doc_ids: set[str] = set()
        for lr in lineage_rows:
            nodes = lr.get("nodes")
            if isinstance(nodes, list):
                for n in nodes:
                    if isinstance(n, dict):
                        ref = n.get("ref")
                        if isinstance(ref, dict) and isinstance(ref.get("doc_id"), str):
                            lineage_doc_ids.add(ref["doc_id"])
        missing = 0
        for r in rows:
            doc_id = _get_path(r, "doc_id")
            if isinstance(doc_id, str) and doc_id not in lineage_doc_ids:
                missing += 1
        if missing:
            vios.append(
                Violation(
                    vtype="SEMANTIC",
                    field="doc_id",
                    severity="HIGH",
                    count=missing,
                    root_cause="week4_lineage_snapshots",
                    lineage_path=_blame_chain("week3_extractions"),
                )
            )

    # Event monotonicity per stream
    if dataset == "week5_events":
        last_pos: dict[str, int] = {}
        breaks = 0
        for r in rows:
            sid = _get_path(r, "stream_id")
            gp = safe_int(_get_path(r, "global_position"))
            if not isinstance(sid, str) or gp is None:
                continue
            prev = last_pos.get(sid)
            if prev is not None and gp < prev:
                breaks += 1
            last_pos[sid] = gp
        if breaks:
            vios.append(
                Violation(
                    vtype="SEMANTIC",
                    field="global_position",
                    severity="HIGH",
                    count=breaks,
                    root_cause="week5_events",
                    lineage_path=_blame_chain("week5_events"),
                )
            )

        # Event payload schema enforcement (Week5 -> Event Schema)
        missing_payload = 0
        for r in rows:
            et = _get_path(r, "event_type")
            payload = _get_path(r, "payload")
            if not isinstance(et, str) or not isinstance(payload, dict):
                continue
            required_fields = _event_type_required_fields(et)
            if required_fields is None:
                continue
            for fld in required_fields:
                if fld not in payload:
                    missing_payload += 1
                    break
        if missing_payload:
            vios.append(
                Violation(
                    vtype="SEMANTIC",
                    field="payload",
                    severity="CRITICAL",
                    count=missing_payload,
                    root_cause="week5_events",
                    lineage_path=_blame_chain("week5_events"),
                )
            )

    return vios


def _event_type_required_fields(event_type: str) -> list[str] | None:
    mapping = {
        "ApplicationSubmitted": ["application_id", "applicant_id", "requested_amount_usd", "submitted_at"],
        "DocumentUploadRequested": ["application_id", "required_document_types", "deadline", "requested_by"],
        "PackageCreated": ["package_id", "application_id", "required_documents", "created_at"],
    }
    return mapping.get(event_type)


def run_validation(contract_path: str, data_path: str, report_path: str | None = None) -> dict[str, Any]:
    safe_mkdir("validation_reports")
    contract: dict[str, Any] = {}
    try:
        with open(contract_path, "r", encoding="utf-8") as f:
            contract = yaml.safe_load(f) or {}
    except Exception as e:
        contract = {"_parse_error": str(e)}

    dataset = _contract_dataset_name(contract) if isinstance(contract, dict) else "unknown_dataset"
    rules = _quality_rules(contract) if isinstance(contract, dict) else []

    rows: list[dict[str, Any]] = [r for r in read_jsonl(data_path) if "_parse_error" not in r]

    violations: list[Violation] = []

    # dataset row count
    try:
        for rule in rules:
            if rule.get("type") == "row_count_min":
                mn = safe_int(rule.get("min")) or 1
                if len(rows) < mn:
                    violations.append(
                        Violation(
                            vtype="SCHEMA",
                            field="<dataset>",
                            severity="CRITICAL",
                            count=int(mn - len(rows)),
                            root_cause=dataset,
                            lineage_path=_blame_chain(dataset),
                        )
                    )
    except Exception:
        pass

    try:
        violations.extend(_validate_structural(dataset, rows, rules))
    except Exception:
        violations.append(
            Violation(
                vtype="SCHEMA",
                field="<engine>",
                severity="CRITICAL",
                count=1,
                root_cause=dataset,
                lineage_path=[dataset],
            )
        )

    try:
        violations.extend(_semantic_checks(dataset, rows))
    except Exception:
        pass

    total = len(rows)
    worst = max([v.count for v in violations], default=0)
    failed_records = min(total, worst)
    pass_rate = float((total - failed_records) / total) if total else 0.0
    status = "PASS" if not violations else "FAIL"

    report = {
        "status": status,
        "violations": [
            {
                "type": v.vtype,
                "field": v.field,
                "severity": v.severity,
                "count": v.count,
                "root_cause": v.root_cause,
                "lineage_path": v.lineage_path,
            }
            for v in violations
        ],
        "summary": {"total_records": int(total), "failed_records": int(failed_records), "pass_rate": float(pass_rate)},
    }

    if report_path is None:
        report_path = os.path.join("validation_reports", f"{os.path.basename(data_path).replace('.jsonl','')}.json")

    try:
        with open(report_path, "w", encoding="utf-8", newline="\n") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="ValidationRunner: never throws; emits report JSON.")
    parser.add_argument("--contract", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--report", required=False)
    args = parser.parse_args()
    try:
        report = run_validation(args.contract, args.data, args.report)
        print(json.dumps(report.get("summary", {}), ensure_ascii=False))
        return 0 if report.get("status") == "PASS" else 1
    except Exception as e:
        safe_mkdir("validation_reports")
        with open(os.path.join("validation_reports", "runner_error.json"), "w", encoding="utf-8", newline="\n") as f:
            json.dump({"error": str(e)}, f, ensure_ascii=False, indent=2)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
