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


def _read_jsonl_with_line_no(path: str) -> tuple[list[tuple[int, dict[str, Any]]], int]:
    rows: list[tuple[int, dict[str, Any]]] = []
    parse_errors = 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        rows.append((line_no, obj))
                    else:
                        rows.append((line_no, {"_non_object": obj}))
                except Exception:
                    parse_errors += 1
    except Exception:
        return [], 0
    return rows, parse_errors


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
    if rule_type in ("regex", "unique", "relationships", "event_payload_required", "event_payload_positive_amount", "token_math"):
        return "CRITICAL"
    if rule_type in ("range_inferred",):
        return "MEDIUM"
    if rule_type in ("zscore_drift",):
        return "MEDIUM"
    return "LOW"


def _clause_id(rule: dict[str, Any], fallback: str) -> str:
    cid = rule.get("clause_id")
    return cid if isinstance(cid, str) and cid else fallback


def _violation_message(rule: dict[str, Any]) -> str:
    rtype = str(rule.get("type") or "")
    if rtype == "not_null":
        return "required field is null or missing"
    if rtype == "uuid_v4":
        return "value is not UUIDv4"
    if rtype == "datetime_iso8601":
        return "value is not ISO8601 datetime"
    if rtype == "enum":
        return "value not in accepted values"
    if rtype in ("range", "range_inferred"):
        return "value outside allowed numeric bounds"
    if rtype == "regex":
        return "value does not match regex"
    if rtype == "unique":
        return "duplicate values detected"
    if rtype == "relationships":
        return "referential integrity failure"
    if rtype == "monotonic_increasing":
        return "sequence is not monotonic increasing"
    if rtype == "event_payload_required":
        return "event payload missing required fields"
    if rtype == "event_payload_positive_amount":
        return "requested_amount_usd must be > 0"
    if rtype == "if_confidence_below_threshold_flag":
        return "confidence below threshold must be flagged"
    if rtype == "token_math":
        return "total_tokens must equal prompt_tokens + completion_tokens"
    if rtype == "weighted_score_math":
        return "weighted_score does not match weighted components"
    if rtype == "time_order":
        return "end_time must be >= start_time"
    if rtype == "zscore_drift":
        return "statistical drift detected (z-score)"
    if rtype == "type":
        return "wrong type"
    return "rule violated"


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
    if rtype in ("range", "range_inferred"):
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
    if rtype == "regex":
        pattern = rule.get("pattern")
        if v is None:
            return True
        if not isinstance(pattern, str) or not pattern:
            return True
        if not isinstance(v, str):
            return False
        try:
            import re

            return re.match(pattern, v) is not None
        except Exception:
            return True
    return True


def _validate_structural(dataset: str, rows: list[dict[str, Any]], rules: list[dict[str, Any]]) -> list[Violation]:
    # Legacy helper retained for backward compatibility (contract-driven evaluation occurs in run_validation).
    failures: dict[tuple[str, str], int] = defaultdict(int)
    for row in rows:
        for rule in rules:
            rtype = str(rule.get("type") or "")
            field = str(rule.get("field") or "")
            if rtype in ("row_count_min", "unique", "relationships", "monotonic_increasing", "event_payload_required", "event_payload_positive_amount", "if_confidence_below_threshold_flag", "token_math", "weighted_score_math", "time_order"):
                continue
            try:
                ok = _validate_rule(rule, row)
            except Exception:
                ok = False
            if not ok:
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


def _execute_rules(
    dataset: str,
    indexed_rows: list[tuple[int, dict[str, Any]]],
    rules: list[dict[str, Any]],
    failed_row_lines: set[int],
) -> list[Violation]:
    """
    Executes all rule types present in contract rules.
    Never throws: caller wraps.
    """
    # Partition rules
    row_rules: list[dict[str, Any]] = []
    dataset_rules: list[dict[str, Any]] = []
    for r in rules:
        rtype = r.get("type")
        if rtype in (
            "unique",
            "relationships",
            "monotonic_increasing",
            "event_payload_required",
            "event_payload_positive_amount",
            "if_confidence_below_threshold_flag",
            "token_math",
            "weighted_score_math",
            "time_order",
        ):
            dataset_rules.append(r)
        elif rtype != "row_count_min":
            row_rules.append(r)

    violations: list[Violation] = []

    # Row-level checks
    failures: dict[str, dict[str, Any]] = {}
    for line_no, row in indexed_rows:
        for rule in row_rules:
            rtype = str(rule.get("type") or "")
            field = str(rule.get("field") or "")
            ok = True
            try:
                ok = _validate_rule(rule, row)
            except Exception:
                ok = False
            if ok:
                continue
            failed_row_lines.add(line_no)
            cid = _clause_id(rule, f"{rtype}:{field}")
            rec = failures.setdefault(
                cid,
                {
                    "rtype": rtype,
                    "field": field or "<unknown>",
                    "count": 0,
                    "samples": [],
                    "rule": rule,
                },
            )
            rec["count"] += 1
            if len(rec["samples"]) < 3:
                rec["samples"].append({"line_no": line_no, "value": _get_path(row, field)})

    for cid, rec in failures.items():
        rule = rec["rule"]
        rtype = rec["rtype"]
        violations.append(
            Violation(
                vtype="SCHEMA",
                field=rec["field"],
                severity=_severity_for_rule(rtype),
                count=int(rec["count"]),
                root_cause=dataset,
                lineage_path=_blame_chain(dataset),
                clause_id=cid,
                message=_violation_message(rule),
                samples=rec["samples"],
            )
        )

    # Dataset-level checks
    for rule in dataset_rules:
        rtype = str(rule.get("type") or "")
        cid = _clause_id(rule, rtype)

        if rtype == "unique":
            field = str(rule.get("field") or "")
            values_to_lines: dict[str, list[int]] = defaultdict(list)
            for line_no, row in indexed_rows:
                v = _get_path(row, field)
                if isinstance(v, str) and v:
                    values_to_lines[v].append(line_no)
            dup_lines: list[int] = []
            for v, lines in values_to_lines.items():
                if len(lines) > 1:
                    dup_lines.extend(lines)
            dup_lines = sorted(set(dup_lines))
            if dup_lines:
                for ln in dup_lines:
                    failed_row_lines.add(ln)
                violations.append(
                    Violation(
                        vtype="SEMANTIC",
                        field=field,
                        severity="CRITICAL",
                        count=len(dup_lines),
                        root_cause=dataset,
                        lineage_path=_blame_chain(dataset),
                        clause_id=cid,
                        message=_violation_message(rule),
                        samples=[{"line_no": ln} for ln in dup_lines[:3]],
                    )
                )

        if rtype == "monotonic_increasing":
            field = str(rule.get("field") or "")
            group_by = str(rule.get("group_by") or "")
            groups: dict[str, list[tuple[int, float]]] = defaultdict(list)
            for line_no, row in indexed_rows:
                if group_by:
                    g = _get_path(row, group_by)
                    if not isinstance(g, str):
                        continue
                else:
                    g = "__all__"
                v = safe_float(_get_path(row, field))
                if v is None:
                    continue
                groups[g].append((line_no, v))
            breaks: list[dict[str, Any]] = []
            for g, lv in groups.items():
                last: float | None = None
                last_ln: int | None = None
                for ln, v in lv:
                    if last is not None and v < last:
                        breaks.append({"line_no": ln, "group": g, "prev_line_no": last_ln, "prev_value": last, "value": v})
                        failed_row_lines.add(ln)
                    last = v
                    last_ln = ln
            if breaks:
                violations.append(
                    Violation(
                        vtype="SEMANTIC",
                        field=field,
                        severity="HIGH",
                        count=len(breaks),
                        root_cause=dataset,
                        lineage_path=_blame_chain(dataset),
                        clause_id=cid,
                        message=_violation_message(rule),
                        samples=breaks[:3],
                    )
                )

        if rtype == "relationships":
            field = str(rule.get("field") or "")
            to_dataset = rule.get("to_dataset")
            to_field = rule.get("to_field")
            if not isinstance(to_dataset, str) or not isinstance(to_field, str):
                continue
            ref_values = _relationship_reference_values(to_dataset, to_field)
            if ref_values is None:
                continue
            missing: list[dict[str, Any]] = []
            missing_count = 0
            for line_no, row in indexed_rows:
                v = _get_path(row, field)
                if isinstance(v, str) and v and v not in ref_values:
                    failed_row_lines.add(line_no)
                    missing_count += 1
                    if len(missing) < 3:
                        missing.append({"line_no": line_no, "value": v})
            if missing:
                violations.append(
                    Violation(
                        vtype="SEMANTIC",
                        field=field,
                        severity="CRITICAL",
                        count=int(missing_count),
                        root_cause=to_dataset,
                        lineage_path=_blame_chain(dataset),
                        clause_id=cid,
                        message=_violation_message(rule),
                        samples=missing,
                    )
                )

        if rtype == "event_payload_required":
            event_type_field = str(rule.get("event_type_field") or "event_type")
            payload_field = str(rule.get("payload_field") or "payload")
            missing = 0
            samples: list[dict[str, Any]] = []
            for line_no, row in indexed_rows:
                et = _get_path(row, event_type_field)
                payload = _get_path(row, payload_field)
                if not isinstance(et, str) or not isinstance(payload, dict):
                    continue
                required_fields = _event_type_required_fields(et)
                if required_fields is None:
                    continue
                absent = [f for f in required_fields if f not in payload]
                if absent:
                    missing += 1
                    failed_row_lines.add(line_no)
                    if len(samples) < 3:
                        samples.append({"line_no": line_no, "event_type": et, "missing": absent})
            if missing:
                violations.append(
                    Violation(
                        vtype="SEMANTIC",
                        field=payload_field,
                        severity="CRITICAL",
                        count=missing,
                        root_cause=dataset,
                        lineage_path=_blame_chain(dataset),
                        clause_id=cid,
                        message=_violation_message(rule),
                        samples=samples,
                    )
                )

        if rtype == "event_payload_positive_amount":
            target_et = rule.get("event_type")
            amount_field = str(rule.get("payload_amount_field") or "")
            if not isinstance(target_et, str) or not amount_field:
                continue
            bad = 0
            samples: list[dict[str, Any]] = []
            for line_no, row in indexed_rows:
                et = _get_path(row, "event_type")
                if et != target_et:
                    continue
                amt = safe_float(_get_path(row, amount_field))
                if amt is None or amt <= 0:
                    bad += 1
                    failed_row_lines.add(line_no)
                    if len(samples) < 3:
                        samples.append({"line_no": line_no, "value": _get_path(row, amount_field)})
            if bad:
                violations.append(
                    Violation(
                        vtype="SEMANTIC",
                        field=amount_field,
                        severity="CRITICAL",
                        count=bad,
                        root_cause=dataset,
                        lineage_path=_blame_chain(dataset),
                        clause_id=cid,
                        message=_violation_message(rule),
                        samples=samples,
                    )
                )

        if rtype == "if_confidence_below_threshold_flag":
            cf = str(rule.get("confidence_field") or "confidence")
            tf = str(rule.get("threshold_field") or "threshold")
            ff = str(rule.get("flag_field") or "flags.flagged_for_review")
            bad = 0
            samples: list[dict[str, Any]] = []
            for line_no, row in indexed_rows:
                c = safe_float(_get_path(row, cf))
                t = safe_float(_get_path(row, tf))
                flag = _get_path(row, ff)
                if c is None or t is None:
                    continue
                if c < t and flag is not True:
                    bad += 1
                    failed_row_lines.add(line_no)
                    if len(samples) < 3:
                        samples.append({"line_no": line_no, "confidence": c, "threshold": t, "flag": flag})
            if bad:
                violations.append(
                    Violation(
                        vtype="SEMANTIC",
                        field=ff,
                        severity="MEDIUM",
                        count=bad,
                        root_cause=dataset,
                        lineage_path=_blame_chain(dataset),
                        clause_id=cid,
                        message=_violation_message(rule),
                        samples=samples,
                    )
                )

        if rtype == "token_math":
            ptf = str(rule.get("prompt_tokens_field") or "prompt_tokens")
            ctf = str(rule.get("completion_tokens_field") or "completion_tokens")
            ttf = str(rule.get("total_tokens_field") or "total_tokens")
            bad = 0
            samples: list[dict[str, Any]] = []
            for line_no, row in indexed_rows:
                pt = safe_int(_get_path(row, ptf))
                ct = safe_int(_get_path(row, ctf))
                tt = safe_int(_get_path(row, ttf))
                if pt is None or ct is None or tt is None:
                    continue
                if tt != pt + ct:
                    bad += 1
                    failed_row_lines.add(line_no)
                    if len(samples) < 3:
                        samples.append({"line_no": line_no, "prompt": pt, "completion": ct, "total": tt})
            if bad:
                violations.append(
                    Violation(
                        vtype="SEMANTIC",
                        field=ttf,
                        severity="CRITICAL",
                        count=bad,
                        root_cause=dataset,
                        lineage_path=_blame_chain(dataset),
                        clause_id=cid,
                        message=_violation_message(rule),
                        samples=samples,
                    )
                )

        if rtype == "time_order":
            sf = str(rule.get("start_field") or "start_time")
            ef = str(rule.get("end_field") or "end_time")
            bad = 0
            samples: list[dict[str, Any]] = []
            for line_no, row in indexed_rows:
                s = parse_iso8601(_get_path(row, sf))
                e = parse_iso8601(_get_path(row, ef))
                if s is None or e is None:
                    continue
                if e < s:
                    bad += 1
                    failed_row_lines.add(line_no)
                    if len(samples) < 3:
                        samples.append({"line_no": line_no, "start": _get_path(row, sf), "end": _get_path(row, ef)})
            if bad:
                violations.append(
                    Violation(
                        vtype="SEMANTIC",
                        field=ef,
                        severity="HIGH",
                        count=bad,
                        root_cause=dataset,
                        lineage_path=_blame_chain(dataset),
                        clause_id=cid,
                        message=_violation_message(rule),
                        samples=samples,
                    )
                )

        if rtype == "weighted_score_math":
            cf = str(rule.get("correctness_field") or "scores.correctness")
            sf = str(rule.get("safety_field") or "scores.safety")
            stf = str(rule.get("style_field") or "scores.style")
            wf = str(rule.get("weights_field") or "scores.weights")
            wsf = str(rule.get("weighted_score_field") or "scores.weighted_score")
            bad = 0
            samples: list[dict[str, Any]] = []
            for line_no, row in indexed_rows:
                w = _get_path(row, wf)
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
                c = safe_float(_get_path(row, cf))
                s = safe_float(_get_path(row, sf))
                st = safe_float(_get_path(row, stf))
                wscore = safe_float(_get_path(row, wsf))
                if c is None or s is None or st is None or wscore is None:
                    continue
                expected = (wc * c + ws * s + wst * st) / denom
                if abs(expected - wscore) > 1e-6:
                    bad += 1
                    failed_row_lines.add(line_no)
                    if len(samples) < 3:
                        samples.append({"line_no": line_no, "expected": expected, "actual": wscore})
            if bad:
                violations.append(
                    Violation(
                        vtype="SEMANTIC",
                        field=wsf,
                        severity="HIGH",
                        count=bad,
                        root_cause=dataset,
                        lineage_path=_blame_chain(dataset),
                        clause_id=cid,
                        message=_violation_message(rule),
                        samples=samples,
                    )
                )

    return violations


def _relationship_reference_values(to_dataset: str, to_field: str) -> set[str] | None:
    """
    Build reference value sets for relationships rules.
    Supported (mastered scope): week4 lineage doc_id set.
    """
    if to_dataset != "week4_lineage_snapshots":
        return None
    path = os.path.join("outputs", "week4", "lineage_snapshots.jsonl")
    if not os.path.exists(path):
        return set()
    doc_ids: set[str] = set()
    for row in read_jsonl(path):
        if "_parse_error" in row:
            continue
        nodes = row.get("nodes")
        if not isinstance(nodes, list):
            continue
        for n in nodes:
            if not isinstance(n, dict):
                continue
            ref = n.get("ref")
            if isinstance(ref, dict) and isinstance(ref.get("doc_id"), str):
                doc_ids.add(ref["doc_id"])
    return doc_ids


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

    indexed_rows, parse_errors = _read_jsonl_with_line_no(data_path)
    rows_only: list[dict[str, Any]] = [r for _, r in indexed_rows]

    violations: list[Violation] = []
    failed_row_lines: set[int] = set()

    # dataset row count
    try:
        for rule in rules:
            if rule.get("type") == "row_count_min":
                mn = safe_int(rule.get("min")) or 1
                if len(rows_only) < mn:
                    violations.append(
                        Violation(
                            vtype="SCHEMA",
                            field="<dataset>",
                            severity="CRITICAL",
                            count=int(mn - len(rows_only)),
                            root_cause=dataset,
                            lineage_path=_blame_chain(dataset),
                            clause_id=_clause_id(rule, "row_count_min"),
                            message=_violation_message(rule),
                            samples=None,
                        )
                    )
    except Exception:
        pass

    # Contract-driven rule execution.
    try:
        violations.extend(_execute_rules(dataset, indexed_rows, rules, failed_row_lines))
    except Exception:
        violations.append(
            Violation(
                vtype="SCHEMA",
                field="<engine>",
                severity="CRITICAL",
                count=1,
                root_cause=dataset,
                lineage_path=_blame_chain(dataset),
                clause_id="engine_failure",
                message="validation engine failure",
                samples=None,
            )
        )

    total = len(rows_only)
    failed_records = min(total, len(failed_row_lines)) if total else 0
    pass_rate = float((total - failed_records) / total) if total else 0.0
    status = "PASS" if not violations else "FAIL"

    total_rules = len(rules)
    rules_failed = len({v.clause_id for v in violations if v.clause_id})
    rows_affected = int(failed_records)
    failure_rate = float(rows_affected / total) if total else 0.0

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
                "clause_id": v.clause_id,
                "message": v.message,
                "samples": v.samples,
            }
            for v in violations
        ],
        "summary": {
            "total_records": int(total),
            "failed_records": int(failed_records),
            "pass_rate": float(pass_rate),
            "total_rules": int(total_rules),
            "rules_failed": int(rules_failed),
            "rows_affected": int(rows_affected),
            "failure_rate": float(failure_rate),
        },
        "parse_errors": int(parse_errors),
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
