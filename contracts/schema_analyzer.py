from __future__ import annotations

import argparse
import difflib
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import yaml

# Allow `python contracts/schema_analyzer.py ...` execution (repo root on sys.path).
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _utc_now_compact() -> str:
    fixed = os.environ.get("SCHEMA_SNAPSHOT_TIMESTAMP")
    if fixed:
        return fixed
    # Include microseconds to avoid collisions when generator runs multiple times per second.
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")


def _safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _load_yaml(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        obj = yaml.safe_load(f) or {}
        return obj if isinstance(obj, dict) else {"_non_object": obj}


def _dump_yaml(path: str, obj: Any) -> None:
    _safe_mkdir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        yaml.safe_dump(obj, f, sort_keys=False, allow_unicode=True)


@dataclass(frozen=True)
class FieldSnapshot:
    name: str
    logical_type: str
    required: bool
    array_item_type: str | None = None
    minimum: float | int | None = None
    maximum: float | int | None = None


def _extract_schema_fields(contract: dict[str, Any]) -> list[FieldSnapshot]:
    schema = contract.get("schema")
    if not (isinstance(schema, list) and schema and isinstance(schema[0], dict)):
        return []
    props = schema[0].get("properties")
    if not isinstance(props, list):
        return []
    fields: list[FieldSnapshot] = []
    for p in props:
        if not isinstance(p, dict):
            continue
        name = p.get("name")
        logical_type = p.get("logicalType")
        required = p.get("required")
        minimum = p.get("min") if isinstance(p.get("min"), (int, float)) else None
        maximum = p.get("max") if isinstance(p.get("max"), (int, float)) else None
        if not isinstance(name, str) or not isinstance(logical_type, str):
            continue
        array_item_type = None
        if logical_type == "array":
            # If available in future schemas; otherwise keep None.
            array_item_type = p.get("itemsLogicalType") if isinstance(p.get("itemsLogicalType"), str) else None
        fields.append(
            FieldSnapshot(
                name=name,
                logical_type=logical_type,
                required=bool(required),
                array_item_type=array_item_type,
                minimum=minimum,
                maximum=maximum,
            )
        )
    fields.sort(key=lambda f: f.name)
    return fields


def _schema_representation(contract: dict[str, Any]) -> dict[str, Any]:
    """
    Snapshot representation: canonical field list + inferred constraints (as present in contract quality rules).
    """
    fields = _extract_schema_fields(contract)
    quality_rules = []
    try:
        q = contract.get("quality")
        if isinstance(q, list) and q and isinstance(q[0], dict):
            impl = q[0].get("implementation")
            if isinstance(impl, dict) and isinstance(impl.get("rules"), list):
                for r in impl["rules"]:
                    if isinstance(r, dict):
                        # store stable subset for schema evolution diffs
                        quality_rules.append(
                            {
                                "type": r.get("type"),
                                "field": r.get("field"),
                                "expected": r.get("expected"),
                                "min": r.get("min"),
                                "max": r.get("max"),
                                "values": r.get("values"),
                                "pattern": r.get("pattern"),
                                "required_fields": r.get("required_fields"),
                                "group_by": r.get("group_by"),
                            }
                        )
    except Exception:
        pass
    quality_rules = sorted(quality_rules, key=lambda r: (str(r.get("type") or ""), str(r.get("field") or "")))
    return {
        "fields": [f.__dict__ for f in fields],
        "quality_rule_fingerprint": quality_rules,
    }


def snapshot_contract(contract_path: str, snapshots_root: str = "schema_snapshots", timestamp: str | None = None) -> str:
    contract = _load_yaml(contract_path)
    contract_id = contract.get("id") if isinstance(contract.get("id"), str) else "unknown_contract"
    ts = timestamp or _utc_now_compact()

    out_dir = os.path.join(snapshots_root, str(contract_id))
    _safe_mkdir(out_dir)
    out_path = os.path.join(out_dir, f"{ts}.yaml")

    snapshot = {
        "snapshot_version": "1.0.0",
        "captured_at": ts,
        "contract": {
            "id": contract_id,
            "name": contract.get("name"),
            "version": contract.get("version"),
            "domain": contract.get("domain"),
            "dataProduct": contract.get("dataProduct"),
        },
        "schema": _schema_representation(contract),
    }
    _dump_yaml(out_path, snapshot)
    return out_path


def _field_map(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    fields = ((snapshot.get("schema") or {}).get("fields")) if isinstance(snapshot.get("schema"), dict) else None
    if not isinstance(fields, list):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for f in fields:
        if isinstance(f, dict) and isinstance(f.get("name"), str):
            out[f["name"]] = f
    return out


def _type_rank(t: str) -> int:
    # Used to classify widen/narrow changes.
    order = {
        "integer": 1,
        "number": 2,
        "string": 3,
        "boolean": 3,
        "object": 4,
        "array": 4,
    }
    return order.get(t, 99)


def _tokenize_field(name: str) -> set[str]:
    parts = []
    for seg in name.replace(".", "_").split("_"):
        seg = seg.strip().lower()
        if seg:
            parts.append(seg)
    return set(parts)


def diff_snapshots(a_path: str, b_path: str) -> dict[str, Any]:
    a = _load_yaml(a_path)
    b = _load_yaml(b_path)
    a_fields = _field_map(a)
    b_fields = _field_map(b)

    removed = sorted(set(a_fields.keys()) - set(b_fields.keys()))
    added = sorted(set(b_fields.keys()) - set(a_fields.keys()))

    changed: list[dict[str, Any]] = []
    for k in sorted(set(a_fields.keys()) & set(b_fields.keys())):
        af = a_fields[k]
        bf = b_fields[k]
        if af.get("logical_type") != bf.get("logical_type") or bool(af.get("required")) != bool(bf.get("required")):
            changed.append(
                {
                    "field": k,
                    "from": {"logical_type": af.get("logical_type"), "required": bool(af.get("required"))},
                    "to": {"logical_type": bf.get("logical_type"), "required": bool(bf.get("required"))},
                }
            )

    # Rename heuristics: removed + added with same type and high token overlap.
    renames: list[dict[str, Any]] = []
    for r in removed:
        for ad in added:
            if str(a_fields[r].get("logical_type")) != str(b_fields[ad].get("logical_type")):
                continue
            rt = _tokenize_field(r)
            at = _tokenize_field(ad)
            if not rt or not at:
                continue
            j = len(rt & at) / max(1, len(rt | at))
            if j >= 0.8:
                renames.append({"from": r, "to": ad, "confidence": round(j, 3)})

    compatibility, reasons = classify_compatibility(a_fields, b_fields, removed, added, changed, renames)

    return {
        "from_snapshot": a_path,
        "to_snapshot": b_path,
        "changes": {"added": added, "removed": removed, "changed": changed, "renames": renames},
        "compatibility": {"verdict": compatibility, "reasons": reasons},
        "diff_text": _unified_diff_text(a_path, b_path),
    }


def classify_compatibility(
    a_fields: dict[str, dict[str, Any]],
    b_fields: dict[str, dict[str, Any]],
    removed: list[str],
    added: list[str],
    changed: list[dict[str, Any]],
    renames: list[dict[str, Any]],
) -> tuple[str, list[str]]:
    """
    Confluent-style classification:
    - Compatible: nullable column added, type widened
    - Breaking: non-nullable added, column removed, type narrowed, column renamed
    """
    reasons: list[str] = []

    # Column removed => breaking
    if removed:
        reasons.append(f"Breaking: removed columns: {', '.join(removed[:20])}")

    # Column added
    for f in added:
        req = bool(b_fields.get(f, {}).get("required"))
        if req:
            reasons.append(f"Breaking: added non-nullable column `{f}`")
        else:
            reasons.append(f"Compatible: added nullable column `{f}`")

    # Type changes and requiredness changes
    for c in changed:
        field = c["field"]
        ft = str(c["from"].get("logical_type") or "")
        tt = str(c["to"].get("logical_type") or "")
        if ft and tt and ft != tt:
            # Explicit narrow-type detection: float[0..1] -> int[0..100] is CRITICAL (confidence scale drift).
            if (
                "confidence" in field.lower()
                and ft == "number"
                and tt == "integer"
                and isinstance(a_fields.get(field, {}).get("maximum"), (int, float))
                and isinstance(b_fields.get(field, {}).get("maximum"), (int, float))
                and float(a_fields[field]["maximum"]) <= 1.0
                and float(b_fields[field]["maximum"]) >= 100.0
            ):
                reasons.append("Breaking: CRITICAL confidence scale narrowing detected (0..1 probability -> 0..100 percentage).")
                continue
            if _type_rank(tt) > _type_rank(ft):
                reasons.append(f"Compatible: widened type `{field}` from {ft} -> {tt}")
            else:
                reasons.append(f"Breaking: narrowed type `{field}` from {ft} -> {tt}")
        if bool(c["from"].get("required")) != bool(c["to"].get("required")):
            if bool(c["to"].get("required")) and not bool(c["from"].get("required")):
                reasons.append(f"Breaking: `{field}` became non-nullable")
            else:
                reasons.append(f"Compatible: `{field}` became nullable")

    # Rename => breaking (by model)
    if renames:
        pairs = [f"{r['from']}->{r['to']}" for r in renames[:10]]
        reasons.append(f"Breaking: potential renames detected: {', '.join(pairs)}")

    verdict = "COMPATIBLE"
    if any(r.startswith("Breaking:") for r in reasons):
        verdict = "BREAKING"
    return verdict, reasons


def _unified_diff_text(a_path: str, b_path: str) -> str:
    a = open(a_path, "r", encoding="utf-8").read().splitlines(keepends=False)
    b = open(b_path, "r", encoding="utf-8").read().splitlines(keepends=False)
    diff = difflib.unified_diff(a, b, fromfile=a_path, tofile=b_path, lineterm="")
    return "\n".join(list(diff)[:4000])


def _lineage_blast_radius(contract_path: str) -> dict[str, Any]:
    contract = _load_yaml(contract_path)
    lineage = (contract.get("customProperties") or {}).get("lineage") if isinstance(contract.get("customProperties"), dict) else None
    if isinstance(lineage, dict):
        return lineage
    return {"upstream": [], "downstream": [], "blast_radius": {"downstream_count": 0}}


def _load_registry(registry_path: str = os.path.join("contract_registry", "subscriptions.yaml")) -> list[dict[str, Any]]:
    if not os.path.exists(registry_path):
        return []
    try:
        reg = yaml.safe_load(open(registry_path, "r", encoding="utf-8")) or {}
    except Exception:
        return []
    subs = reg.get("subscriptions")
    return subs if isinstance(subs, list) else []


def _consumer_impact(
    *,
    dataset: str,
    affected_fields: list[str],
    lineage_path: str = os.path.join("outputs", "week4", "lineage_snapshots.jsonl"),
    registry_path: str = os.path.join("contract_registry", "subscriptions.yaml"),
) -> list[dict[str, Any]]:
    """
    Per-consumer impact: uses registry as primary, lineage as secondary.
    Adds contamination_depth per hop.
    """
    subs = _load_registry(registry_path)
    # build graph
    children: dict[str, set[str]] = {}
    breaking_by_edge: dict[tuple[str, str], list[str]] = {}
    for s in subs:
        if not isinstance(s, dict):
            continue
        frm = s.get("from")
        to = s.get("to")
        if isinstance(frm, str) and isinstance(to, str):
            children.setdefault(frm, set()).add(to)
            bf = s.get("breaking_fields")
            breaking_by_edge[(frm, to)] = bf if isinstance(bf, list) else []

    # augment with lineage edges if present
    if os.path.exists(lineage_path):
        try:
            for row in _load_yaml(lineage_path) if False else []:
                _ = row
        except Exception:
            pass
        # lineage snapshots are jsonl; read lightly without importing utils
        try:
            import json

            with open(lineage_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    edges = obj.get("edges")
                    if not isinstance(edges, list):
                        continue
                    for e in edges:
                        if isinstance(e, dict) and e.get("from_dataset") == dataset and isinstance(e.get("to_dataset"), str):
                            children.setdefault(dataset, set()).add(e["to_dataset"])
        except Exception:
            pass

    impacted: list[dict[str, Any]] = []
    # BFS
    q: list[tuple[str, int]] = [(dataset, 0)]
    seen = {dataset}
    while q:
        cur, depth = q.pop(0)
        for ch in sorted(children.get(cur, set())):
            if ch in seen:
                continue
            seen.add(ch)
            edge_breaking = breaking_by_edge.get((cur, ch), [])
            # intersect
            fields = sorted(set(affected_fields) & set([str(x) for x in edge_breaking]))
            impacted.append(
                {
                    "consumer": ch,
                    "contamination_depth": int(depth + 1),
                    "breaking_fields_matched": fields,
                    "failure_mode": "Consumer may fail validation or produce incorrect results if affected fields change without migration.",
                }
            )
            q.append((ch, depth + 1))
    return impacted[:50]

def generate_migration_report(contract_path: str, a_snapshot: str, b_snapshot: str, out_dir: str) -> tuple[str, str]:
    d = diff_snapshots(a_snapshot, b_snapshot)
    lineage = _lineage_blast_radius(contract_path)
    # Consumer impact analysis (registry + lineage).
    contract = _load_yaml(contract_path)
    dataset = None
    try:
        schema = contract.get("schema")
        if isinstance(schema, list) and schema and isinstance(schema[0], dict):
            dataset = schema[0].get("name")
    except Exception:
        dataset = None
    changes = d.get("changes") if isinstance(d.get("changes"), dict) else {}
    affected_fields = sorted(
        set((changes.get("added") or []) + (changes.get("removed") or []) + [c.get("field") for c in (changes.get("changed") or []) if isinstance(c, dict)])
    )
    consumers = _consumer_impact(dataset=str(dataset or "unknown"), affected_fields=affected_fields)

    verdict = d["compatibility"]["verdict"]
    checklist = [
        "Freeze downstream consumers and announce change window.",
        "Add producer-side dual-write (old+new fields) if possible.",
        "Backfill historical records for new required fields.",
        "Deploy consumer migration with feature flag.",
        "Validate using ValidationRunner + AI extensions on staging.",
        "Promote to production and monitor violation rates.",
    ]
    rollback = [
        "Re-deploy prior producer version that writes previous schema.",
        "Re-run generator to restore prior contracts.",
        "Re-validate and confirm violation rates return to baseline.",
        "Backfill/replay events from last known good offset if needed.",
    ]

    report = {
        "report_version": "1.0.0",
        "generated_at": _utc_now_compact(),
        "contract_path": contract_path,
        "snapshots": {"from": a_snapshot, "to": b_snapshot},
        "compatibility": d["compatibility"],
        "lineage_blast_radius": lineage,
        "per_consumer_impact": consumers,
        "exact_diff": d["diff_text"],
        "migration_checklist": checklist,
        "rollback_plan": rollback,
    }

    _safe_mkdir(out_dir)
    base = os.path.basename(contract_path).replace(".yaml", "")
    yml_path = os.path.join(out_dir, f"{base}_migration_report.yaml")
    _dump_yaml(yml_path, report)
    pdf_path = os.path.join(out_dir, f"{base}_migration_report.pdf")
    _write_pdf_from_text(pdf_path, yaml.safe_dump(report, sort_keys=False, allow_unicode=True))
    return yml_path, pdf_path


def _write_pdf_from_text(pdf_path: str, text: str) -> None:
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.units import inch
        from reportlab.pdfgen import canvas
    except Exception:
        # Dependency-free fallback (deterministic text PDF).
        try:
            from contracts.utils import write_pdf_from_text

            write_pdf_from_text(pdf_path, title="Schema Migration Report", text=text)
        except Exception:
            return
    _safe_mkdir(os.path.dirname(pdf_path))
    c = canvas.Canvas(pdf_path, pagesize=letter)
    width, height = letter
    left = 0.75 * inch
    top = height - 0.75 * inch
    y = top
    line_h = 12
    for line in text.splitlines():
        if y < 0.75 * inch:
            c.showPage()
            y = top
        c.drawString(left, y, line[:160])
        y -= line_h
    c.save()


def _latest_two_snapshots(contract_id: str, snapshots_root: str) -> tuple[str | None, str | None]:
    d = os.path.join(snapshots_root, contract_id)
    if not os.path.isdir(d):
        return None, None
    snaps = sorted([os.path.join(d, f) for f in os.listdir(d) if f.endswith(".yaml")])
    if len(snaps) < 2:
        return None, None
    return snaps[-2], snaps[-1]


def _list_snapshots(contract_id: str, snapshots_root: str, since: str | None = None) -> list[str]:
    d = os.path.join(snapshots_root, contract_id)
    if not os.path.isdir(d):
        return []
    snaps = sorted([os.path.join(d, f) for f in os.listdir(d) if f.endswith(".yaml")])
    if since:
        snaps = [s for s in snaps if os.path.basename(s) >= since]
    return snaps


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 3: Schema Evolution Analyzer")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_snap = sub.add_parser("snapshot", help="Store a schema snapshot for a contract YAML")
    p_snap.add_argument("--contract", required=True)
    p_snap.add_argument("--snapshots-root", default="schema_snapshots")
    p_snap.add_argument("--timestamp", required=False)

    p_diff = sub.add_parser("diff", help="Diff two snapshots")
    p_diff.add_argument("--from-snapshot", required=True)
    p_diff.add_argument("--to-snapshot", required=True)
    p_diff.add_argument("--out", required=False)

    p_rep = sub.add_parser("report", help="Generate migration report for two snapshots")
    p_rep.add_argument("--contract", required=True)
    p_rep.add_argument("--from-snapshot", required=True)
    p_rep.add_argument("--to-snapshot", required=True)
    p_rep.add_argument("--out-dir", default=os.path.join("reports", "schema_migration_reports"))

    p_rep_latest = sub.add_parser("report-latest", help="Generate migration report for latest 2 snapshots")
    p_rep_latest.add_argument("--contract", required=True)
    p_rep_latest.add_argument("--snapshots-root", default="schema_snapshots")
    p_rep_latest.add_argument("--out-dir", default=os.path.join("reports", "schema_migration_reports"))

    p_list = sub.add_parser("list", help="List snapshots for a contract id (optionally since timestamp)")
    p_list.add_argument("--contract-id", required=True)
    p_list.add_argument("--snapshots-root", default="schema_snapshots")
    p_list.add_argument("--since", required=False, help="Filter snapshots with basename >= since (e.g., 20260404T120000.000000Z.yaml)")

    args = parser.parse_args()

    try:
        if args.cmd == "snapshot":
            out = snapshot_contract(args.contract, args.snapshots_root, args.timestamp)
            print(out)
            return 0
        if args.cmd == "diff":
            d = diff_snapshots(args.from_snapshot, args.to_snapshot)
            if args.out:
                _dump_yaml(args.out, d)
            else:
                print(yaml.safe_dump(d, sort_keys=False, allow_unicode=True))
            return 0
        if args.cmd == "report":
            yml, pdf = generate_migration_report(args.contract, args.from_snapshot, args.to_snapshot, args.out_dir)
            print(yml)
            print(pdf)
            return 0
        if args.cmd == "report-latest":
            contract = _load_yaml(args.contract)
            cid = contract.get("id") if isinstance(contract.get("id"), str) else "unknown_contract"
            a, b = _latest_two_snapshots(cid, args.snapshots_root)
            if not a or not b:
                print("NOOP: need >=2 snapshots")
                return 0
            yml, pdf = generate_migration_report(args.contract, a, b, args.out_dir)
            print(yml)
            print(pdf)
            return 0
        if args.cmd == "list":
            snaps = _list_snapshots(args.contract_id, args.snapshots_root, args.since)
            print(yaml.safe_dump({"contract_id": args.contract_id, "snapshots": snaps}, sort_keys=False))
            return 0
        return 2
    except Exception as e:
        _safe_mkdir(os.path.join("reports", "schema_migration_reports"))
        _dump_yaml(os.path.join("reports", "schema_migration_reports", "schema_analyzer_error.yaml"), {"error": str(e)})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
