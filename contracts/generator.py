from __future__ import annotations

import os
import sys

# Allow `python contracts/generator.py ...` execution (repo root on sys.path).
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import statistics
from collections import Counter
from typing import Any

import yaml

from contracts.canonical import DatasetSpec, FieldSpec, canonical_specs
from contracts.odcs import (
    append_quality_rule,
    dataset_to_odcs_contract,
    dbt_schema_yml_for_dataset,
    minimal_quality_rules,
)
from contracts.utils import read_jsonl, safe_float, safe_int, stable_uuid_v4


def _flatten(obj: Any, prefix: str = "") -> dict[str, Any]:
    flat: dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            if isinstance(v, dict):
                flat.update(_flatten(v, key))
            else:
                flat[key] = v
    else:
        flat[prefix or "value"] = obj
    return flat


def _quantile(sorted_values: list[float], q: float) -> float | None:
    if not sorted_values:
        return None
    if q <= 0:
        return float(sorted_values[0])
    if q >= 1:
        return float(sorted_values[-1])
    i = int(round((len(sorted_values) - 1) * q))
    i = max(0, min(len(sorted_values) - 1, i))
    return float(sorted_values[i])


def _field_values(rows: list[dict[str, Any]], field: str) -> list[Any]:
    out: list[Any] = []
    for r in rows:
        out.append(r.get(field))
    return out


def _null_fraction(rows: list[dict[str, Any]], field: str) -> float:
    if not rows:
        return 1.0
    nulls = 0
    for r in rows:
        if field not in r or r.get(field) is None:
            nulls += 1
    return nulls / len(rows)


def _distinct_counts(rows: list[dict[str, Any]], field: str, max_track: int = 1000) -> tuple[int, Counter[Any]]:
    c: Counter[Any] = Counter()
    for r in rows:
        v = r.get(field)
        if v is None:
            continue
        c[v] += 1
        if len(c) > max_track:
            # stop tracking too many categories
            break
    return len(c), c


def _infer_enum_values(rows: list[dict[str, Any]], field: str, max_distinct: int = 20, min_coverage: float = 0.98) -> list[str] | None:
    # Only infer for string-valued fields.
    vals: list[str] = []
    for r in rows:
        v = r.get(field)
        if isinstance(v, str):
            vals.append(v)
        elif v is None:
            continue
        else:
            return None
    if len(vals) < 25:
        return None
    c = Counter(vals)
    distinct = len(c)
    if distinct <= 1 or distinct > max_distinct:
        return None
    coverage = sum(c.values()) / max(1, len(vals))
    if coverage < min_coverage:
        return None
    # Deterministic ordering: (-count, value)
    return [k for k, _ in sorted(c.items(), key=lambda kv: (-kv[1], kv[0]))]


def _infer_regex_prefix(rows: list[dict[str, Any]], field: str, min_coverage: float = 0.95) -> str | None:
    # Heuristic: if values look like "<prefix>-..." and few prefixes dominate, infer a regex.
    prefixes: list[str] = []
    total = 0
    for r in rows:
        v = r.get(field)
        if v is None:
            continue
        if not isinstance(v, str):
            return None
        total += 1
        if "-" in v:
            prefixes.append(v.split("-", 1)[0])
        else:
            prefixes.append("")
    if total < 25:
        return None
    c = Counter(prefixes)
    common = [p for p, _ in sorted(c.items(), key=lambda kv: (-kv[1], kv[0]))[:5] if p]
    covered = sum(n for p, n in c.items() if p in set(common))
    if not common or (covered / max(1, total)) < min_coverage:
        return None
    # escape regex meta in prefixes
    safe = [p.replace("\\", "\\\\").replace(".", "\\.").replace("+", "\\+").replace("*", "\\*").replace("?", "\\?").replace("|", "\\|").replace("(", "\\(").replace(")", "\\)") for p in common]
    return r"^(" + "|".join(safe) + r")-.+$"


def _infer_dataset_name_from_path(path: str) -> str:
    base = os.path.basename(path).replace(".jsonl", "")
    parts = os.path.normpath(path).split(os.sep)
    if "outputs" in parts:
        try:
            i = parts.index("outputs")
            if i + 2 < len(parts):
                return f"{parts[i + 1]}_{base}"
        except Exception:
            pass
    return base


def _load_rows(path: str, limit: int | None = 5000) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in read_jsonl(path):
        if len(rows) >= (limit or 10**9):
            break
        if "_parse_error" in row:
            continue
        rows.append(row)
    return rows


def _infer_field_specs(rows: list[dict[str, Any]], dataset_name: str) -> list[FieldSpec]:
    canon = canonical_specs().get(dataset_name)
    if canon:
        return canon.fields

    counters: dict[str, Counter[str]] = {}
    for r in rows:
        for k, v in r.items():
            counters.setdefault(k, Counter())[_py_type(v)] += 1

    specs: list[FieldSpec] = []
    for k, c in sorted(counters.items()):
        t = c.most_common(1)[0][0]
        specs.append(FieldSpec(k, _map_py_to_logical(k, t), required=False))
    return specs


def _py_type(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int) and not isinstance(v, bool):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "str"
    if isinstance(v, list):
        return "list"
    if isinstance(v, dict):
        return "dict"
    return "other"


def _map_py_to_logical(field: str, t: str) -> str:
    if field.endswith("_id"):
        return "uuid"
    if field.endswith("_at") or field.endswith("_time"):
        return "datetime"
    if "confidence" in field:
        return "number"
    if t == "int":
        return "integer"
    if t == "float":
        return "number"
    if t == "bool":
        return "boolean"
    if t == "list":
        return "array"
    if t == "dict":
        return "object"
    return "string"


def _profile_numeric(rows: list[dict[str, Any]], path: str) -> dict[str, float] | None:
    values: list[float] = []
    for r in rows:
        fv = safe_float(r.get(path))
        if fv is not None:
            values.append(fv)
    if len(values) < 10:
        return None
    mean = statistics.fmean(values)
    stdev = statistics.pstdev(values) if len(values) > 1 else 0.0
    return {
        "count": float(len(values)),
        "min": float(min(values)),
        "max": float(max(values)),
        "mean": float(mean),
        "stdev": float(stdev),
    }


def _profile_numeric_robust(rows: list[dict[str, Any]], field: str) -> dict[str, float] | None:
    values: list[float] = []
    for r in rows:
        fv = safe_float(r.get(field))
        if fv is not None:
            values.append(fv)
    if len(values) < 25:
        return None
    values.sort()
    p01 = _quantile(values, 0.01)
    p99 = _quantile(values, 0.99)
    if p01 is None or p99 is None:
        return None
    return {"p01": float(p01), "p99": float(p99)}


def _rule_id(dataset: str, idx: int) -> str:
    return f"{dataset}__clause_{idx:03d}"


def _dedupe_rules(rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for r in rules:
        # Dedupe by semantic signature (ignore clause_id/source metadata).
        rr = dict(r)
        rr.pop("clause_id", None)
        rr.pop("source", None)
        rr.pop("description", None)
        key = yaml.safe_dump(rr, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _rule_description(rule: dict[str, Any]) -> str:
    rtype = str(rule.get("type") or "")
    field = rule.get("field")
    if rtype == "row_count_min":
        return "Prevents empty/insufficient datasets from silently passing."
    if rtype == "type":
        return f"Prevents implicit coercion by enforcing declared type for `{field}`."
    if rtype == "not_null":
        return f"Prevents missing required values for `{field}`."
    if rtype == "uuid_v4":
        return f"Prevents malformed identifiers by enforcing UUIDv4 format for `{field}`."
    if rtype == "datetime_iso8601":
        return f"Prevents invalid timestamps by enforcing ISO8601 datetime for `{field}`."
    if rtype == "enum":
        return f"Prevents unexpected categories by enforcing accepted values for `{field}`."
    if rtype == "range":
        return f"Prevents out-of-bounds numeric values for `{field}`."
    if rtype == "range_inferred":
        return f"Prevents extreme outliers for `{field}` using inferred p01–p99 bounds."
    if rtype == "zscore_drift":
        return f"Detects distribution drift for `{field}` as data volume scales (tunable over time)."
    if rtype == "regex":
        return f"Prevents format drift by enforcing regex pattern for `{field}`."
    if rtype == "unique":
        return f"Prevents duplicate identifiers by enforcing uniqueness of `{field}`."
    if rtype == "relationships":
        return f"Prevents referential integrity breaks by requiring `{field}` to exist upstream."
    if rtype == "monotonic_increasing":
        return f"Prevents event-sourcing replay errors by enforcing monotonic `{field}`."
    if rtype == "event_payload_required":
        return "Prevents schema drift by enforcing required payload fields per event_type."
    if rtype == "event_payload_positive_amount":
        return "Prevents invalid loan requests by enforcing requested_amount_usd > 0."
    if rtype == "if_confidence_below_threshold_flag":
        return "Prevents low-confidence extractions from skipping review workflow."
    if rtype == "token_math":
        return "Prevents cost/usage analytics drift by enforcing token accounting invariants."
    if rtype == "weighted_score_math":
        return "Prevents scoring inconsistencies by enforcing weighted score computation."
    if rtype == "time_order":
        return "Prevents negative latency and timing inconsistencies in trace records."
    return "Prevents downstream failures by enforcing this contract clause."


def _infer_quality_rules(rows: list[dict[str, Any]], spec: DatasetSpec) -> list[dict[str, Any]]:
    quality = minimal_quality_rules(spec.dataset)
    clause_idx = 1

    def add(rule: dict[str, Any]) -> None:
        nonlocal clause_idx
        rule.setdefault("clause_id", _rule_id(spec.dataset, clause_idx))
        rule.setdefault("description", _rule_description(rule))
        clause_idx += 1
        append_quality_rule(quality, rule)

    add({"type": "row_count_min", "min": max(1, spec.min_records)})

    # Structural type checks (no implicit coercions).
    for f in spec.fields:
        add({"type": "type", "field": f.path, "expected": f.logical_type})

    for f in spec.fields:
        if f.required:
            add({"type": "not_null", "field": f.path})
        if f.logical_type == "uuid":
            add({"type": "uuid_v4", "field": f.path})
        if f.logical_type == "datetime":
            add({"type": "datetime_iso8601", "field": f.path})
        if f.enum:
            add({"type": "enum", "field": f.path, "values": list(f.enum)})
        if f.minimum is not None or f.maximum is not None:
            add({"type": "range", "field": f.path, "min": f.minimum, "max": f.maximum})
        if (f.minimum is None and f.maximum is None) and (f.path == "confidence" or f.path.endswith(".confidence") or "confidence" in f.path):
            add({"type": "range", "field": f.path, "min": 0.0, "max": 1.0})

    # Data-driven inference: required fields by null_fraction.
    for f in spec.fields:
        if f.required:
            continue
        nf = _null_fraction(rows, f.path)
        if nf <= 0.01:  # <1% null/missing in sample -> require
            add({"type": "not_null", "field": f.path, "source": "inferred_null_fraction<=0.01"})

    # Data-driven inference: enums for low-cardinality strings.
    for f in spec.fields:
        if f.logical_type != "string":
            continue
        inferred = _infer_enum_values(rows, f.path)
        if inferred and not f.enum:
            add({"type": "enum", "field": f.path, "values": inferred, "source": "inferred_low_cardinality"})

    # Data-driven inference: regex for prefixed identifiers (e.g., stream_id).
    for f in spec.fields:
        if f.logical_type != "string":
            continue
        pattern = _infer_regex_prefix(rows, f.path)
        if pattern:
            add({"type": "regex", "field": f.path, "pattern": pattern, "source": "inferred_prefix_regex"})

    # Data-driven inference: uniqueness for identifier-like fields.
    non_unique_ids = {
        "trace_id",
        "intent_id",
        "doc_id",
        "metadata.correlation_id",
    }
    for f in spec.fields:
        if f.logical_type != "uuid":
            continue
        if f.path in non_unique_ids:
            continue
        if _null_fraction(rows, f.path) > 0.0:
            continue
        values = [v for v in _field_values(rows, f.path) if isinstance(v, str)]
        if len(values) < 25:
            continue
        unique_ratio = (len(set(values)) / len(values)) if values else 0.0
        if unique_ratio >= 0.999:
            add({"type": "unique", "field": f.path, "source": f"inferred_unique_ratio={unique_ratio:.3f}"})

    for f in spec.fields:
        if f.logical_type in ("integer", "number"):
            prof = _profile_numeric(rows, f.path)
            if prof:
                add({"type": "zscore_drift", "field": f.path, "mean": prof["mean"], "stdev": prof["stdev"], "max_z": 3.5})

            robust = _profile_numeric_robust(rows, f.path)
            if robust:
                add(
                    {
                        "type": "range_inferred",
                        "field": f.path,
                        "min": robust["p01"],
                        "max": robust["p99"],
                        "source": "inferred_p01_p99",
                    }
                )

    # Dataset-specific strong rules (Week3 + Week5).
    if spec.dataset == "week3_extractions":
        add(
            {
                "type": "relationships",
                "field": "doc_id",
                "to_dataset": "week4_lineage_snapshots",
                "to_field": "nodes.ref.doc_id",
                "source": "lineage_contract",
            }
        )
        add(
            {
                "type": "if_confidence_below_threshold_flag",
                "confidence_field": "confidence",
                "threshold_field": "threshold",
                "flag_field": "flags.flagged_for_review",
                "source": "semantic_guardrail",
            }
        )

    if spec.dataset == "week5_events":
        add(
            {
                "type": "monotonic_increasing",
                "field": "global_position",
                "source": "event_sourcing_invariant",
            }
        )
        add(
            {
                "type": "event_payload_required",
                "event_type_field": "event_type",
                "payload_field": "payload",
                "source": "event_schema",
            }
        )
        add(
            {
                "type": "event_payload_positive_amount",
                "event_type": "ApplicationSubmitted",
                "payload_amount_field": "payload.requested_amount_usd",
                "source": "event_schema",
            }
        )

    if spec.dataset == "traces_runs":
        add({"type": "token_math", "prompt_tokens_field": "prompt_tokens", "completion_tokens_field": "completion_tokens", "total_tokens_field": "total_tokens"})
        add({"type": "time_order", "start_field": "start_time", "end_field": "end_time"})

    if spec.dataset == "week2_verdicts":
        add(
            {
                "type": "weighted_score_math",
                "correctness_field": "scores.correctness",
                "safety_field": "scores.safety",
                "style_field": "scores.style",
                "weights_field": "scores.weights",
                "weighted_score_field": "scores.weighted_score",
            }
        )

    # Dedupe and re-attach.
    impl = quality[0].get("implementation")
    if isinstance(impl, dict) and isinstance(impl.get("rules"), list):
        impl["rules"] = _dedupe_rules([r for r in impl["rules"] if isinstance(r, dict)])
    return quality


def _lineage_custom_properties(dataset_name: str) -> dict[str, Any]:
    lineage_path = os.path.join("outputs", "week4", "lineage_snapshots.jsonl")
    if not os.path.exists(lineage_path):
        return {"lineage": {"upstream": [], "downstream": [], "blast_radius": {"downstream_count": 0}}}

    upstream: set[str] = set()
    downstream: set[str] = set()
    for row in read_jsonl(lineage_path):
        if "_parse_error" in row:
            continue
        for e in row.get("edges", []) if isinstance(row.get("edges"), list) else []:
            if not isinstance(e, dict):
                continue
            evidence = e.get("evidence")
            if isinstance(evidence, dict) and evidence.get("dataset") == dataset_name:
                if isinstance(e.get("from_dataset"), str):
                    upstream.add(e["from_dataset"])
                if isinstance(e.get("to_dataset"), str):
                    downstream.add(e["to_dataset"])
    return {
        "lineage": {
            "upstream": sorted(upstream),
            "downstream": sorted(downstream),
            "blast_radius": {"downstream_count": len(downstream), "impact_hint": "derived from outputs/week4/lineage_snapshots.jsonl"},
        }
    }


def _failure_modes_for_dataset(dataset_name: str) -> list[str]:
    if dataset_name == "week3_extractions":
        return [
            "Confidence semantics drift (probability 0–1 changed to percent 0–100).",
            "Doc_id referential breaks against lineage snapshots (attribution failures).",
            "Out-of-range page_number or negative processing_time_ms (profiling corruption).",
            "Low-confidence extractions not flagged for review (workflow bypass).",
            "Statistical drift in processing_time_ms causing SLA regression.",
        ]
    if dataset_name == "week5_events":
        return [
            "Non-monotonic global_position leading to replay/order bugs.",
            "Missing required payload fields for event_type (schema drift).",
            "Invalid recorded_at timestamps causing temporal analytics errors.",
            "Malformed stream_id formats breaking routing/partitioning.",
            "Requested amount <= 0 for ApplicationSubmitted (domain invalid).",
        ]
    if dataset_name == "traces_runs":
        return [
            "Token accounting mismatch (total != prompt + completion) corrupting cost analytics.",
            "Timing inconsistencies (end_time < start_time) producing negative latencies.",
            "Provider/project/name cardinality explosion (tag drift).",
            "Status/error schema drift breaking observability pipelines.",
        ]
    if dataset_name == "week2_verdicts":
        return [
            "Weighted score math drift (stored score diverges from components).",
            "Score range violations (outside 1–5) skewing evaluation.",
            "Target_ref mismatches against upstream intents (broken linkage).",
            "Confidence scale drift (0–1 changed) causing gating errors.",
        ]
    return [
        "Unexpected null expansion on key fields (requiredness drift).",
        "Cardinality or distribution drift beyond expected bounds.",
        "Identifier format drift (regex/UUID) breaking joins and lineage.",
        "Downstream incompatibility due to unversioned schema changes.",
    ]


def _dbt_mapping_block() -> dict[str, Any]:
    return {
        "notes": "Documentation-only view of dbt tests emitted alongside this contract.",
        "mappings": {
            "not_null": "not_null",
            "unique": "unique",
            "enum": "accepted_values",
            "uuid_v4": "regex_match (uuidv4)",
            "datetime_iso8601": "regex_match (datetime prefix)",
            "range": "accepted_range (generic test macro)",
            "range_inferred": "accepted_range (generic test macro)",
            "regex": "regex_match (generic test macro)",
            "relationships": "relationships",
            "monotonic_increasing": "not available in dbt schema.yml (enforced in runner)",
            "event_payload_*": "not available in dbt schema.yml (enforced in runner)",
            "zscore_drift": "not available in dbt schema.yml (enforced in runner)",
        },
    }


def generate_contract(source: str, output_dir: str) -> tuple[str, str]:
    dataset_name = _infer_dataset_name_from_path(source)
    raw_rows = _load_rows(source)
    flat_rows = [_flatten(r) for r in raw_rows]

    canon = canonical_specs().get(dataset_name)
    if canon:
        spec = canon
    else:
        inferred = sorted(_infer_field_specs(raw_rows, dataset_name), key=lambda f: f.path)
        spec = DatasetSpec(dataset=dataset_name, fields=inferred, min_records=50)
    quality = _infer_quality_rules(flat_rows, spec)
    custom_props = _lineage_custom_properties(dataset_name)

    contract = dataset_to_odcs_contract(
        contract_id=stable_uuid_v4(f"odcs::{dataset_name}"),
        name=f"{dataset_name}_contract",
        version="1.0.0",
        status="production",
        domain="tenx",
        data_product="week7_data_contract_enforcer",
        description_purpose=f"Week7 contract for {dataset_name}",
        spec=spec,
        quality_rules=quality,
        custom_properties=custom_props,
    )
    # Contract-level risk documentation (no rule duplication).
    contract.setdefault(
        "failure_modes",
        _failure_modes_for_dataset(dataset_name),
    )
    # Documentation-only: visibility into dbt tests already emitted.
    contract.setdefault(
        "dbt_mapping",
        _dbt_mapping_block(),
    )

    os.makedirs(output_dir, exist_ok=True)
    contract_path = os.path.join(output_dir, f"{dataset_name}.yaml")
    dbt_path = os.path.join(output_dir, f"{dataset_name}.schema.yml")

    with open(contract_path, "w", encoding="utf-8", newline="\n") as f:
        yaml.safe_dump(contract, f, sort_keys=False, allow_unicode=True)

    # Map quality rules into dbt tests.
    impl = quality[0].get("implementation") if quality and isinstance(quality[0], dict) else {}
    rules = impl.get("rules") if isinstance(impl, dict) else []
    dbt_schema = dbt_schema_yml_for_dataset(spec, model_name=dataset_name, quality_rules=rules if isinstance(rules, list) else None)
    with open(dbt_path, "w", encoding="utf-8", newline="\n") as f:
        yaml.safe_dump(dbt_schema, f, sort_keys=False, allow_unicode=True)

    return contract_path, dbt_path


def main() -> int:
    parser = argparse.ArgumentParser(description="ContractGenerator: ODCS/Bitol YAML + dbt schema.yml")
    parser.add_argument("--source", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    try:
        contract_path, dbt_path = generate_contract(args.source, args.output)
        print(f"[OK] wrote contract: {contract_path}")
        print(f"[OK] wrote dbt schema: {dbt_path}")
        return 0
    except Exception as e:
        os.makedirs(args.output, exist_ok=True)
        with open(os.path.join(args.output, "generator_error.txt"), "w", encoding="utf-8", newline="\n") as f:
            f.write(str(e))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
