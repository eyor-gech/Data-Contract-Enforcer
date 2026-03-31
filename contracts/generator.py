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


def _infer_quality_rules(rows: list[dict[str, Any]], spec: DatasetSpec) -> list[dict[str, Any]]:
    quality = minimal_quality_rules(spec.dataset)

    append_quality_rule(quality, {"type": "row_count_min", "min": max(1, spec.min_records)})

    # Structural type checks (no implicit coercions).
    for f in spec.fields:
        append_quality_rule(quality, {"type": "type", "field": f.path, "expected": f.logical_type})

    for f in spec.fields:
        if f.required:
            append_quality_rule(quality, {"type": "not_null", "field": f.path})
        if f.logical_type == "uuid":
            append_quality_rule(quality, {"type": "uuid_v4", "field": f.path})
        if f.logical_type == "datetime":
            append_quality_rule(quality, {"type": "datetime_iso8601", "field": f.path})
        if f.enum:
            append_quality_rule(quality, {"type": "enum", "field": f.path, "values": list(f.enum)})
        if f.minimum is not None or f.maximum is not None:
            append_quality_rule(quality, {"type": "range", "field": f.path, "min": f.minimum, "max": f.maximum})
        if f.path == "confidence" or f.path.endswith(".confidence") or "confidence" in f.path:
            append_quality_rule(quality, {"type": "range", "field": f.path, "min": 0.0, "max": 1.0})

    for f in spec.fields:
        if f.logical_type in ("integer", "number"):
            prof = _profile_numeric(rows, f.path)
            if prof:
                append_quality_rule(
                    quality,
                    {
                        "type": "zscore_drift",
                        "field": f.path,
                        "mean": prof["mean"],
                        "stdev": prof["stdev"],
                        "max_z": 3.5,
                    },
                )

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


def generate_contract(source: str, output_dir: str) -> tuple[str, str]:
    dataset_name = _infer_dataset_name_from_path(source)
    raw_rows = _load_rows(source)
    flat_rows = [_flatten(r) for r in raw_rows]

    canon = canonical_specs().get(dataset_name)
    spec = canon or DatasetSpec(dataset=dataset_name, fields=_infer_field_specs(raw_rows, dataset_name), min_records=50)
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

    os.makedirs(output_dir, exist_ok=True)
    contract_path = os.path.join(output_dir, f"{dataset_name}.yaml")
    dbt_path = os.path.join(output_dir, f"{dataset_name}.schema.yml")

    with open(contract_path, "w", encoding="utf-8", newline="\n") as f:
        yaml.safe_dump(contract, f, sort_keys=False, allow_unicode=True)

    dbt_schema = dbt_schema_yml_for_dataset(spec, model_name=dataset_name)
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
