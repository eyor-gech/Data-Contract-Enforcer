from __future__ import annotations

from typing import Any

from contracts.canonical import DatasetSpec, FieldSpec


def field_to_property(field: FieldSpec) -> dict[str, Any]:
    p: dict[str, Any] = {
        "name": field.path,
        "logicalType": _map_logical_type(field.logical_type),
        "required": bool(field.required),
        "description": field.description,
    }
    if field.enum:
        p["validValues"] = list(field.enum)
    if field.minimum is not None:
        p["min"] = field.minimum
    if field.maximum is not None:
        p["max"] = field.maximum
    return {k: v for k, v in p.items() if v is not None}


def dataset_to_odcs_contract(
    *,
    contract_id: str,
    name: str,
    version: str,
    status: str,
    domain: str,
    data_product: str,
    description_purpose: str,
    spec: DatasetSpec,
    quality_rules: list[dict[str, Any]],
    custom_properties: dict[str, Any] | None = None,
) -> dict[str, Any]:
    contract: dict[str, Any] = {
        "apiVersion": "3.0.0",
        "kind": "DataContract",
        "id": contract_id,
        "name": name,
        "version": version,
        "status": status,
        "domain": domain,
        "dataProduct": data_product,
        "description": {"purpose": description_purpose, "limitations": None, "usage": None},
        "schema": [
            {
                "name": spec.dataset,
                "logicalType": "object",
                "properties": [field_to_property(f) for f in spec.fields],
            }
        ],
        "quality": quality_rules,
    }
    if custom_properties:
        contract["customProperties"] = custom_properties
    return contract


def _map_logical_type(t: str) -> str:
    mapping = {
        "uuid": "string",
        "datetime": "string",
        "string": "string",
        "integer": "integer",
        "number": "number",
        "boolean": "boolean",
        "object": "object",
        "array": "array",
    }
    return mapping.get(t, "string")


def minimal_quality_rules(dataset: str) -> list[dict[str, Any]]:
    return [
        {
            "type": "custom",
            "engine": "week7_enforcer",
            "implementation": {"dataset": dataset, "rules": []},
        }
    ]


def append_quality_rule(quality: list[dict[str, Any]], rule: dict[str, Any]) -> None:
    if not quality:
        return
    impl = quality[0].get("implementation")
    if isinstance(impl, dict):
        impl.setdefault("rules", []).append(rule)


def dbt_schema_yml_for_dataset(
    spec: DatasetSpec, model_name: str, quality_rules: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    quality_rules = quality_rules or []
    tests_by_field = _dbt_tests_from_quality_rules(quality_rules)
    columns = []
    for f in spec.fields:
        columns.append(
            {
                "name": f.path.replace(".", "__"),
                "description": f.description or "",
                "tests": _merge_tests(_dbt_tests_for_field(f), tests_by_field.get(f.path, [])),
            }
        )
    return {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "description": f"Auto-generated dbt schema.yml for {spec.dataset}",
                "columns": columns,
            }
        ],
    }


def _dbt_tests_for_field(f: FieldSpec) -> list[Any]:
    tests: list[Any] = []
    if f.required:
        tests.append("not_null")
    if f.enum:
        tests.append({"accepted_values": {"values": list(f.enum)}})
    if f.logical_type == "uuid":
        tests.append({"regex_match": {"regex": "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"}})
    if f.logical_type == "datetime":
        tests.append({"regex_match": {"regex": "^\\d{4}-\\d{2}-\\d{2}T"}})
    if f.minimum is not None or f.maximum is not None:
        tests.append(
            {
                "accepted_range": {
                    "min_value": f.minimum,
                    "max_value": f.maximum,
                    "inclusive": True,
                }
            }
        )
    return tests


def _merge_tests(a: list[Any], b: list[Any]) -> list[Any]:
    # Deterministic merge; remove exact duplicates.
    out: list[Any] = []
    seen: set[str] = set()

    def key(t: Any) -> str:
        return str(t)

    for t in a + b:
        k = key(t)
        if k in seen:
            continue
        seen.add(k)
        out.append(t)
    return out


def _dbt_tests_from_quality_rules(quality_rules: list[dict[str, Any]]) -> dict[str, list[Any]]:
    """
    Map Week7 quality rules to dbt schema.yml tests.
    Produces tests on flattened column names (dots become __).
    """
    by_field: dict[str, list[Any]] = {}
    for r in quality_rules:
        if not isinstance(r, dict):
            continue
        rtype = r.get("type")
        field = r.get("field")
        if not isinstance(rtype, str) or not isinstance(field, str) or not field:
            continue
        if rtype == "unique":
            by_field.setdefault(field, []).append("unique")
        if rtype == "regex":
            pattern = r.get("pattern")
            if isinstance(pattern, str) and pattern:
                by_field.setdefault(field, []).append({"regex_match": {"regex": pattern}})
        if rtype == "range_inferred":
            by_field.setdefault(field, []).append(
                {
                    "accepted_range": {
                        "min_value": r.get("min"),
                        "max_value": r.get("max"),
                        "inclusive": True,
                    }
                }
            )
        if rtype == "relationships":
            to_model = r.get("to_model")
            to_field = r.get("to_field")
            if isinstance(to_model, str) and isinstance(to_field, str):
                by_field.setdefault(field, []).append({"relationships": {"to": f"ref('{to_model}')", "field": to_field}})
    return by_field
