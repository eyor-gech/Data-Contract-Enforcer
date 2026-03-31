from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
from dataclasses import dataclass

from contracts.canonical import REQUIRED_DATASETS
from contracts.canonical import canonical_specs
from contracts.utils import count_lines, parse_iso8601, read_jsonl, top_level_keys_sample


@dataclass(frozen=True)
class DiscoveryRow:
    file: str
    found: str
    absolute_path: str
    line_count: int
    top_level_keys: list[str]
    status: str


def _status_for(path: str) -> str:
    if not os.path.exists(path):
        return "MISSING"
    if count_lines(path) <= 0:
        return "INVALID"
    return "VALID"


def _dataset_key_for_required_path(rel: str) -> str | None:
    mapping = {
        "outputs/week1/intent_records.jsonl": "week1_intent_records",
        "outputs/week2/verdicts.jsonl": "week2_verdicts",
        "outputs/week3/extractions.jsonl": "week3_extractions",
        "outputs/week4/lineage_snapshots.jsonl": "week4_lineage_snapshots",
        "outputs/week5/events.jsonl": "week5_events",
        "outputs/traces/runs.jsonl": "traces_runs",
    }
    return mapping.get(rel.replace("\\", "/"))


def _get_path(obj: dict, path: str):
    cur: Any = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _type_ok(expected: str, v) -> bool:
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
        return isinstance(v, str) and (parse_iso8601(v) is not None)
    return True


def _validate_against_canonical(abs_path: str, dataset_key: str) -> bool:
    spec = canonical_specs().get(dataset_key)
    if spec is None:
        return True
    # record count gate
    if count_lines(abs_path) < spec.min_records:
        return False
    # sample structural validation
    bad = 0
    seen = 0
    for r in read_jsonl(abs_path):
        if "_parse_error" in r:
            bad += 1
            continue
        seen += 1
        for f in spec.fields:
            if not f.required:
                continue
            v = _get_path(r, f.path)
            if not _type_ok(f.logical_type, v):
                bad += 1
                break
        if seen >= 50:
            break
    return bad == 0


def discover(root: str, extra_roots: list[str]) -> tuple[list[str], list[str]]:
    jsonl_files: list[str] = []
    outputs_dirs: list[str] = []

    def walk(r: str) -> None:
        for dirpath, dirnames, filenames in os.walk(r):
            for d in dirnames:
                if d == "outputs":
                    outputs_dirs.append(os.path.join(dirpath, d))
            for fn in filenames:
                if fn.lower().endswith(".jsonl"):
                    jsonl_files.append(os.path.join(dirpath, fn))

    walk(root)
    for r in extra_roots:
        if os.path.exists(r):
            walk(r)
    return sorted(set(jsonl_files)), sorted(set(outputs_dirs))


def build_required_table(repo_root: str) -> list[DiscoveryRow]:
    rows: list[DiscoveryRow] = []
    for rel in REQUIRED_DATASETS:
        abs_path = os.path.abspath(os.path.join(repo_root, rel))
        exists = os.path.exists(abs_path)
        dataset_key = _dataset_key_for_required_path(rel)
        schema_ok = _validate_against_canonical(abs_path, dataset_key) if (exists and dataset_key) else exists
        status = "MISSING" if not exists else ("VALID" if schema_ok else "INVALID")
        rows.append(
            DiscoveryRow(
                file=rel,
                found="YES" if exists else "NO",
                absolute_path=abs_path if exists else "",
                line_count=count_lines(abs_path) if exists else 0,
                top_level_keys=top_level_keys_sample(abs_path) if exists else [],
                status=status,
            )
        )
    return rows


def render_md(required_rows: list[DiscoveryRow], jsonl_files: list[str], outputs_dirs: list[str]) -> str:
    lines: list[str] = []
    lines.append("# DATA DISCOVERY (Week 7)\n")
    lines.append("## Required Datasets\n")
    lines.append("| File | Found | Absolute Path | Line Count | Top-Level Keys | Status |")
    lines.append("|---|---:|---|---:|---|---|")
    for r in required_rows:
        keys = ", ".join(r.top_level_keys[:30])
        lines.append(
            f"| `{r.file}` | {r.found} | `{r.absolute_path}` | {r.line_count} | `{keys}` | {r.status} |"
        )

    lines.append("\n## Repos Under Cloned Root\n")
    lines.append("### outputs/ Directories\n")
    for d in outputs_dirs:
        lines.append(f"- `{d}`")
    lines.append("\n### .jsonl Files\n")
    for p in jsonl_files:
        lines.append(f"- `{p}`")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cloned-root", default=r"C:\\Users\\Eyor.G\\Documents\\Cloned")
    parser.add_argument("--repo-root", default=os.getcwd())
    parser.add_argument("--out", default="DATA_DISCOVERY.md")
    args = parser.parse_args()

    jsonl_files, outputs_dirs = discover(args.cloned_root, [args.repo_root])
    required_rows = build_required_table(args.repo_root)
    md = render_md(required_rows, jsonl_files, outputs_dirs)
    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        f.write(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
