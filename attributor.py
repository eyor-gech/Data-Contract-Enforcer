from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict, deque
from datetime import datetime
from typing import Any

import yaml

from contracts.utils import now_utc_iso, stable_uuid_v4


def _safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _read_jsonl(path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    rows.append(obj)
            except Exception:
                continue
    return rows


def _lineage_graph(lineage_path: str) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    parents: dict[str, set[str]] = defaultdict(set)
    children: dict[str, set[str]] = defaultdict(set)
    for row in _read_jsonl(lineage_path):
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


def _registry_graph(registry_path: str) -> tuple[dict[str, set[str]], dict[str, set[str]], dict[str, Any]]:
    parents: dict[str, set[str]] = defaultdict(set)
    children: dict[str, set[str]] = defaultdict(set)
    meta: dict[str, Any] = {"subscriptions": []}
    if not os.path.exists(registry_path):
        return parents, children, meta
    try:
        reg = yaml.safe_load(open(registry_path, "r", encoding="utf-8")) or {}
    except Exception:
        return parents, children, meta
    subs = reg.get("subscriptions")
    if not isinstance(subs, list):
        return parents, children, meta
    meta["subscriptions"] = subs
    for s in subs:
        if not isinstance(s, dict):
            continue
        frm = s.get("from")
        to = s.get("to")
        if isinstance(frm, str) and isinstance(to, str) and frm and to:
            parents[to].add(frm)
            children[frm].add(to)
    return parents, children, meta


def _dependency_graph(
    *,
    registry_path: str,
    lineage_path: str,
) -> tuple[dict[str, set[str]], dict[str, set[str]], dict[str, Any]]:
    # Registry must be applied before lineage traversal (rubric requirement).
    r_par, r_ch, meta = _registry_graph(registry_path)
    l_par, l_ch = _lineage_graph(lineage_path)
    parents: dict[str, set[str]] = defaultdict(set)
    children: dict[str, set[str]] = defaultdict(set)
    for k, vs in r_par.items():
        parents[k].update(vs)
    for k, vs in r_ch.items():
        children[k].update(vs)
    for k, vs in l_par.items():
        parents[k].update(vs)
    for k, vs in l_ch.items():
        children[k].update(vs)
    return parents, children, meta


def blame_chain(dataset: str, lineage_path: str, max_depth: int = 10, registry_path: str | None = None) -> list[str]:
    parents, _, _ = _dependency_graph(registry_path=registry_path or os.path.join("contract_registry", "subscriptions.yaml"), lineage_path=lineage_path)
    chain = [dataset]
    seen = {dataset}
    cur = dataset
    for _ in range(max_depth):
        ps = sorted([p for p in parents.get(cur, set()) if p not in seen])
        if not ps:
            break
        cur = ps[0]
        chain.append(cur)
        seen.add(cur)
    return list(reversed(chain))


def blast_radius(dataset: str, lineage_path: str, max_nodes: int = 50, registry_path: str | None = None) -> list[str]:
    _, children, _ = _dependency_graph(registry_path=registry_path or os.path.join("contract_registry", "subscriptions.yaml"), lineage_path=lineage_path)
    out: list[str] = []
    q = deque([dataset])
    seen = {dataset}
    while q and len(out) < max_nodes:
        cur = q.popleft()
        for ch in sorted(children.get(cur, set())):
            if ch in seen:
                continue
            seen.add(ch)
            out.append(ch)
            q.append(ch)
    return out


def blast_radius_detailed(dataset: str, lineage_path: str, max_nodes: int = 50, registry_path: str | None = None) -> list[dict[str, Any]]:
    _, children, _ = _dependency_graph(registry_path=registry_path or os.path.join("contract_registry", "subscriptions.yaml"), lineage_path=lineage_path)
    out: list[dict[str, Any]] = []
    q = deque([(dataset, 0)])
    seen = {dataset}
    while q and len(out) < max_nodes:
        cur, depth = q.popleft()
        for ch in sorted(children.get(cur, set())):
            if ch in seen:
                continue
            seen.add(ch)
            out.append({"dataset": ch, "contamination_depth": int(depth + 1)})
            q.append((ch, depth + 1))
    return out


def _days_since_repo_commit(dataset: str) -> int:
    # Heuristic mapping: week3_extractions -> CLONED_ROOT/Week 3
    cloned_root = os.environ.get("CLONED_ROOT") or r"C:\Users\Eyor.G\Documents\Cloned"
    wk = None
    if dataset.startswith("week") and "_" in dataset:
        wk = dataset.split("_", 1)[0].replace("week", "").strip()
    repo_dir = os.path.join(cloned_root, f"Week {wk}") if wk else cloned_root
    try:
        import git

        repo = git.Repo(repo_dir, search_parent_directories=True)
        dt = repo.head.commit.committed_datetime
        now = datetime.utcnow().replace(tzinfo=dt.tzinfo) if dt.tzinfo else datetime.utcnow()
        return max(0, int((now - dt).days))
    except Exception:
        return 0


def _candidate_confidence(days_since_commit: int, lineage_hops: int) -> float:
    base = 1.0 - (days_since_commit * 0.1)
    conf = base - (0.2 * lineage_hops)
    return float(max(0.0, min(1.0, conf)))


def _producer_files_from_registry(meta: dict[str, Any], dataset: str) -> list[str]:
    subs = meta.get("subscriptions") if isinstance(meta, dict) else None
    if not isinstance(subs, list):
        return []
    files: set[str] = set()
    for s in subs:
        if not isinstance(s, dict):
            continue
        if str(s.get("from") or "") != dataset:
            continue
        pf = s.get("producer_files")
        if isinstance(pf, list):
            for p in pf:
                if isinstance(p, str) and p.strip():
                    files.add(p.strip())
    return sorted(files)


def _infer_repo_dir_for_dataset(dataset: str) -> str:
    cloned_root = os.environ.get("CLONED_ROOT") or r"C:\Users\Eyor.G\Documents\Cloned"
    wk = None
    if dataset.startswith("week") and "_" in dataset:
        wk = dataset.split("_", 1)[0].replace("week", "").strip()
    repo_dir = os.path.join(cloned_root, f"Week {wk}") if wk else cloned_root
    return repo_dir


def _candidate_files_heuristic(dataset: str, repo_dir: str, hint_field: str | None = None) -> list[str]:
    """
    Deterministic fallback: pick likely producer files based on filename keywords.
    """
    keywords = set()
    ds = dataset.lower()
    for k in ds.replace("week", "").replace("_", " ").split():
        if k:
            keywords.add(k)
    if "extractions" in ds or "extraction" in ds:
        keywords.update({"extract", "extraction"})
    if "events" in ds or "event" in ds:
        keywords.update({"event", "store"})
    if "verdict" in ds:
        keywords.update({"verdict", "judge", "score"})
    if "trace" in ds:
        keywords.update({"trace", "langsmith"})
    if isinstance(hint_field, str) and hint_field.strip():
        for seg in hint_field.replace(".", "_").replace("-", "_").split("_"):
            seg = seg.strip().lower()
            if seg:
                keywords.add(seg)

    try:
        import git

        repo = git.Repo(repo_dir, search_parent_directories=True)
        files = [f for f in repo.git.ls_files().splitlines() if f]
    except Exception:
        return []

    scored: list[tuple[int, str]] = []
    for f in files:
        lf = f.lower()
        if not any(lf.endswith(ext) for ext in (".py", ".sql", ".ts", ".js", ".yaml", ".yml")):
            continue
        score = 0
        for kw in keywords:
            if kw and kw in lf:
                score += 1
        if score > 0:
            scored.append((score, f))
    scored.sort(key=lambda t: (-t[0], t[1]))
    return [p for _, p in scored[:10]]


def _git_last_commit(repo_dir: str, rel_path: str) -> dict[str, Any] | None:
    try:
        import git

        repo = git.Repo(repo_dir, search_parent_directories=True)
        commits = list(repo.iter_commits(paths=rel_path, max_count=1))
        if not commits:
            return None
        c = commits[0]
        dt = c.committed_datetime
        ts = dt.isoformat()
        return {
            "commit_hash": c.hexsha,
            "author": getattr(c.author, "name", None) or str(c.author),
            "commit_timestamp": ts,
            "commit_message": (c.message or "").strip().splitlines()[0][:200],
        }
    except Exception:
        return None


def rank_candidates(violation_report: dict[str, Any], lineage_path: str, registry_path: str) -> list[dict[str, Any]]:
    vios = violation_report.get("violations") if isinstance(violation_report, dict) else None
    if not isinstance(vios, list):
        return []
    # Aggregate by root cause dataset.
    agg: dict[str, dict[str, Any]] = {}
    for v in vios:
        if not isinstance(v, dict):
            continue
        ds = str(v.get("root_cause") or "")
        if not ds:
            continue
        rec = agg.setdefault(ds, {"dataset": ds, "violation_count": 0, "severities": defaultdict(int), "fields": defaultdict(int)})
        rec["violation_count"] += int(v.get("count") or 1)
        sev = str(v.get("severity") or "LOW").upper()
        rec["severities"][sev] += 1
        fld = str(v.get("field") or "")
        if fld:
            rec["fields"][fld] += 1
    candidates: list[dict[str, Any]] = []
    for ds, rec in agg.items():
        chain = blame_chain(ds, lineage_path=lineage_path, registry_path=registry_path)
        hops = max(0, len(chain) - 1)
        repo_dir = _infer_repo_dir_for_dataset(ds)
        days = _days_since_repo_commit(ds)
        base_conf = _candidate_confidence(days, hops)

        # Deterministic file selection: registry-provided files first, then heuristic fallback.
        _, _, meta = _dependency_graph(registry_path=registry_path, lineage_path=lineage_path)
        files = _producer_files_from_registry(meta, ds)
        if not files:
            hint = None
            try:
                fields = rec.get("fields")
                if isinstance(fields, dict) and fields:
                    hint = sorted(fields.items(), key=lambda t: (-int(t[1]), str(t[0])))[0][0]
            except Exception:
                hint = None
            files = _candidate_files_heuristic(ds, repo_dir, hint_field=hint)

        if not files:
            candidates.append(
                {
                    "dataset": ds,
                    "file": None,
                    "violation_count": int(rec["violation_count"]),
                    "lineage_hops": int(hops),
                    "days_since_commit": int(days),
                    "confidence_score": float(base_conf),
                    "commit_hash": None,
                    "author": None,
                    "commit_timestamp": None,
                    "commit_message": None,
                    "blame_chain": chain,
                }
            )
            continue

        for rel in files[:10]:
            info = _git_last_commit(repo_dir, rel)
            # If git metadata missing, still return candidate deterministically.
            cand = {
                "dataset": ds,
                "file": rel,
                "violation_count": int(rec["violation_count"]),
                "lineage_hops": int(hops),
                "days_since_commit": int(days),
                "confidence_score": float(base_conf),
                "commit_hash": (info or {}).get("commit_hash"),
                "author": (info or {}).get("author"),
                "commit_timestamp": (info or {}).get("commit_timestamp"),
                "commit_message": (info or {}).get("commit_message"),
                "blame_chain": chain,
            }
            candidates.append(cand)

    candidates.sort(key=lambda c: (-float(c.get("confidence_score") or 0.0), str(c.get("file") or ""), str(c.get("dataset") or "")))
    return candidates[:5]


def main() -> int:
    parser = argparse.ArgumentParser(description="Attribution: lineage-aware blame chains + blast radius.")
    parser.add_argument("--dataset", required=True, help="Dataset name (e.g., week3_extractions)")
    parser.add_argument("--lineage", default=os.path.join("outputs", "week4", "lineage_snapshots.jsonl"))
    parser.add_argument("--registry", default=os.path.join("contract_registry", "subscriptions.yaml"))
    parser.add_argument("--violation-report", required=False, help="Optional validation report json to attach")
    parser.add_argument("--out", default=os.path.join("enforcer_report", "attribution.json"))
    args = parser.parse_args()

    try:
        detected_at = now_utc_iso()
        chain = blame_chain(args.dataset, args.lineage, registry_path=args.registry)
        radius = blast_radius(args.dataset, args.lineage, registry_path=args.registry)
        radius_detailed = blast_radius_detailed(args.dataset, args.lineage, registry_path=args.registry)
        pipelines: list[dict[str, Any]] = []
        try:
            _, _, meta = _dependency_graph(registry_path=args.registry, lineage_path=args.lineage)
            for s in meta.get("subscriptions") if isinstance(meta, dict) else []:
                if isinstance(s, dict) and str(s.get("from") or "") == args.dataset:
                    pipelines.append({"from": s.get("from"), "to": s.get("to"), "subscriber_id": s.get("subscriber_id"), "validation_mode": s.get("validation_mode")})
        except Exception:
            pipelines = []

        violation_id = stable_uuid_v4(f"{args.dataset}:{args.violation_report or ''}:{detected_at}")
        check_id = None
        payload: dict[str, Any] = {
            "violation_id": violation_id,
            "check_id": check_id,
            "detected_at": detected_at,
            "dataset": args.dataset,
            "blame_chain_path": chain,
            "blame_chain": [],
            "blast_radius": {"datasets": radius, "pipelines": pipelines, "detailed": radius_detailed},
        }
        if args.violation_report and os.path.exists(args.violation_report):
            rep = json.load(open(args.violation_report, "r", encoding="utf-8"))
            payload["violation_report"] = rep
            candidates = rank_candidates(rep, args.lineage, args.registry)
            payload["blame_chain"] = candidates
            payload["ranked_candidates"] = candidates
            try:
                v0 = (rep.get("violations") or [None])[0] if isinstance(rep, dict) else None
                if isinstance(v0, dict):
                    payload["check_id"] = v0.get("clause_id") or v0.get("field")
            except Exception:
                pass
        _safe_mkdir(os.path.dirname(args.out))
        json.dump(payload, open(args.out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        print(args.out)
        return 0
    except Exception as e:
        _safe_mkdir(os.path.dirname(args.out))
        json.dump({"error": str(e)}, open(args.out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
