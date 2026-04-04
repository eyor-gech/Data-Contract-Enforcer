from __future__ import annotations

import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env", override=False)
load_dotenv(dotenv_path=REPO_ROOT / ".env", override=False)


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _safe_read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                rows.append(obj)
        except Exception as e:
            rows.append({"_parse_error": str(e), "_line_no": line_no})
    return rows


def _friendly_dataset_label(dataset: str) -> str:
    mapping = {
        "week1_intent_records": "Week 1 — Intent Records",
        "week2_verdicts": "Week 2 — LLM Verdicts",
        "week3_extractions": "Week 3 — Extractions",
        "week4_lineage_snapshots": "Week 4 — Lineage Map",
        "week5_events": "Week 5 — Event Store",
        "traces_runs": "LangSmith — Monitoring",
    }
    return mapping.get(dataset, dataset)


def _validation_status_for_dataset(dataset: str) -> str | None:
    # Runner report names are based on the input .jsonl basename.
    # In this repo those are typically:
    # - outputs/week1/intent_records.jsonl -> validation_reports/intent_records.json
    # - outputs/week2/verdicts.jsonl       -> validation_reports/verdicts.json
    # - outputs/week3/extractions.jsonl    -> validation_reports/week3_extractions.json (Week7 bootstrap naming)
    # - outputs/week5/events.jsonl         -> validation_reports/week5_events.json
    # - outputs/traces/runs.jsonl          -> validation_reports/traces_runs.json
    dataset_to_report = {
        "week1_intent_records": "intent_records",
        "week2_verdicts": "verdicts",
        "week3_extractions": "week3_extractions",
        "week4_lineage_snapshots": "phase0_lineage" ,  # may not exist; left for completeness
        "week5_events": "week5_events",
        "traces_runs": "traces_runs",
    }
    base = dataset_to_report.get(dataset, dataset)
    report_candidates = [REPO_ROOT / "validation_reports" / f"{base}.json"]
    for p in report_candidates:
        if p.exists():
            rep = _safe_read_json(p)
            st = rep.get("status")
            if isinstance(st, str):
                return st.upper()
    return None


def _load_report_data(refresh: bool) -> dict[str, Any]:
    """
    Loads `enforcer_report/report_data.json` if present; optionally regenerates it using the existing module.
    """
    out_dir = REPO_ROOT / "enforcer_report"
    report_json = out_dir / "report_data.json"

    if refresh or (not report_json.exists()):
        try:
            from scripts.report_generator import generate_report  # type: ignore

            generate_report(str(out_dir))
        except Exception:
            # If generation fails, we still try to read whatever is there.
            pass

    return _safe_read_json(report_json)


def _top_risks_from_report(report_data: dict[str, Any]) -> list[str]:
    # Prefer prebuilt narrative if present (already plain-language).
    narrative = report_data.get("narrative")
    if isinstance(narrative, list):
        items = [str(x) for x in narrative if isinstance(x, (str, int, float))]
        return items[:3]

    violations = report_data.get("violations")
    if isinstance(violations, list):
        out: list[str] = []
        for v in violations:
            if not isinstance(v, dict):
                continue
            sev = str(v.get("severity") or "LOW").upper()
            field = str(v.get("field") or "field")
            msg = str(v.get("message") or "contract violation")
            out.append(f"[{sev}] {field} — {msg}")
        return out[:3]
    return []


def _contract_promises_for_dataset(dataset: str) -> list[str]:
    """
    Produces plain-language “promises” using canonical specs (Week 7).
    """
    try:
        from contracts.canonical import canonical_specs  # type: ignore

        spec = canonical_specs().get(dataset)
        if spec is None:
            return []
        required = [f.path for f in spec.fields if f.required]
        highlights = required[:6]
        out = [
            f"Includes the required business identifiers and timestamps for {dataset.replace('_', ' ')}.",
            f"Required fields are present (examples: {', '.join(highlights)}).",
            "Values stay within agreed bounds (enums, numeric ranges, and cross-system links).",
        ]
        return out
    except Exception:
        return []


def _lineage_edges() -> list[tuple[str, str]]:
    path = REPO_ROOT / "outputs" / "week4" / "lineage_snapshots.jsonl"
    edges: set[tuple[str, str]] = set()
    for row in _safe_read_jsonl(path):
        if row.get("_parse_error"):
            continue
        for e in row.get("edges") or []:
            if not isinstance(e, dict):
                continue
            frm = e.get("from_dataset")
            to = e.get("to_dataset")
            if isinstance(frm, str) and isinstance(to, str) and frm and to:
                edges.add((frm, to))

    # Ensure the expected Week1–Week5 + LangSmith flow exists (for demo clarity).
    canonical_flow = [
        ("week1_intent_records", "week2_verdicts"),
        ("week2_verdicts", "week3_extractions"),
        ("week3_extractions", "week4_lineage_snapshots"),
        ("week4_lineage_snapshots", "week5_events"),
        ("week5_events", "traces_runs"),
    ]
    for e in canonical_flow:
        edges.add(e)
    return sorted(edges)


app = FastAPI(title="Data Contract Enforcer — Adapter API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def api_health(refresh: bool = Query(default=False, description="Regenerate report_data.json if missing")):
    report = _load_report_data(refresh=refresh)
    score = report.get("data_health_score")
    score = int(score) if isinstance(score, (int, float)) else 0
    narrative_list = report.get("narrative")
    narrative = ""
    if isinstance(narrative_list, list) and narrative_list:
        narrative = " ".join(str(x) for x in narrative_list[:5])
    else:
        narrative = "Overall health is computed from contract violations and AI checks across the pipeline."

    return {
        "score": max(0, min(100, score)),
        "narrative": narrative,
        "top_risks": _top_risks_from_report(report),
        "generated_at": report.get("generated_at") or _now_iso_z(),
    }


@app.get("/api/executive-llm-summary")
def api_executive_llm_summary(refresh: bool = Query(default=False, description="Refresh underlying report_data.json first")):
    """
    Executive Summary LLM feature.
    Produces an executive-friendly brief derived from existing module outputs (no code changes to core modules).

    Requires:
    - OPENROUTER_API_KEY in `adapter_api/.env` (or repo root `.env`)
    """
    report = _load_report_data(refresh=refresh)
    score = report.get("data_health_score")
    score = int(score) if isinstance(score, (int, float)) else 0

    # Build compact evidence for the LLM (avoid dumping huge payloads).
    violations = report.get("violations") if isinstance(report.get("violations"), list) else []
    top_violations: list[dict[str, Any]] = []
    for v in violations[:40]:
        if not isinstance(v, dict):
            continue
        top_violations.append(
            {
                "severity": v.get("severity"),
                "root_cause": v.get("root_cause"),
                "field": v.get("field"),
                "message": v.get("message"),
                "count": v.get("count"),
            }
        )

    schema_reports = report.get("schema_migration_reports")
    schema_summaries: list[dict[str, Any]] = []
    if isinstance(schema_reports, list):
        for r in schema_reports[:5]:
            if not isinstance(r, dict):
                continue
            rep = r.get("report")
            if not isinstance(rep, dict):
                continue
            comp = rep.get("compatibility")
            schema_summaries.append(
                {
                    "source": r.get("source"),
                    "compatibility_verdict": (comp or {}).get("verdict") if isinstance(comp, dict) else None,
                    "top_reasons": ((comp or {}).get("reasons") if isinstance(comp, dict) else None) or [],
                }
            )

    prompt = {
        "generated_at": report.get("generated_at") or _now_iso_z(),
        "health_score_0_100": max(0, min(100, score)),
        "top_violations": top_violations,
        "schema_evolution": schema_summaries,
    }

    try:
        from adapter_api.openrouter_client import OpenRouterError, chat_completion  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to import OpenRouter client: {e}")

    system = (
        "You are an executive-facing data reliability advisor. "
        "Write short, non-technical summaries. Avoid jargon (schema, json, nulls) unless unavoidable. "
        "Use clear, business-impact language."
    )
    user = (
        "Using this reliability evidence, produce:\n"
        "1) A 3–5 sentence executive summary narrative.\n"
        "2) Top 3 business risks (bullets, plain language).\n"
        "3) Top 3 recommended actions (bullets, pragmatic).\n"
        "Output STRICT JSON with keys: narrative (string), risks (string[]), actions (string[]).\n"
        f"Evidence:\n{json.dumps(prompt, ensure_ascii=False)}"
    )

    try:
        raw = chat_completion(messages=[{"role": "system", "content": system}, {"role": "user", "content": user}])
    except OpenRouterError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LLM call failed: {e}")

    content = None
    try:
        content = (((raw.get("choices") or [])[0] or {}).get("message") or {}).get("content")
    except Exception:
        content = None

    if not isinstance(content, str) or not content.strip():
        raise HTTPException(status_code=500, detail="OpenRouter returned empty content.")

    # Try to parse JSON; if the model returns text, wrap it safely.
    try:
        parsed = json.loads(content)
        if isinstance(parsed, dict):
            narrative = parsed.get("narrative")
            risks = parsed.get("risks")
            actions = parsed.get("actions")
            if isinstance(narrative, str) and isinstance(risks, list) and isinstance(actions, list):
                return {
                    "narrative": narrative,
                    "risks": [str(r) for r in risks][:3],
                    "actions": [str(a) for a in actions][:3],
                    "model": raw.get("model"),
                    "generated_at": _now_iso_z(),
                }
    except Exception:
        pass

    return {
        "narrative": content.strip(),
        "risks": [],
        "actions": [],
        "model": raw.get("model"),
        "generated_at": _now_iso_z(),
    }


@app.get("/api/report/pdf")
def api_report_pdf(refresh: bool = Query(default=False, description="Regenerate PDF if missing")):
    out_dir = REPO_ROOT / "enforcer_report"
    pdf_path = out_dir / "enforcer_report.pdf"
    if refresh or (not pdf_path.exists()):
        try:
            from scripts.report_generator import generate_report  # type: ignore

            generate_report(str(out_dir))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to generate report PDF: {e}")

    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="Report PDF not found.")
    return FileResponse(path=str(pdf_path), media_type="application/pdf", filename="enforcer_report.pdf")


@app.get("/api/contract-status")
def api_contract_status():
    edges = _lineage_edges()
    nodes = sorted({n for e in edges for n in e})
    node_payload = [{"id": n, "label": _friendly_dataset_label(n)} for n in nodes]

    edge_payload: list[dict[str, Any]] = []
    for frm, to in edges:
        st = _validation_status_for_dataset(to)
        status = "UNKNOWN"
        if st == "PASS":
            status = "OK"
        elif st == "FAIL":
            status = "BROKEN"
        edge_payload.append(
            {
                "id": f"{frm}->{to}",
                "source": frm,
                "target": to,
                "status": status,
                "promises": _contract_promises_for_dataset(to),
            }
        )

    return {"nodes": node_payload, "edges": edge_payload}


@app.get("/api/blame-chain")
def api_blame_chain(
    dataset: str = Query(default="week3_extractions", description="Dataset key used in outputs/week4 lineage"),
):
    try:
        from attributor import blame_chain as _blame_chain  # type: ignore
        from attributor import blast_radius as _blast_radius  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to import attribution module: {e}")

    lineage_path = REPO_ROOT / "outputs" / "week4" / "lineage_snapshots.jsonl"
    chain = _blame_chain(dataset, str(lineage_path))
    radius = _blast_radius(dataset, str(lineage_path))

    report = _load_report_data(refresh=False)
    violations: list[dict[str, Any]] = []
    for v in report.get("violations") or []:
        if not isinstance(v, dict):
            continue
        root = str(v.get("root_cause") or "")
        if root and root != dataset:
            continue
        violations.append(
            {
                "timestamp": report.get("generated_at") or _now_iso_z(),
                "system": root or dataset,
                "message": str(v.get("message") or "Contract violation detected."),
                "severity": str(v.get("severity") or "LOW").upper(),
                "downstream": radius[:8],
            }
        )

    breaking = any(v.get("severity") in ("CRITICAL", "HIGH") for v in violations)
    alert = (
        f"Breaking change risk detected in {dataset}. Review blast radius and remediation steps."
        if breaking
        else f"No breaking change flagged for {dataset}."
    )

    return {
        "breaking_change": breaking,
        "alert": alert,
        "blame_chain": chain,
        "violations": violations,
        "blast_radius": {"source": dataset, "affected": radius},
    }


@app.get("/api/schema-diff")
def api_schema_diff(
    contract: str = Query(default="generated_contracts/week3_extractions.yaml", description="Contract YAML path (repo-relative)"),
):
    contract_path = (REPO_ROOT / contract).resolve()
    if not contract_path.exists():
        raise HTTPException(status_code=404, detail=f"Contract not found: {contract}")

    try:
        from contracts import schema_analyzer  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to import schema analyzer: {e}")

    # Load contract id to locate snapshots.
    try:
        import yaml  # type: ignore

        contract_obj = yaml.safe_load(contract_path.read_text(encoding="utf-8")) or {}
        contract_id = contract_obj.get("id") if isinstance(contract_obj, dict) else None
    except Exception:
        contract_id = None

    if not isinstance(contract_id, str) or not contract_id:
        raise HTTPException(status_code=500, detail="Contract id missing/invalid; cannot locate snapshots.")

    snap_dir = REPO_ROOT / "schema_snapshots" / contract_id
    snaps = sorted([p for p in snap_dir.glob("*.yaml")]) if snap_dir.exists() else []
    if len(snaps) < 2:
        raise HTTPException(
            status_code=409,
            detail=f"Need >= 2 snapshots under schema_snapshots/{contract_id}. Run `python contracts/schema_analyzer.py snapshot --contract {contract}` twice.",
        )

    a_path, b_path = snaps[-2], snaps[-1]
    diff = schema_analyzer.diff_snapshots(str(a_path), str(b_path))

    verdict = (diff.get("compatibility") or {}).get("verdict")
    verdict = str(verdict).upper()
    ui_verdict = "SAFE" if verdict == "COMPATIBLE" else ("DANGEROUS" if verdict == "BREAKING" else "UNKNOWN")

    reasons = (diff.get("compatibility") or {}).get("reasons") or []
    checklist = [
        "Confirm downstream consumers are ready for the change.",
        "Roll out with a feature flag and monitor violations.",
        "Backfill and replay if required fields changed.",
    ]
    if isinstance(reasons, list) and reasons:
        checklist = [f"Review: {str(r)}" for r in reasons[:8]] + checklist

    return {
        "before": _safe_read_json(a_path) if a_path.suffix == ".json" else a_path.read_text(encoding="utf-8"),
        "after": _safe_read_json(b_path) if b_path.suffix == ".json" else b_path.read_text(encoding="utf-8"),
        "diff": diff.get("diff_text"),
        "verdict": ui_verdict,
        "migration_checklist": checklist,
        "from_snapshot": str(a_path.relative_to(REPO_ROOT)),
        "to_snapshot": str(b_path.relative_to(REPO_ROOT)),
    }


@app.get("/api/ai-drift")
def api_ai_drift(refresh: bool = Query(default=False, description="Run ai_extensions if missing")):
    report_path = REPO_ROOT / "validation_reports" / "ai_extensions.json"

    if refresh or (not report_path.exists()):
        try:
            from contracts.ai_extensions import run_all  # type: ignore

            run_all(
                week3_extractions=str(REPO_ROOT / "outputs" / "week3" / "extractions.jsonl"),
                traces_runs=str(REPO_ROOT / "outputs" / "traces" / "runs.jsonl"),
                week2_verdicts=str(REPO_ROOT / "outputs" / "week2" / "verdicts.jsonl"),
                week1_intents=str(REPO_ROOT / "outputs" / "week1" / "intent_records.jsonl"),
                out_report=str(report_path),
            )
        except Exception:
            pass

    data = _safe_read_json(report_path)
    drift = (((data.get("results") or {}).get("embedding_drift")) if isinstance(data.get("results"), dict) else None) or {}
    mean_dist = drift.get("mean_cosine_distance")
    threshold = drift.get("drift_threshold") or 0.15
    try:
        mean_dist_f = float(mean_dist) if mean_dist is not None else 0.0
    except Exception:
        mean_dist_f = 0.0
    try:
        threshold_f = float(threshold)
    except Exception:
        threshold_f = 0.15

    # Executive-friendly drift meter: 100 means “at/above threshold”.
    score = 0.0 if threshold_f <= 0 else (mean_dist_f / threshold_f) * 100.0
    score = max(0.0, min(100.0, score))

    status = str(drift.get("status") or "UNKNOWN")
    narrative = (
        f"Embedding drift is within threshold ({mean_dist_f:.3f} ≤ {threshold_f:.3f})."
        if status == "PASS"
        else f"Embedding drift exceeded threshold ({mean_dist_f:.3f} > {threshold_f:.3f})."
    )

    return {
        "drift_score": int(round(score)),
        "mean_cosine_distance": mean_dist_f,
        "threshold": threshold_f,
        "status": status,
        "narrative": narrative,
        "generated_at": data.get("generated_at") or _now_iso_z(),
    }


@app.get("/api/llm-violations")
def api_llm_violations():
    """
    Executive trend signal based on `violation_log/violations.jsonl` emitted by `contracts/ai_extensions.py`.
    """
    log_path = REPO_ROOT / "violation_log" / "violations.jsonl"
    rows = _safe_read_jsonl(log_path)

    by_day: dict[str, int] = defaultdict(int)
    for r in rows:
        if r.get("_parse_error"):
            continue
        if str(r.get("type") or "").upper() != "AI":
            continue
        ts = r.get("logged_at") or r.get("timestamp") or r.get("created_at") or ""
        if not isinstance(ts, str) or not ts:
            continue
        day = ts.split("T")[0]
        by_day[day] += 1

    points = [{"date": d, "count": int(c)} for d, c in sorted(by_day.items())]
    return {"points": points}


@app.get("/api/ping")
def api_ping():
    return {"ok": True, "repo_root": str(REPO_ROOT), "time": _now_iso_z()}
