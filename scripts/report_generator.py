from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

import yaml

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _utc_now_iso() -> str:
    fixed = os.environ.get("ENFORCER_NOW_UTC")
    if fixed:
        return fixed
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _load_json(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _load_yaml(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = yaml.safe_load(f) or {}
            return obj if isinstance(obj, dict) else {"_non_object": obj}
    except Exception:
        return {}


def _severity_points(sev: str) -> int:
    s = (sev or "").upper()
    return {"CRITICAL": 20, "HIGH": 10, "MEDIUM": 5, "LOW": 1}.get(s, 1)


def _business_narrative(v: dict[str, Any]) -> str:
    dataset = v.get("root_cause") or "unknown_system"
    field = v.get("field") or "unknown_field"
    msg = v.get("message") or "contract violation detected"
    sev = (v.get("severity") or "LOW").upper()
    impact = v.get("downstream_impact")
    if not impact and isinstance(v.get("lineage_path"), list):
        lp = [str(x) for x in (v.get("lineage_path") or []) if x]
        if len(lp) > 1:
            impact = f"Downstream impact: {', '.join(lp[1:])}"
    if impact:
        return f"[{sev}] {dataset}: {field} — {msg} ({impact})"
    return f"[{sev}] {dataset}: {field} — {msg}"


def _repo_files_index(root: str = ".") -> list[str]:
    out: list[str] = []
    for base, dirs, files in os.walk(root):
        # skip virtualenvs / node_modules-like directories deterministically
        dirs[:] = sorted([d for d in dirs if d not in (".git", ".venv", "node_modules", "__pycache__")])
        for fn in sorted(files):
            rel = os.path.relpath(os.path.join(base, fn), root)
            if rel.startswith(".git" + os.sep):
                continue
            out.append(rel.replace("\\", "/"))
    out.sort()
    return out


def _tokenize(text: str) -> list[str]:
    t = (text or "").replace(".", "_").replace("-", "_").replace("/", "_").lower()
    toks = [x.strip() for x in t.split("_") if x.strip()]
    return toks


def _best_file_match(files: list[str], tokens: list[str], prefer_prefixes: list[str]) -> str | None:
    scored: list[tuple[int, str]] = []
    for f in files:
        lf = f.lower()
        if not any(lf.endswith(ext) for ext in (".py", ".sql", ".yml", ".yaml", ".md")):
            continue
        score = 0
        for p in prefer_prefixes:
            if lf.startswith(p):
                score += 3
        for tok in tokens:
            if tok and tok in lf:
                score += 1
        if score > 0:
            scored.append((score, f))
    scored.sort(key=lambda t: (-t[0], t[1]))
    return scored[0][1] if scored else None


def _infer_file_path_for_dataset(dataset: str, files: list[str]) -> str:
    toks = _tokenize(dataset)
    prefer = ["outputs/migrate/", "scripts/", "contracts/"]
    best = _best_file_match(files, toks, prefer_prefixes=prefer)
    return best or "README.md"


def _suggest_file_path(v: dict[str, Any], files: list[str]) -> str:
    fp = v.get("file_path") or v.get("producer_file") or v.get("file")
    if isinstance(fp, str) and fp.strip():
        return fp.strip()
    dataset = str(v.get("root_cause") or v.get("dataset") or "")
    if dataset:
        return _infer_file_path_for_dataset(dataset, files)
    return "README.md"


def _clause_id(v: dict[str, Any]) -> str:
    cid = v.get("clause_id") or v.get("check_id") or v.get("contract_clause")
    if cid:
        return str(cid)
    # Deterministic fallback: still clause-level (type + field) rather than unknown.
    t = str(v.get("type") or "violation").lower()
    f = str(v.get("field") or "unknown_field")
    return f"{t}:{f}"


def _recommendation(v: dict[str, Any], *, files: list[str]) -> str:
    field = str(v.get("field") or "")
    msg = str(v.get("message") or "")
    clause = _clause_id(v)
    file_path = _suggest_file_path(v, files)
    if "confidence" in field:
        return f"Update `{file_path}` to output `confidence` as float in [0.0, 1.0] (contract clause `{clause}`)."
    if "total_tokens" in field or "token" in msg:
        return f"Update `{file_path}` so `total_tokens == prompt_tokens + completion_tokens` (contract clause `{clause}`)."
    if "global_position" in field and "monotonic" in msg:
        return f"Update `{file_path}` to guarantee monotonic `global_position` (contract clause `{clause}`)."
    if "payload" in field:
        return f"Update `{file_path}` to emit required `payload` fields per `event_type` (contract clause `{clause}`)."
    if "uuid" in msg.lower():
        return f"Update `{file_path}` to emit UUIDv4 identifiers (contract clause `{clause}`)."
    return f"Investigate producer changes and update `{file_path}` to satisfy contract clause `{clause}`."


def _collect_runner_reports() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for name in ["week3_extractions.json", "week5_events.json", "traces_runs.json", "verdicts.json"]:
        p = os.path.join("validation_reports", name)
        if os.path.exists(p):
            out.append({"source": p, "report": _load_json(p)})
    # include phase0 reports (structural/cross-dataset gate)
    for fn in os.listdir("validation_reports") if os.path.isdir("validation_reports") else []:
        if fn.startswith("phase0_") and fn.endswith(".json"):
            out.append({"source": os.path.join("validation_reports", fn), "report": _load_json(os.path.join("validation_reports", fn))})
    return out


def _collect_schema_reports() -> list[dict[str, Any]]:
    d = os.path.join("reports", "schema_migration_reports")
    if not os.path.isdir(d):
        return []
    out: list[dict[str, Any]] = []
    for fn in sorted(os.listdir(d)):
        if fn.endswith("_migration_report.yaml"):
            out.append({"source": os.path.join(d, fn), "report": _load_yaml(os.path.join(d, fn))})
    return out[-5:]


def _collect_ai_extensions() -> dict[str, Any]:
    return _load_json(os.path.join("validation_reports", "ai_extensions.json"))


def _collect_violation_log() -> list[dict[str, Any]]:
    path = os.path.join("violation_log", "violations.jsonl")
    if not os.path.exists(path):
        return []
    out: list[dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        obj = dict(obj)
                        obj.setdefault("source_report", path)
                        out.append(obj)
                except Exception:
                    continue
    except Exception:
        return []
    return out


def generate_report(out_dir: str) -> tuple[str, str]:
    _safe_mkdir(out_dir)
    files_idx = _repo_files_index(".")
    runner_reports = _collect_runner_reports()
    schema_reports = _collect_schema_reports()
    ai_ext = _collect_ai_extensions()
    violation_log = _collect_violation_log()

    violations: list[dict[str, Any]] = []
    for rr in runner_reports:
        rep = rr.get("report") or {}
        for v in rep.get("violations") or []:
            if isinstance(v, dict):
                v = dict(v)
                v.setdefault("source_report", rr.get("source"))
                if "downstream_impact" not in v and isinstance(v.get("lineage_path"), list):
                    lp = [str(x) for x in (v.get("lineage_path") or []) if x]
                    if len(lp) > 1:
                        v["downstream_impact"] = ", ".join(lp[1:])
                v.setdefault("clause_id", v.get("clause_id") or v.get("check_id"))
                violations.append(v)

    # Include AI/system violations written to the shared violation log.
    for v in violation_log:
        if isinstance(v, dict):
            v = dict(v)
            v.setdefault("clause_id", v.get("clause_id") or v.get("check_id") or v.get("contract_clause"))
            violations.append(dict(v))

    # AI extensions may also have actionable failures without runner violations.
    if isinstance(ai_ext.get("results"), dict):
        drift = (ai_ext["results"].get("embedding_drift") or {}) if isinstance(ai_ext["results"], dict) else {}
        if isinstance(drift, dict) and drift.get("status") == "FAIL":
            violations.append(
                {
                    "type": "AI",
                    "field": "embedding_drift",
                    "severity": "CRITICAL",
                    "count": 1,
                    "root_cause": "week3_extractions",
                    "clause_id": "ai_extensions:embedding_drift_week3",
                    "message": "Embedding drift exceeded threshold; extraction semantics likely changed.",
                    "lineage_path": ["week3_extractions"],
                }
            )

    narrative: list[str] = [_business_narrative(v) for v in violations]

    # Health score formula (rubric): score = (passed/total) * 100 - (20 * critical_count)
    total_checks = 0
    passed_checks = 0
    for rr in runner_reports:
        rep = rr.get("report") or {}
        # Prefer mastered report fields; fall back to summary totals if older report format exists.
        tc = rep.get("total_checks")
        pc = rep.get("passed")
        if isinstance(tc, int) and isinstance(pc, int) and tc > 0:
            total_checks += tc
            passed_checks += pc
            continue
        summ = rep.get("summary") or {}
        tr = summ.get("total_rules")
        fr = summ.get("rules_failed")
        if isinstance(tr, int) and tr > 0:
            total_checks += tr
            if isinstance(fr, int):
                passed_checks += max(0, tr - fr)
    critical_count = sum(1 for v in violations if str(v.get("severity") or "").upper() == "CRITICAL")
    score = 0.0
    if total_checks > 0:
        score = (passed_checks / total_checks) * 100.0
    score = score - (20.0 * float(critical_count))
    score = max(0.0, min(100.0, score))

    # Deterministic ordering for stakeholder readability.
    violations_sorted = sorted(
        violations,
        key=lambda v: (-_severity_points(str(v.get("severity") or "LOW")), str(v.get("root_cause") or ""), str(v.get("field") or ""), _clause_id(v)),
    )

    recommended_actions: list[dict[str, Any]] = []
    for v in violations_sorted[:200]:
        recommended_actions.append(
            {
                "file_path": _suggest_file_path(v, files_idx),
                "contract_clause": _clause_id(v),
                "action": _recommendation(v, files=files_idx),
                "section": None,
            }
        )

    # Section 3: schema changes detected -> derive clause-level actions from migration reports (taxonomy + rollback).
    schema_actions: list[dict[str, Any]] = []
    schema_section_items: list[dict[str, Any]] = []
    for s in schema_reports:
        if not isinstance(s, dict):
            continue
        src = s.get("source")
        rep = s.get("report") or {}
        if not isinstance(rep, dict):
            continue
        schema_section_items.append(
            {
                "source": src,
                "compatibility_verdict": ((rep.get("compatibility") or {}).get("verdict")) if isinstance(rep.get("compatibility"), dict) else None,
                "reasons": ((rep.get("compatibility") or {}).get("reasons")) if isinstance(rep.get("compatibility"), dict) else None,
            }
        )
        detected = ((rep.get("change_taxonomy") or {}).get("detected")) if isinstance(rep.get("change_taxonomy"), dict) else None
        if not isinstance(detected, list):
            continue
        for c in detected:
            if not isinstance(c, dict):
                continue
            sev = str(c.get("severity") or "LOW").upper()
            comp = str(c.get("compatibility") or "").upper()
            if comp not in ("BREAKING", "COMPATIBLE"):
                continue
            field = str(c.get("field") or "")
            ctype = str(c.get("change_type") or "")
            clause = f"schema_change:{ctype}:{field}" if field else f"schema_change:{ctype}"
            rollback_steps = c.get("rollback_plan") if isinstance(c.get("rollback_plan"), list) else []
            file_path = _infer_file_path_for_dataset(str(((rep.get("contract_path") or "")).split("/")[-1]).replace(".yaml", ""), files_idx)
            schema_actions.append(
                {
                    "file_path": file_path,
                    "contract_clause": clause,
                    "action": f"Apply rollback/migration for `{field}` ({ctype}, severity={sev}): {str(rollback_steps[0]) if rollback_steps else 'review per-change rollback plan in migration report.'}",
                    "section": "3) Schema changes detected",
                    "source_report": src,
                }
            )

    # Section 4: AI system risk assessment -> clause-level actions derived from ai_extensions.json
    ai_actions: list[dict[str, Any]] = []
    ai_items: dict[str, Any] = {
        "embedding_drift": {},
        "prompt_inputs": {},
        "llm_outputs": {},
    }
    if isinstance(ai_ext.get("results"), dict):
        ai_items["embedding_drift"] = ai_ext["results"].get("embedding_drift") or {}
        ai_items["prompt_inputs"] = ai_ext["results"].get("prompt_inputs") or {}
        ai_items["llm_outputs"] = ai_ext["results"].get("llm_outputs") or {}
        # embedding drift
        ed = ai_items["embedding_drift"] if isinstance(ai_items["embedding_drift"], dict) else {}
        if str(ed.get("status") or "").upper() in ("FAIL", "WARN"):
            clause = "ai_extensions:embedding_drift_week3"
            ai_actions.append(
                {
                    "file_path": _best_file_match(files_idx, ["ai_extensions.py"], prefer_prefixes=["contracts/"]) or "contracts/ai_extensions.py",
                    "contract_clause": clause,
                    "action": f"Investigate embedding drift and rebaseline only with versioned change control (contract clause `{clause}`).",
                    "section": "4) AI system risk assessment",
                }
            )
        # prompt schema
        pi = ai_items["prompt_inputs"] if isinstance(ai_items["prompt_inputs"], dict) else {}
        inv = pi.get("invalid_count")
        if isinstance(inv, int) and inv > 0:
            clause = "ai_extensions:prompt_input_schema"
            ai_actions.append(
                {
                    "file_path": _best_file_match(files_idx, ["ai_extensions.py"], prefer_prefixes=["contracts/"]) or "contracts/ai_extensions.py",
                    "contract_clause": clause,
                    "action": f"Fix prompt input builder to satisfy JSON Schema; quarantined records exist (contract clause `{clause}`).",
                    "section": "4) AI system risk assessment",
                }
            )
        # LLM output schema violation rate
        lo = ai_items["llm_outputs"] if isinstance(ai_items["llm_outputs"], dict) else {}
        vr = lo.get("output_schema_violation_rate")
        status = str(lo.get("status") or "").upper()
        if status in ("WARN", "FAIL") or (isinstance(vr, (int, float)) and float(vr) > 0.0):
            clause = "ai_extensions:output_schema_violation_rate"
            ai_actions.append(
                {
                    "file_path": _best_file_match(files_idx, ["ai_extensions.py"], prefer_prefixes=["contracts/"]) or "contracts/ai_extensions.py",
                    "contract_clause": clause,
                    "action": f"Enforce structured LLM output schema; monitor violation trend and roll back prompt/template on regression (contract clause `{clause}`).",
                    "section": "4) AI system risk assessment",
                }
            )

    # Assign section tags to base recommended actions deterministically.
    for a in recommended_actions:
        a["section"] = "2) Violations this week"

    for a in schema_actions:
        recommended_actions.append(a)
    for a in ai_actions:
        recommended_actions.append(a)

    # Section 1: Data Health Score actions -> focus on most severe current violations.
    health_actions: list[dict[str, Any]] = []
    for v in violations_sorted[:10]:
        health_actions.append(
            {
                "file_path": _suggest_file_path(v, files_idx),
                "contract_clause": _clause_id(v),
                "action": _recommendation(v, files=files_idx),
                "section": "1) Data Health Score",
            }
        )
    recommended_actions.extend(health_actions)

    # De-duplicate actions deterministically.
    seen = set()
    uniq_actions: list[dict[str, Any]] = []
    for a in recommended_actions:
        key = (str(a.get("file_path") or ""), str(a.get("contract_clause") or ""), str(a.get("action") or ""), str(a.get("section") or ""))
        if key in seen:
            continue
        seen.add(key)
        uniq_actions.append(a)
    uniq_actions.sort(key=lambda x: (str(x.get("section") or ""), str(x.get("file_path") or ""), str(x.get("contract_clause") or ""), str(x.get("action") or "")))

    sections_data = {
        "1) Data Health Score": {
            "score_formula": "score = (passed/total)*100 - (20*critical_count)",
            "inputs": {"total": int(total_checks), "passed": int(passed_checks), "critical_count": int(critical_count)},
            "recommended_actions": health_actions[:50],
        },
        "2) Violations this week": {
            "violations": violations_sorted[:200],
            "recommended_actions": [a for a in uniq_actions if a.get("section") == "2) Violations this week"][:200],
        },
        "3) Schema changes detected": {
            "schema_changes": schema_section_items,
            "recommended_actions": schema_actions[:200],
        },
        "4) AI system risk assessment": {
            "ai": ai_items,
            "recommended_actions": ai_actions[:200],
        },
        "5) Recommended actions": {
            "recommended_actions": uniq_actions[:300],
        },
    }

    report_data = {
        "report_version": "1.0.0",
        "generated_at": _utc_now_iso(),
        "data_health_score": int(round(score)),
        "health_score_inputs": {"total": int(total_checks), "passed": int(passed_checks), "critical_count": int(critical_count)},
        "inputs": {"runner_reports": [r.get("source") for r in runner_reports], "schema_reports": [s.get("source") for s in schema_reports]},
        "violations": violations_sorted,
        "narrative": narrative[:200],
        "recommended_actions": uniq_actions,
        "sections": [
            "1) Data Health Score",
            "2) Violations this week",
            "3) Schema changes detected",
            "4) AI system risk assessment",
            "5) Recommended actions",
        ],
        "sections_data": sections_data,
        "schema_migration_reports": schema_reports,
        "ai_extensions": ai_ext,
        "schema_changes_detected": schema_section_items,
        "ai_system_risk_assessment": ai_items,
    }

    json_path = os.path.join(out_dir, "report_data.json")
    with open(json_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(report_data, f, ensure_ascii=False, indent=2)

    pdf_path = os.path.join(out_dir, "enforcer_report.pdf")
    _write_pdf(pdf_path, report_data)
    return json_path, pdf_path


def _write_pdf(pdf_path: str, report_data: dict[str, Any]) -> None:
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.units import inch
        from reportlab.pdfgen import canvas
    except Exception:
        try:
            from contracts.utils import write_pdf_from_text

            write_pdf_from_text(pdf_path, title="Week 7 Enforcer Report", text=yaml.safe_dump(report_data, sort_keys=False, allow_unicode=True))
        except Exception:
            return
    _safe_mkdir(os.path.dirname(pdf_path))
    c = canvas.Canvas(pdf_path, pagesize=letter)
    width, height = letter
    left = 0.75 * inch
    top = height - 0.75 * inch
    y = top
    line_h = 12

    def draw(line: str) -> None:
        nonlocal y
        if y < 0.75 * inch:
            c.showPage()
            y = top
        c.drawString(left, y, line[:160])
        y -= line_h

    draw("Week 7 Enforcer Report")
    draw(f"Generated: {report_data.get('generated_at')}")
    draw(f"Data Health Score: {report_data.get('data_health_score')}/100")
    draw("")
    sections = report_data.get("sections") or []
    sections_data = report_data.get("sections_data") or {}
    if isinstance(sections, list) and isinstance(sections_data, dict):
        for sec in sections:
            draw(str(sec))
            block = sections_data.get(sec) if isinstance(sec, str) else None
            if not isinstance(block, dict):
                draw("  (no data)")
                draw("")
                continue
            if sec == "1) Data Health Score":
                draw(f"  Inputs: {block.get('inputs')}")
                for a in (block.get('recommended_actions') or [])[:10]:
                    if isinstance(a, dict):
                        draw(f"  - {a.get('action')}")
                draw("")
                continue
            if sec == "2) Violations this week":
                for v in (block.get("violations") or [])[:25]:
                    if isinstance(v, dict):
                        draw(f"  - {_business_narrative(v)}")
                for a in (block.get("recommended_actions") or [])[:10]:
                    if isinstance(a, dict):
                        draw(f"  * {a.get('action')}")
                draw("")
                continue
            if sec == "3) Schema changes detected":
                for it in (block.get("schema_changes") or [])[:10]:
                    if isinstance(it, dict):
                        draw(f"  - {it.get('source')}: {it.get('compatibility_verdict')}")
                for a in (block.get("recommended_actions") or [])[:10]:
                    if isinstance(a, dict):
                        draw(f"  * {a.get('action')}")
                draw("")
                continue
            if sec == "4) AI system risk assessment":
                ai = block.get("ai") or {}
                if isinstance(ai, dict):
                    ed = ai.get("embedding_drift") or {}
                    if isinstance(ed, dict):
                        draw(f"  embedding_drift.status={ed.get('status')} mean={ed.get('mean_cosine_distance')}")
                    pi = ai.get("prompt_inputs") or {}
                    if isinstance(pi, dict):
                        draw(f"  prompt_inputs.invalid_count={pi.get('invalid_count')} quarantine={pi.get('quarantine_path')}")
                    lo = ai.get("llm_outputs") or {}
                    if isinstance(lo, dict):
                        draw(f"  llm_outputs.violation_rate={lo.get('output_schema_violation_rate')} trend={lo.get('violation_rate_trend_last5')} status={lo.get('status')}")
                for a in (block.get("recommended_actions") or [])[:10]:
                    if isinstance(a, dict):
                        draw(f"  * {a.get('action')}")
                draw("")
                continue
            if sec == "5) Recommended actions":
                for a in (block.get("recommended_actions") or [])[:30]:
                    if isinstance(a, dict):
                        draw(f"  - {a.get('action')}")
                draw("")
    c.save()


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 4B: Enforcer Report Generator")
    parser.add_argument("--out-dir", default=os.path.join("enforcer_report"))
    args = parser.parse_args()
    try:
        j, p = generate_report(args.out_dir)
        print(j)
        print(p)
        return 0
    except Exception as e:
        _safe_mkdir(args.out_dir)
        with open(os.path.join(args.out_dir, "report_error.json"), "w", encoding="utf-8", newline="\n") as f:
            json.dump({"error": str(e)}, f, ensure_ascii=False, indent=2)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
