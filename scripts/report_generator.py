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


def _suggest_file_path(v: dict[str, Any]) -> str:
    # Keep recommendations actionable within this repo (no external path assumptions).
    dataset = str(v.get("root_cause") or "")
    if dataset == "week3_extractions":
        return os.path.join("outputs", "migrate", "extractions_migration.py")
    if dataset == "week5_events":
        return os.path.join("outputs", "migrate", "events_migration.py")
    if dataset == "traces_runs":
        return os.path.join("scripts", "generate_traces_runs.py")
    if dataset == "week2_verdicts":
        return os.path.join("scripts", "generate_week2_verdicts.py")
    return "README.md"


def _recommendation(v: dict[str, Any]) -> str:
    field = str(v.get("field") or "")
    msg = str(v.get("message") or "")
    clause = str(v.get("clause_id") or "unknown_clause")
    file_path = _suggest_file_path(v)
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
                violations.append(v)

    # Include AI/system violations written to the shared violation log.
    for v in violation_log:
        if isinstance(v, dict):
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
                    "message": "Embedding drift exceeded threshold; extraction semantics likely changed.",
                    "lineage_path": ["week3_extractions"],
                }
            )

    narrative: list[str] = [_business_narrative(v) for v in violations]

    # Health score formula (rubric): (passed/total)*100 - (20*critical_count)
    total_rules = 0
    failed_rules = 0
    for rr in runner_reports:
        rep = rr.get("report") or {}
        summ = rep.get("summary") or {}
        tr = summ.get("total_rules")
        fr = summ.get("rules_failed")
        if isinstance(tr, int):
            total_rules += tr
        if isinstance(fr, int):
            failed_rules += fr
    passed_rules = max(0, total_rules - failed_rules)
    critical_count = sum(1 for v in violations if str(v.get("severity") or "").upper() == "CRITICAL")
    score = 0.0
    if total_rules > 0:
        score = (passed_rules / total_rules) * 100.0
    score = score - (20.0 * float(critical_count))
    score = max(0.0, min(100.0, score))

    recs: list[str] = [_recommendation(v) for v in violations[:50]]
    recommended_actions: list[dict[str, Any]] = []
    for v in violations[:50]:
        recommended_actions.append(
            {
                "file_path": _suggest_file_path(v),
                "contract_clause": v.get("clause_id"),
                "action": _recommendation(v),
            }
        )

    report_data = {
        "report_version": "1.0.0",
        "generated_at": _utc_now_iso(),
        "data_health_score": int(round(score)),
        "health_score_inputs": {"total_rules": total_rules, "failed_rules": failed_rules, "critical_count": critical_count},
        "inputs": {"runner_reports": [r.get("source") for r in runner_reports], "schema_reports": [s.get("source") for s in schema_reports]},
        "violations": violations,
        "narrative": narrative[:200],
        "recommendations": sorted(set(recs)),
        "recommended_actions": recommended_actions,
        "sections": [
            "1) Data Health Score",
            "2) Violations this week",
            "3) Schema changes detected",
            "4) AI system risk assessment",
            "5) Recommended actions",
        ],
        "schema_migration_reports": schema_reports,
        "ai_extensions": ai_ext,
        "schema_changes_detected": [
            {
                "source": s.get("source"),
                "compatibility_verdict": ((s.get("report") or {}).get("compatibility") or {}).get("verdict"),
                "reasons": ((s.get("report") or {}).get("compatibility") or {}).get("reasons"),
            }
            for s in schema_reports
            if isinstance(s, dict)
        ],
        "ai_system_risk_assessment": {
            "embedding_drift": ((ai_ext.get("results") or {}).get("embedding_drift") or {}) if isinstance(ai_ext.get("results"), dict) else {},
            "prompt_inputs": ((ai_ext.get("results") or {}).get("prompt_inputs") or {}) if isinstance(ai_ext.get("results"), dict) else {},
            "llm_outputs": ((ai_ext.get("results") or {}).get("llm_outputs") or {}) if isinstance(ai_ext.get("results"), dict) else {},
        },
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
    draw("1) Overview")
    draw("This report aggregates contract validation, schema evolution, and AI extension checks.")
    draw("")
    draw("2) Data Health Score")
    draw(f"Inputs: {report_data.get('health_score_inputs')}")
    draw("")
    draw("3) Violations & Business Impact")
    for line in (report_data.get("narrative") or [])[:60]:
        draw(f"- {line}")
    draw("")
    draw("4) Migration / Consumer Impact")
    for s in (report_data.get("schema_migration_reports") or [])[:5]:
        src = s.get("source")
        rep = s.get("report") or {}
        draw(f"- {src}: {((rep.get('compatibility') or {}).get('verdict'))}")
    draw("")
    draw("5) Recommendations (file + clause)")
    for r in (report_data.get("recommended_actions") or [])[:30]:
        if isinstance(r, dict):
            draw(f"- {r.get('action')}")
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
