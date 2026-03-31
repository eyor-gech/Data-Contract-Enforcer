from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas


def _load_json(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_md(out_path: str) -> None:
    v_week3 = _load_json(os.path.join("validation_reports", "week3_extractions.json"))
    v_week5 = _load_json(os.path.join("validation_reports", "week5_events.json"))

    md: list[str] = []
    md.append("# Week 7 — Data Contract Enforcer Report\n")
    md.append(f"_Generated: {datetime.utcnow().isoformat()}Z_\n")

    md.append("## PDF Link (Google Drive)\n")
    md.append("- Offline build produced `reports/week7_report.pdf`.\n")
    md.append("- To publish: run `python scripts/publish_report_google_drive.py --pdf reports/week7_report.pdf`.\n")

    md.append("## Data Flow Diagram\n")
    md.append("```mermaid\nflowchart LR\n")
    md.append('  W1["Week 1: intent_records"] -->|intent_records v1| W2["Week 2: verdicts"]\n')
    md.append('  W2 -->|verdicts v1| W3["Week 3: extractions"]\n')
    md.append('  W3 -->|extractions v1| W4["Week 4: lineage_snapshots"]\n')
    md.append('  W4 -->|lineage graph| W7["Week 7: enforcer"]\n')
    md.append('  W5["Week 5: events"] -->|events v1| W7\n')
    md.append('  T["LangSmith traces"] -->|runs v1| W7\n')
    md.append("```\n")

    md.append("## Contract Coverage Table\n")
    md.append("| Interface | Schema | Contract | Notes |\n|---|---|---|---|\n")
    md.append("| Week1 → Week2 | `week1_intent_records` → `week2_verdicts` | Yes | Enforces file referential link |\n")
    md.append("| Week3 → Week4 | `week3_extractions` → `week4_lineage_snapshots` | Yes | Enforces doc_id appears as lineage node |\n")
    md.append("| Week5 → Week7 | `week5_events` | Yes | Enforces payload per event_type |\n")
    md.append("| Trace → Token math | `traces_runs` | Yes | Enforces total_tokens = prompt + completion |\n")

    md.append("## First Validation Run Results\n")
    md.append(f"- Week3 summary: `{v_week3.get('summary')}`\n")
    md.append(f"- Week5 summary: `{v_week5.get('summary')}`\n")

    md.append("## Reflection (max 400 words)\n")
    md.append(
        "The biggest surprise was how many issues were not ‘JSON errors’ but contract-level correctness errors. "
        "For Week 5, the source event payload carried `requested_amount_usd` as a string, which is easy to miss until "
        "a numeric range or drift rule is enforced. For Week 3, confidence looks valid at a glance, but once we "
        "enforce probability semantics (0–1) and connect extractions to Week 4 lineage, referential gaps become "
        "visible and traceable. The exercise also exposed an assumption that lineage snapshots are inherently "
        "complete; in reality, lineage can be partial or stale, so the enforcer must treat missing lineage as a "
        "first-class violation with an explicit blame chain. Finally, token accounting in traces is an AI-specific "
        "contract surface that standard schema checks do not cover; enforcing token math early prevents downstream "
        "cost and latency analytics from silently drifting.\n"
    )

    _ensure_dir(os.path.dirname(out_path))
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("".join(md))


def write_pdf(md_path: str, pdf_path: str) -> None:
    _ensure_dir(os.path.dirname(pdf_path))
    c = canvas.Canvas(pdf_path, pagesize=letter)
    width, height = letter
    left = 0.75 * inch
    top = height - 0.75 * inch
    y = top
    line_h = 12

    def draw_line(text: str) -> None:
        nonlocal y
        if y < 0.75 * inch:
            c.showPage()
            y = top
        c.drawString(left, y, text[:160])
        y -= line_h

    with open(md_path, "r", encoding="utf-8") as f:
        for line in f:
            draw_line(line.rstrip("\n"))

    c.save()


def main() -> int:
    md_path = os.path.join("reports", "week7_report.md")
    pdf_path = os.path.join("reports", "week7_report.pdf")
    write_md(md_path)
    write_pdf(md_path, pdf_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

