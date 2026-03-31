from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import subprocess

from contracts.generator import generate_contract
from contracts.runner import run_validation
from contracts.utils import safe_mkdir


def _run_py(script: str, args: list[str]) -> None:
    subprocess.run(["python", script, *args], check=False)


def bootstrap(cloned_root: str) -> None:
    safe_mkdir("outputs")
    safe_mkdir("outputs/migrate")
    safe_mkdir("generated_contracts")
    safe_mkdir("validation_reports")

    # Phase 0: migrate/synthesize datasets into outputs/
    _run_py("outputs/migrate/intent_records_migration.py", ["--cloned-root", cloned_root])
    _run_py("outputs/migrate/extractions_migration.py", ["--cloned-root", cloned_root])
    _run_py("outputs/migrate/lineage_snapshots_migration.py", ["--cloned-root", cloned_root])
    _run_py("outputs/migrate/events_migration.py", ["--cloned-root", cloned_root])
    _run_py("scripts/generate_week2_verdicts.py", [])
    _run_py("scripts/generate_traces_runs.py", [])

    # Phase 0.3 strict schema + cross-dataset validation
    _run_py("scripts/phase0_validate.py", [])

    # Phase 0.1 discovery report
    _run_py("scripts/data_discovery.py", ["--cloned-root", cloned_root, "--out", "DATA_DISCOVERY.md"])

    # Phase 2: generate contracts (minimum Week3 + Week5)
    generate_contract(os.path.join("outputs", "week3", "extractions.jsonl"), "generated_contracts")
    generate_contract(os.path.join("outputs", "week5", "events.jsonl"), "generated_contracts")
    generate_contract(os.path.join("outputs", "traces", "runs.jsonl"), "generated_contracts")
    generate_contract(os.path.join("outputs", "week2", "verdicts.jsonl"), "generated_contracts")

    # Phase 3: produce real validation reports
    run_validation(
        os.path.join("generated_contracts", "week3_extractions.yaml"),
        os.path.join("outputs", "week3", "extractions.jsonl"),
        os.path.join("validation_reports", "week3_extractions.json"),
    )
    run_validation(
        os.path.join("generated_contracts", "week5_events.yaml"),
        os.path.join("outputs", "week5", "events.jsonl"),
        os.path.join("validation_reports", "week5_events.json"),
    )

    # Phase 1: domain notes (evidence-backed)
    _run_py("scripts/write_domain_notes.py", [])

    # PDF/MD report artifacts (local build)
    _run_py("scripts/build_week7_report.py", [])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cloned-root", default=r"C:\\Users\\Eyor.G\\Documents\\Cloned")
    args = parser.parse_args()
    bootstrap(args.cloned_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
