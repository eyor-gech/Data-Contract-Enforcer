from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import numpy as np
from jsonschema import Draft202012Validator

# Allow `python contracts/ai_extensions.py ...` execution (repo root on sys.path).
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from contracts.utils import read_jsonl, safe_float, safe_int, stable_uuid_v4, write_jsonl  # noqa: E402


def _utc_now_iso() -> str:
    fixed = os.environ.get("ENFORCER_NOW_UTC")
    if fixed:
        return fixed
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _append_violation(v: dict[str, Any]) -> None:
    _safe_mkdir("violation_log")
    path = os.path.join("violation_log", "violations.jsonl")
    v = dict(v)
    v.setdefault("logged_at", _utc_now_iso())
    with open(path, "a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(v, ensure_ascii=False))
        f.write("\n")


def _hash_embed(texts: list[str], n_features: int = 256) -> np.ndarray:
    """
    Deterministic local embedding (no network): hashing trick -> l2-normalized vectors.
    """
    from sklearn.feature_extraction.text import HashingVectorizer

    vec = HashingVectorizer(
        n_features=n_features,
        alternate_sign=False,
        norm="l2",
        lowercase=True,
        token_pattern=r"(?u)\b\w+\b",
    )
    X = vec.transform(texts)
    return X.astype(np.float32).toarray()


def _cosine_distance_to_centroid(X: np.ndarray, centroid: np.ndarray) -> np.ndarray:
    # X and centroid should already be l2-ish; normalize defensively.
    Xn = X / (np.linalg.norm(X, axis=1, keepdims=True) + 1e-12)
    cn = centroid / (np.linalg.norm(centroid) + 1e-12)
    sims = Xn @ cn
    return 1.0 - sims


def embedding_drift_week3(
    extractions_path: str,
    baseline_path: str,
    sample_n: int = 200,
    drift_threshold: float = 0.15,
) -> dict[str, Any]:
    rows = [r for r in read_jsonl(extractions_path) if "_parse_error" not in r]
    # Deterministic sample: by extraction_id
    rows = sorted(rows, key=lambda r: str(r.get("extraction_id") or ""))
    rows = rows[:sample_n]

    texts: list[str] = []
    for r in rows:
        t = r.get("text")
        if isinstance(t, str) and t.strip():
            texts.append(t.strip())
        else:
            # No silent drops: deterministic fallback that still produces a stable embedding surface.
            texts.append(f"doc={r.get('doc_key')} page={r.get('page_number')} strategy={((r.get('strategy') or {}).get('name') if isinstance(r.get('strategy'), dict) else None)}")

    X = _hash_embed(texts, n_features=256)
    centroid = None
    baseline = {}
    if os.path.exists(baseline_path):
        try:
            baseline = json.load(open(baseline_path, "r", encoding="utf-8"))
            c = baseline.get("centroid")
            if isinstance(c, list) and len(c) == 256:
                centroid = np.array(c, dtype=np.float32)
        except Exception:
            baseline = {}

    if centroid is None:
        centroid = X.mean(axis=0)
        _safe_mkdir(os.path.dirname(baseline_path))
        json.dump(
            {"baseline_version": "1.0.0", "created_at": _utc_now_iso(), "centroid": centroid.tolist()},
            open(baseline_path, "w", encoding="utf-8"),
            ensure_ascii=False,
            indent=2,
        )

    dists = _cosine_distance_to_centroid(X, centroid)
    mean_dist = float(np.mean(dists)) if len(dists) else 0.0
    status = "PASS" if mean_dist <= drift_threshold else "FAIL"
    result = {
        "check": "embedding_drift_week3",
        "status": status,
        "drift_threshold": drift_threshold,
        "sample_n": int(len(rows)),
        "mean_cosine_distance": mean_dist,
        "p95_cosine_distance": float(np.quantile(dists, 0.95)) if len(dists) else 0.0,
    }
    if status == "FAIL":
        _append_violation(
            {
                "phase": "4A",
                "type": "AI",
                "severity": "CRITICAL",
                "dataset": "week3_extractions",
                "check": "embedding_drift_week3",
                "message": f"Embedding drift mean cosine distance {mean_dist:.3f} exceeded threshold {drift_threshold:.3f}.",
                "recommendation": "Investigate Week3 extractor prompt/model changes; rebaseline if change is intentional and versioned.",
            }
        )
    return result


def validate_prompt_inputs(
    traces_path: str,
    quarantine_dir: str,
    schema: dict[str, Any],
) -> dict[str, Any]:
    validator = Draft202012Validator(schema)
    rows = [(i, r) for i, r in enumerate(read_jsonl(traces_path), start=1) if "_parse_error" not in r]
    invalid: list[dict[str, Any]] = []
    for line_no, r in rows:
        inputs = r.get("inputs")
        errs = list(validator.iter_errors(inputs))
        if errs:
            invalid.append({"line_no": line_no, "run_id": r.get("run_id"), "inputs": inputs, "errors": [e.message for e in errs[:5]]})
            _append_violation(
                {
                    "phase": "4A",
                    "type": "AI",
                    "severity": "HIGH",
                    "dataset": "traces_runs",
                    "check": "prompt_input_schema",
                    "message": f"Prompt inputs failed JSON Schema at line {line_no}: {errs[0].message}",
                    "recommendation": "Fix prompt input builder to emit required structured keys (prompt, context_bytes).",
                }
            )
    _safe_mkdir(quarantine_dir)
    qpath = os.path.join(quarantine_dir, "traces_runs_inputs_invalid.jsonl")
    write_jsonl(qpath, invalid)
    return {"check": "prompt_input_schema", "invalid_count": len(invalid), "quarantine_path": qpath}


def validate_llm_outputs_week2(
    verdicts_path: str,
    intents_path: str,
    baseline_path: str,
) -> dict[str, Any]:
    # JSON Schema: strict structural + numeric bounds.
    schema = {
        "type": "object",
        "required": ["verdict_id", "trace_id", "intent_id", "created_at", "target_ref", "model", "scores", "verdict"],
        "properties": {
            "verdict_id": {"type": "string"},
            "trace_id": {"type": "string"},
            "intent_id": {"type": "string"},
            "created_at": {"type": "string"},
            "target_ref": {
                "type": "object",
                "required": ["file", "span"],
                "properties": {
                    "file": {"type": "string"},
                    "span": {
                        "type": "object",
                        "required": ["start_line", "end_line"],
                        "properties": {"start_line": {"type": "integer", "minimum": 1}, "end_line": {"type": "integer", "minimum": 1}},
                    },
                },
            },
            "model": {"type": "object", "required": ["provider", "name"], "properties": {"provider": {"type": "string"}, "name": {"type": "string"}}},
            "scores": {
                "type": "object",
                "required": ["correctness", "safety", "style", "weights", "weighted_score"],
                "properties": {
                    "correctness": {"type": "integer", "minimum": 1, "maximum": 5},
                    "safety": {"type": "integer", "minimum": 1, "maximum": 5},
                    "style": {"type": "integer", "minimum": 1, "maximum": 5},
                    "weights": {"type": "object"},
                    "weighted_score": {"type": "number", "minimum": 1.0, "maximum": 5.0},
                },
            },
            "verdict": {"type": "object", "required": ["label", "rationale", "confidence"], "properties": {"label": {"type": "string"}, "rationale": {"type": "string"}, "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0}}},
        },
        "additionalProperties": True,
    }
    validator = Draft202012Validator(schema)
    verdicts = [(i, r) for i, r in enumerate(read_jsonl(verdicts_path), start=1) if "_parse_error" not in r]
    total = len(verdicts)

    invalid_schema = 0
    invalid_weighted = 0
    invalid_link = 0
    samples: list[dict[str, Any]] = []

    # intent file set for link check (Week1 -> Week2 contract)
    intent_files: set[str] = set()
    for r in read_jsonl(intents_path):
        if "_parse_error" in r:
            continue
        intent = r.get("intent")
        if isinstance(intent, dict) and isinstance(intent.get("code_refs"), list):
            for cref in intent["code_refs"]:
                if isinstance(cref, dict) and isinstance(cref.get("file"), str):
                    intent_files.add(cref["file"])

    for line_no, r in verdicts:
        errs = list(validator.iter_errors(r))
        if errs:
            invalid_schema += 1
            if len(samples) < 3:
                samples.append({"line_no": line_no, "verdict_id": r.get("verdict_id"), "error": errs[0].message})
            _append_violation(
                {
                    "phase": "4A",
                    "type": "AI",
                    "severity": "HIGH",
                    "dataset": "week2_verdicts",
                    "check": "llm_output_schema",
                    "message": f"Week2 verdict schema invalid at line {line_no}: {errs[0].message}",
                    "recommendation": "Update verdict formatter to match required schema and score bounds.",
                }
            )

        # AI-specific: weighted score math check (detects prompt/model formatting regressions).
        w = ((r.get("scores") or {}).get("weights")) if isinstance(r.get("scores"), dict) else None
        if isinstance(w, dict):
            wc = safe_float(w.get("correctness"))
            ws = safe_float(w.get("safety"))
            wst = safe_float(w.get("style"))
            denom = (wc or 0) + (ws or 0) + (wst or 0)
            if wc is not None and ws is not None and wst is not None and denom > 0:
                c = safe_float((r.get("scores") or {}).get("correctness"))
                s = safe_float((r.get("scores") or {}).get("safety"))
                st = safe_float((r.get("scores") or {}).get("style"))
                actual = safe_float((r.get("scores") or {}).get("weighted_score"))
                if c is not None and s is not None and st is not None and actual is not None:
                    expected = (wc * c + ws * s + wst * st) / denom
                    if abs(expected - actual) > 1e-6:
                        invalid_weighted += 1
                        _append_violation(
                            {
                                "phase": "4A",
                                "type": "AI",
                                "severity": "MEDIUM",
                                "dataset": "week2_verdicts",
                                "check": "weighted_score_math",
                                "message": f"Weighted score mismatch at line {line_no}: expected {expected:.6f}, got {actual:.6f}",
                                "recommendation": "Recompute weighted_score from component scores before emitting verdicts.",
                            }
                        )

        # Cross-system link: target_ref.file must appear in upstream intent code_refs (prevents hallucinated file refs).
        f = ((r.get("target_ref") or {}).get("file")) if isinstance(r.get("target_ref"), dict) else None
        if isinstance(f, str) and intent_files and f not in intent_files:
            invalid_link += 1
            _append_violation(
                {
                    "phase": "4A",
                    "type": "AI",
                    "severity": "CRITICAL",
                    "dataset": "week2_verdicts",
                    "check": "target_ref_link",
                    "message": f"Week2 verdict references file not present in Week1 intents: {f}",
                    "recommendation": "Constrain the LLM to only reference files present in intent.code_refs; add retrieval/grounding.",
                }
            )

    violation_rate = (invalid_schema / total) if total else 0.0
    history_path = os.path.join("validation_reports", "week2_violation_rate_history.jsonl")
    try:
        _safe_mkdir(os.path.dirname(history_path))
        with open(history_path, "a", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps({"captured_at": _utc_now_iso(), "violation_rate": float(violation_rate)}, ensure_ascii=False))
            f.write("\n")
    except Exception:
        pass

    baseline = {}
    baseline_rate = None
    if os.path.exists(baseline_path):
        try:
            baseline = json.load(open(baseline_path, "r", encoding="utf-8"))
            baseline_rate = float(baseline.get("output_schema_violation_rate"))
        except Exception:
            baseline_rate = None

    if baseline_rate is None:
        _safe_mkdir(os.path.dirname(baseline_path))
        json.dump(
            {"baseline_version": "1.0.0", "created_at": _utc_now_iso(), "output_schema_violation_rate": violation_rate},
            open(baseline_path, "w", encoding="utf-8"),
            ensure_ascii=False,
            indent=2,
        )
        baseline_rate = violation_rate

    threshold = 1.5 * max(baseline_rate, 1e-6)
    status = "PASS"
    if violation_rate > threshold:
        status = "WARN" if violation_rate <= (3.0 * max(baseline_rate, 1e-6)) else "FAIL"
        _append_violation(
            {
                "phase": "4A",
                "type": "AI",
                "severity": "HIGH" if status == "WARN" else "CRITICAL",
                "dataset": "week2_verdicts",
                "check": "output_schema_violation_rate",
                "message": f"Schema violation rate {violation_rate:.4f} exceeded 1.5x baseline {baseline_rate:.4f}",
                "recommendation": "Investigate prompt/template changes; restore prior schema; introduce versioned output adapters.",
            }
        )
        _append_violation(
            {
                "phase": "4A",
                "type": "AI",
                "severity": "MEDIUM",
                "dataset": "week2_verdicts",
                "check": "output_schema_violation_rate_trend",
                "message": f"WARN: output_schema_violation_rate={violation_rate:.4f} exceeded threshold={threshold:.4f}",
                "recommendation": "Monitor violation trend; if sustained increase, revert prompt/schema template or introduce versioned adapter.",
            }
        )

    trend = None
    try:
        rates: list[float] = []
        if os.path.exists(history_path):
            with open(history_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        r = obj.get("violation_rate")
                        if isinstance(r, (int, float)):
                            rates.append(float(r))
                    except Exception:
                        continue
        window = rates[-5:]
        if len(window) >= 2:
            trend = float(window[-1] - window[0])
    except Exception:
        trend = None

    return {
        "check": "llm_output_enforcement_week2",
        "status": status,
        "total_records": total,
        "schema_invalid": invalid_schema,
        "weighted_math_invalid": invalid_weighted,
        "target_ref_link_invalid": invalid_link,
        "output_schema_violation_rate": float(violation_rate),
        "violation_rate_trend_last5": trend,
        "baseline_output_schema_violation_rate": float(baseline_rate),
        "threshold_rate": float(threshold),
        "history_path": history_path,
        "samples": samples,
    }


def run_all(
    *,
    week3_extractions: str,
    traces_runs: str,
    week2_verdicts: str,
    week1_intents: str,
    out_report: str,
) -> dict[str, Any]:
    _safe_mkdir("validation_reports")
    _safe_mkdir("outputs/quarantine")

    drift = embedding_drift_week3(
        week3_extractions,
        baseline_path=os.path.join("validation_reports", "embedding_baseline_week3.json"),
        sample_n=200,
        drift_threshold=0.15,
    )

    prompt_schema = {
        "type": "object",
        "required": ["prompt", "context_bytes"],
        "properties": {"prompt": {"type": "string", "minLength": 1}, "context_bytes": {"type": "integer", "minimum": 0}},
        "additionalProperties": True,
    }
    prompt_inputs = validate_prompt_inputs(
        traces_runs,
        quarantine_dir=os.path.join("outputs", "quarantine"),
        schema=prompt_schema,
    )

    outputs = validate_llm_outputs_week2(
        week2_verdicts,
        intents_path=week1_intents,
        baseline_path=os.path.join("validation_reports", "ai_extensions_baseline.json"),
    )

    report = {
        "ai_extensions_version": "1.0.0",
        "generated_at": _utc_now_iso(),
        "results": {"embedding_drift": drift, "prompt_inputs": prompt_inputs, "llm_outputs": outputs},
    }
    with open(out_report, "w", encoding="utf-8", newline="\n") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 4A: AI Contract Extensions")
    parser.add_argument("--week3", default=os.path.join("outputs", "week3", "extractions.jsonl"))
    parser.add_argument("--traces", default=os.path.join("outputs", "traces", "runs.jsonl"))
    parser.add_argument("--week2", default=os.path.join("outputs", "week2", "verdicts.jsonl"))
    parser.add_argument("--week1", default=os.path.join("outputs", "week1", "intent_records.jsonl"))
    parser.add_argument("--out", default=os.path.join("validation_reports", "ai_extensions.json"))
    args = parser.parse_args()

    try:
        run_all(
            week3_extractions=args.week3,
            traces_runs=args.traces,
            week2_verdicts=args.week2,
            week1_intents=args.week1,
            out_report=args.out,
        )
        return 0
    except Exception as e:
        _safe_mkdir("validation_reports")
        with open(os.path.join("validation_reports", "ai_extensions_error.json"), "w", encoding="utf-8", newline="\n") as f:
            json.dump({"error": str(e)}, f, ensure_ascii=False, indent=2)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
