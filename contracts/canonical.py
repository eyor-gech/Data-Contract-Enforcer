from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FieldSpec:
    path: str
    logical_type: str  # string|integer|number|boolean|object|array|datetime|uuid
    required: bool = True
    enum: list[Any] | None = None
    minimum: float | int | None = None
    maximum: float | int | None = None
    description: str | None = None


@dataclass(frozen=True)
class DatasetSpec:
    dataset: str
    fields: list[FieldSpec]
    min_records: int = 50


def canonical_specs() -> dict[str, DatasetSpec]:
    """
    Canonical Week1–Week5 + traces datasets used by Week 7 enforcer.
    """
    return {
        "week1_intent_records": DatasetSpec(
            dataset="week1_intent_records",
            fields=[
                FieldSpec("intent_id", "uuid", True),
                FieldSpec("trace_id", "uuid", True),
                FieldSpec("created_at", "datetime", True),
                FieldSpec("actor.agent_id", "string", True),
                FieldSpec("actor.agent_role", "string", True),
                FieldSpec("intent.type", "string", True, enum=["READ", "WRITE", "RUN", "PLAN"]),
                FieldSpec("intent.description", "string", True),
                FieldSpec("intent.code_refs", "array", True),
                FieldSpec("tool.name", "string", True),
                FieldSpec("tool.args", "object", True),
                FieldSpec("outcome.status", "string", True, enum=["SUCCESS", "FAILED"]),
                FieldSpec("outcome.error_type", "string", False),
                FieldSpec("outcome.error_message", "string", False),
                FieldSpec("mutation_class", "string", True),
            ],
        ),
        "week2_verdicts": DatasetSpec(
            dataset="week2_verdicts",
            fields=[
                FieldSpec("verdict_id", "uuid", True),
                FieldSpec("trace_id", "uuid", True),
                FieldSpec("intent_id", "uuid", True),
                FieldSpec("created_at", "datetime", True),
                FieldSpec("target_ref.file", "string", True),
                FieldSpec("target_ref.span.start_line", "integer", True, minimum=1),
                FieldSpec("target_ref.span.end_line", "integer", True, minimum=1),
                FieldSpec("model.provider", "string", True, enum=["openai", "anthropic"]),
                FieldSpec("model.name", "string", True),
                FieldSpec("scores.correctness", "integer", True, minimum=1, maximum=5),
                FieldSpec("scores.safety", "integer", True, minimum=1, maximum=5),
                FieldSpec("scores.style", "integer", True, minimum=1, maximum=5),
                FieldSpec("scores.weighted_score", "number", True, minimum=1.0, maximum=5.0),
                FieldSpec("scores.weights", "object", True),
                FieldSpec("verdict.label", "string", True, enum=["APPROVE", "REJECT", "NEEDS_WORK"]),
                FieldSpec("verdict.rationale", "string", True),
                FieldSpec("verdict.confidence", "number", True, minimum=0.0, maximum=1.0),
            ],
        ),
        "week3_extractions": DatasetSpec(
            dataset="week3_extractions",
            fields=[
                FieldSpec("extraction_id", "uuid", True),
                FieldSpec("trace_id", "uuid", True),
                FieldSpec("created_at", "datetime", True),
                FieldSpec("doc_id", "uuid", True),
                FieldSpec("doc_key", "string", True),
                FieldSpec("page_number", "integer", True, minimum=1),
                FieldSpec("strategy.name", "string", True, enum=["vision", "ocr", "hybrid"]),
                FieldSpec("confidence", "number", True, minimum=0.0, maximum=1.0),
                FieldSpec("threshold", "number", True, minimum=0.0, maximum=1.0),
                FieldSpec("processing_time_ms", "number", True, minimum=0.0),
                FieldSpec("cost_usd", "number", True, minimum=0.0),
                FieldSpec("flags.escalated", "boolean", True),
                FieldSpec("flags.flagged_for_review", "boolean", True),
                FieldSpec("text", "string", False),
                FieldSpec("labels", "array", True),
            ],
        ),
        "week4_lineage_snapshots": DatasetSpec(
            dataset="week4_lineage_snapshots",
            fields=[
                FieldSpec("snapshot_id", "uuid", True),
                FieldSpec("recorded_at", "datetime", True),
                FieldSpec("source.system", "string", True),
                FieldSpec("source.version", "string", True),
                FieldSpec("nodes", "array", True),
                FieldSpec("edges", "array", True),
            ],
        ),
        "week5_events": DatasetSpec(
            dataset="week5_events",
            fields=[
                FieldSpec("event_id", "uuid", True),
                FieldSpec("trace_id", "uuid", True),
                FieldSpec("global_position", "integer", True, minimum=1),
                FieldSpec("stream_id", "string", True),
                FieldSpec("event_type", "string", True),
                FieldSpec("event_version", "integer", True, minimum=1),
                FieldSpec("payload", "object", True),
                FieldSpec("metadata.correlation_id", "string", True),
                FieldSpec("metadata.causation_id", "string", False),
                FieldSpec("metadata.generated_by", "string", True),
                FieldSpec("recorded_at", "datetime", True),
            ],
        ),
        "traces_runs": DatasetSpec(
            dataset="traces_runs",
            fields=[
                FieldSpec("run_id", "uuid", True),
                FieldSpec("trace_id", "uuid", True),
                FieldSpec("provider", "string", True, enum=["langsmith"]),
                FieldSpec("project", "string", True),
                FieldSpec("name", "string", True),
                FieldSpec("start_time", "datetime", True),
                FieldSpec("end_time", "datetime", True),
                FieldSpec("latency_ms", "number", True, minimum=0.0),
                FieldSpec("prompt_tokens", "integer", True, minimum=0),
                FieldSpec("completion_tokens", "integer", True, minimum=0),
                FieldSpec("total_tokens", "integer", True, minimum=0),
                FieldSpec("cost_usd", "number", True, minimum=0.0),
                FieldSpec("status", "string", True, enum=["success", "error"]),
                FieldSpec("error", "object", False),
                FieldSpec("inputs", "object", True),
                FieldSpec("outputs", "object", True),
            ],
        ),
    }


REQUIRED_DATASETS: list[str] = [
    "outputs/week1/intent_records.jsonl",
    "outputs/week2/verdicts.jsonl",
    "outputs/week3/extractions.jsonl",
    "outputs/week4/lineage_snapshots.jsonl",
    "outputs/week5/events.jsonl",
    "outputs/traces/runs.jsonl",
]

