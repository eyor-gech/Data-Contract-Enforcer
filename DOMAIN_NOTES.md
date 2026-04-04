# DOMAIN NOTES — Week 7 Data Contract Enforcer

## Phase 0 — Evidence Snapshot

- `outputs/week3/extractions.jsonl` records: 200
- `outputs/week5/events.jsonl` records: 200
- `outputs/week2/verdicts.jsonl` records: 80
- `outputs/traces/runs.jsonl` records: 120

### Python-Derived Statistics (Week 3 Extractions)

- `confidence` stats: {'count': 200, 'min': 0.2, 'max': 1.2, 'mean': 0.5529999999999999, 'stdev': 0.39848588431712356}
- `threshold` stats: {'count': 200, 'min': 0.7, 'max': 0.7, 'mean': 0.7, 'stdev': 0.0}
- `processing_time_ms` stats: {'count': 200, 'min': 2905.3274000179954, 'max': 51353.10219996609, 'mean': 8596.84737550182, 'stdev': 6355.05497008448}
- `cost_usd` stats: {'count': 200, 'min': 0.0, 'max': 0.0, 'mean': 0.0, 'stdev': 0.0}
- `doc_id` cardinality: 7 (top: [('aba83fb1-f830-4349-827a-716c1d9779b6', 96), ('354a583f-472d-461f-8fc0-f7b04ee6a099', 60), ('ef3f0c4b-1838-4330-aa66-2ca820db8a9d', 15), ('527e9d58-01b7-49bc-ab5c-2ac679205641', 13), ('19a6adaa-b7cb-47f4-b752-5f79332a8830', 13)])

### Python-Derived Statistics (Week 5 Events)

- `global_position` stats: {'count': 200, 'min': -7.0, 'max': 200.0, 'mean': 100.455, 'stdev': 57.814513532503234}
- `event_type` cardinality: 17 (top: [('DocumentUploaded', 44), ('DocumentAdded', 44), ('ApplicationSubmitted', 20), ('DocumentUploadRequested', 19), ('PackageCreated', 18), ('AgentNodeExecuted', 9), ('DocumentFormatValidated', 6), ('ExtractionStarted', 6), ('AgentToolCalled', 6), ('ExtractionCompleted', 6)])

### Python-Derived Statistics (LangSmith Traces)

- `total_tokens` stats: {'count': 120, 'min': 86.0, 'max': 852.0, 'mean': 457.5083333333333, 'stdev': 177.35519895740927}

### Null Rate Highlights (Week 3)

- `text` null_fraction=1.000

## (1) Schema Change Taxonomy (Evidence-Derived)

A **backward-compatible** schema change is one where existing downstream consumers can keep operating without code changes (they may ignore new fields). A **breaking** schema change is one where a previously valid record can no longer be parsed/validated by downstream consumers, or where semantics change in a way that violates existing invariants.

### Backward-Compatible Changes — Concrete Examples

1) **Adding optional metadata fields**: In our migrated `week3_extractions` records, we carry `metadata.source` and `metadata.line_no` (see `outputs/week3/extractions.jsonl`). These additions do not affect the required fields (`extraction_id`, `doc_id`, `confidence`, etc.) and therefore do not break structural validation.

2) **Adding an optional error payload**: `traces_runs.error` is allowed to be null, and appears only when `status='error'`. Downstream can ignore `error` while still validating token math and timing fields.

3) **Adding a new event_type version (non-breaking with versioning)**: `week5_events` includes `event_version`; adding a new `event_type` *alongside* a version bump can be made backward-compatible when consumers gate by `event_type` + `event_version` and ignore unknown events.

### Breaking Changes — Concrete Examples

1) **Type change on a required field**: The observed Week 5 source events encode `payload.requested_amount_usd` as a string (example in cloned Week 5 outputs). Our migration explicitly coerces it to float. If we changed it back to string *without versioning*, numeric range checks and statistical checks in contract validation become invalid and downstream aggregations fail.

2) **Removing a required field**: We injected an `ApplicationSubmitted` record missing `payload.application_id` in `outputs/week5/events.jsonl`. This is a breaking change because `ApplicationSubmitted` consumers rely on application_id to correlate streams, and the Week 7 runner flags it as a CRITICAL semantic violation.

3) **Renaming a field without aliasing**: The Week 3 source ledger uses `confidence_score` while canonical uses `confidence`. Without a migration step, downstream that expects `confidence` sees nulls/absent data; the Week 7 runner enforces non-null + range and fails structurally.

## (2) Violation Tracing & Prevention — Confidence float[0..1] → int[0..100]

In our canonical Week 3 dataset, `confidence` is a numeric probability with observed distribution (mean=0.5529999999999999, stdev=0.39848588431712356, min=0.2, max=1.2). If upstream changes `confidence` to an integer percentage (0–100), two failures occur:
- **Structural/type failure**: the contract’s `type` rule expects `number` representing probability semantics, and the numeric range rule enforces [0.0, 1.0]. A value like 87 would violate the range rule immediately.
- **Propagation break into Week 4 Cartographer**: our lineage snapshots explicitly mark the edge evidence `dataset=week3_extractions, field=doc_id` but also rely on confidence for ranking/attribution. When confidence is no longer a probability, Cartographer-style prioritization would mis-rank low-quality extractions as high confidence, causing downstream event emission and audit decisions to be wrong.

### Bitol-Compatible YAML Clause (Prevention Gate)

```yaml
quality:
- type: custom
  engine: week7_enforcer
  implementation:
    dataset: week3_extractions
    rules:
    - type: type
      field: confidence
      expected: number
    - type: range
      field: confidence
      min: 0.0
      max: 1.0
```

## (3) Blame Chain Logic — Lineage Graph Traversal Algorithm

The Enforcer constructs a dataset dependency graph from `outputs/week4/lineage_snapshots.jsonl` by reading each edge’s `from_dataset` and `to_dataset`. For a detected violation on dataset D, it performs a deterministic reverse traversal (upstream search) to compute a blame chain:
1) Initialize `chain=[D]` and `seen={D}`.
2) While D has parents and depth < 8: choose the lexicographically smallest unseen parent P of D (to guarantee determinism), append P, and set D=P.
3) Reverse the accumulated list so the chain is ordered upstream→downstream.
This chain is written into every violation as `lineage_path`, enabling attribution even when the immediate failure appears downstream.

## (4) LangSmith Contract Specification — trace_record (Bitol/ODCS YAML)

Below is a contract design for `traces_runs` that includes one structural rule, one statistical rule, and one AI-specific rule. The Week 7 generator writes the full contract to `generated_contracts/traces_runs.yaml`.

```yaml
apiVersion: 3.0.0
kind: DataContract
name: traces_runs_contract
schema:
- name: traces_runs
  properties:
  - name: total_tokens
    logicalType: integer
    required: true
quality:
- type: custom
  engine: week7_enforcer
  implementation:
    dataset: traces_runs
    rules:
    - type: not_null
      field: total_tokens
    - type: zscore_drift
      field: latency_ms
      mean: 800.0
      stdev: 350.0
      max_z: 3.5
    - type: token_math
      field: total_tokens
```

## (5) Operational Sustainability — Preventing Schema Staleness

The dominant production failure mode for contract systems is **schema staleness**: contracts get written once and never updated when producers evolve. This architecture avoids obsolescence by making contracts **generated artifacts** derived from (a) canonical schemas and (b) observed runtime profiling. Concretely:
- The generator flattens records and profiles numeric fields (mean/stdev) and emits drift checks (z-score) so contracts remain sensitive to distribution changes, not only field presence.
- Canonical dataset specs encode strict invariants (UUID v4 ids, ISO8601 timestamps, confidence bounds) that must hold across weeks, and migrations turn source artifacts into canonical shape explicitly.
- Validation is lineage-aware: every violation includes a reverse-traversed dataset blame chain extracted from Week 4 lineage snapshots, so owners can remediate at the upstream origin rather than patching downstream.
- The system fails gracefully: both generator and runner emit error files instead of throwing exceptions.
In this repo, the first validation run summaries are persisted as JSON reports (see `validation_reports/`).

## Real First Validation Run Results (Evidence)

- Week 3 validation summary: {'total_records': 200, 'failed_records': 8, 'pass_rate': 0.96, 'total_rules': 54, 'rules_failed': 7, 'rows_affected': 8, 'failure_rate': 0.04}
- Week 3 violations (types): ['SCHEMA', 'SCHEMA', 'SCHEMA', 'SCHEMA', 'SCHEMA', 'SCHEMA', 'SEMANTIC']
- Week 5 validation summary: {'total_records': 200, 'failed_records': 5, 'pass_rate': 0.975, 'total_rules': 38, 'rules_failed': 5, 'rows_affected': 5, 'failure_rate': 0.025}
- Week 5 violations (types): ['SCHEMA', 'SCHEMA', 'SEMANTIC', 'SEMANTIC', 'SEMANTIC']

## Real Schema Mismatches Observed (Not Hypothetical)

- `week1_intent_records` source: `C:\\Users\\Eyor.G\\Documents\\Cloned\Week 1\outputs\week1\intent_records.jsonl`
  - observed keys: `timestamp, intent_id, tool, target, status, error_type, mutation_class, trace_id`
  - canonical additions: `actor, intent.code_refs, tool.args, outcome.error_message, created_at(ISO8601Z)`
- `week3_extractions` source: `C:\\Users\\Eyor.G\\Documents\\Cloned\Week 3\.refinery\extraction_ledger.jsonl`
  - observed keys: `document_id, page_number, strategy_used, confidence_score, threshold, processing_time, cost_estimate, escalation_occurred, flagged_for_review`
  - canonical additions: `extraction_id, trace_id, created_at, doc_id, processing_time_ms, cost_usd, flags`
- `week4_lineage_snapshots` source: `C:\\Users\\Eyor.G\\Documents\\Cloned\Week 4\outputs\week4\lineage_snapshots.jsonl`
  - observed keys: `source_datasets, target_datasets, transformation_type, source_file, line_range, dynamic_reference`
  - canonical additions: `snapshot_id, recorded_at, source, nodes, edges, from_dataset/to_dataset evidence`
- `week5_events` source: `C:\\Users\\Eyor.G\\Documents\\Cloned\Week 5\outputs\week5\events.jsonl`
  - observed keys: `global_position, stream_id, event_type, event_version, payload, metadata, recorded_at`
  - canonical additions: `event_id, trace_id, recorded_at(ISO8601Z), payload numeric coercions (explicit)`

## Contract Risk Assessment (Evidence-Backed)

The highest-risk contract surfaces in this system are the ones that combine (a) high fan-out lineage and (b) high semantic load: Week 3 extraction confidence and Week 5 event payload schemas. In our own data we intentionally injected three Week 3 structural violations (confidence>1, non-UUID doc_id, page_number=0) and Week 5 semantic violations (invalid recorded_at, monotonicity break, missing payload.application_id). These violations demonstrate where production systems typically drift: producers emit ‘almost-correct’ records that look valid to JSON parsers but break invariants required by downstream ranking, attribution, and event-sourcing replay.
