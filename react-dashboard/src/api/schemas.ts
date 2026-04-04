import { z } from "zod";

const numberLike = z.union([z.number(), z.string().transform((s) => Number(s))]);

/**
 * The backend response shapes can vary by implementation.
 * These zod schemas intentionally accept a few common field aliases so the UI
 * remains resilient while still giving you strong TS types after normalization.
 */
export const healthResponseSchema = z
  .object({
    score: z.number().min(0).max(100).optional(),
    health_score: z.number().min(0).max(100).optional(),
    summary: z.string().optional(),
    narrative: z.string().optional(),
    top_risks: z.array(z.string()).optional(),
    risks: z.array(z.string()).optional()
  })
  .passthrough();

export const driftResponseSchema = z
  .object({
    drift: numberLike.optional(),
    drift_score: numberLike.optional(),
    score: numberLike.optional(),
    narrative: z.string().optional()
  })
  .passthrough();

export const llmViolationsResponseSchema = z
  .object({
    points: z
      .array(
        z
          .object({
            date: z.string(),
            count: numberLike,
            severity: z.string().optional()
          })
          .passthrough()
      )
      .optional(),
    violations: z
      .array(
        z
          .object({
            date: z.string().optional(),
            timestamp: z.string().optional(),
            count: numberLike.optional(),
            total: numberLike.optional(),
            severity: z.string().optional()
          })
          .passthrough()
      )
      .optional()
  })
  .passthrough();

export const contractStatusResponseSchema = z
  .object({
    nodes: z
      .array(
        z.object({
          id: z.string(),
          label: z.string().optional(),
          group: z.string().optional()
        })
      )
      .optional(),
    edges: z
      .array(
        z
          .object({
            id: z.string().optional(),
            source: z.string().optional(),
            target: z.string().optional(),
            from: z.string().optional(),
            to: z.string().optional(),
            status: z.string().optional(),
            ok: z.boolean().optional(),
            promises: z.array(z.string()).optional(),
            contract: z.unknown().optional()
          })
          .passthrough()
      )
      .optional(),
    links: z
      .array(
        z
          .object({
            from: z.string(),
            to: z.string(),
            status: z.string().optional(),
            promises: z.array(z.string()).optional()
          })
          .passthrough()
      )
      .optional()
  })
  .passthrough();

export const blameChainResponseSchema = z
  .object({
    breaking_change: z.boolean().optional(),
    alert: z.string().optional(),
    violations: z
      .array(
        z
          .object({
            id: z.string().optional(),
            timestamp: z.string().optional(),
            time: z.string().optional(),
            system: z.string().optional(),
            dataset: z.string().optional(),
            message: z.string().optional(),
            clause_id: z.string().optional(),
            severity: z.string().optional(),
            downstream: z.array(z.string()).optional()
          })
          .passthrough()
      )
      .optional(),
    blast_radius: z
      .object({
        source: z.string().optional(),
        affected: z.array(z.string()).optional()
      })
      .optional()
  })
  .passthrough();

export const schemaDiffResponseSchema = z
  .object({
    before: z.unknown().optional(),
    after: z.unknown().optional(),
    diff: z.string().optional(),
    verdict: z.string().optional(),
    compatibility: z.string().optional(),
    checklist: z.array(z.string()).optional(),
    migration_checklist: z.array(z.string()).optional()
  })
  .passthrough();

export const executiveLlmSummarySchema = z
  .object({
    narrative: z.string().optional(),
    risks: z.array(z.string()).optional(),
    actions: z.array(z.string()).optional(),
    generated_at: z.string().optional()
  })
  .passthrough();

export const demoGenerateContractSchema = z
  .object({
    yaml: z.string(),
    clause_count: z.number().optional(),
    highlight_confidence_clause: z
      .object({
        present: z.boolean().optional(),
        clause: z.any().optional()
      })
      .optional()
  })
  .passthrough();

export const demoRunValidationSchema = z
  .object({
    checks: z
      .array(
        z.object({
          name: z.string(),
          result: z.string(),
          severity: z.string().optional(),
          records_failing: z.number().optional(),
          field: z.string().optional(),
          message: z.string().optional()
        })
      )
      .optional(),
    summary: z.any().optional()
  })
  .passthrough();

export const demoRunAttributionSchema = z
  .object({
    lineage: z.array(z.string()).optional(),
    commit_hash: z.string().nullable().optional(),
    author: z.string().nullable().optional(),
    blast_radius: z.array(z.string()).optional()
  })
  .passthrough();

export const demoSchemaEvolutionSchema = z
  .object({
    breaking_change: z.boolean().optional(),
    classification: z.string().optional(),
    migration_report: z.string().optional(),
    key_actions: z.array(z.string()).optional(),
    risk_level: z.string().optional()
  })
  .passthrough();

export const demoAiExtensionsSchema = z
  .object({
    embedding_drift_score: z.number().optional(),
    prompt_validation: z.string().optional(),
    schema_violation_rate: z.number().optional(),
    explanation: z.string().nullable().optional(),
    recommended_actions: z.array(z.string()).optional()
  })
  .passthrough();

export const demoGenerateReportSchema = z
  .object({
    data_health_score: z.number().optional(),
    top_violations: z.array(z.string()).optional(),
    narrative: z.string().nullable().optional()
  })
  .passthrough();
