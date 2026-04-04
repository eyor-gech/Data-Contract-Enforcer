import { http } from "./http";
import {
  blameChainResponseSchema,
  contractStatusResponseSchema,
  driftResponseSchema,
  executiveLlmSummarySchema,
  healthResponseSchema,
  llmViolationsResponseSchema,
  schemaDiffResponseSchema,
  demoAiExtensionsSchema,
  demoGenerateContractSchema,
  demoGenerateReportSchema,
  demoRunAttributionSchema,
  demoRunValidationSchema,
  demoSchemaEvolutionSchema
} from "./schemas";
import type {
  NormalizedBlameChain,
  NormalizedContractMap,
  NormalizedDrift,
  NormalizedHealth,
  NormalizedSchemaDiff,
  NormalizedTrendPoint
} from "./types";

function clamp01to100(n: number) {
  if (Number.isNaN(n)) return 0;
  return Math.max(0, Math.min(100, n));
}

function stringifyMaybeYamlOrJson(value: unknown): string {
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function normalizeStatus(raw: unknown): "OK" | "BROKEN" | "UNKNOWN" {
  if (typeof raw === "boolean") return raw ? "OK" : "BROKEN";
  if (typeof raw !== "string") return "UNKNOWN";
  const s = raw.toLowerCase();
  if (["ok", "pass", "passed", "green", "healthy", "true"].includes(s)) return "OK";
  if (["broken", "fail", "failed", "red", "unhealthy", "false"].includes(s)) return "BROKEN";
  return "UNKNOWN";
}

function normalizeSeverity(raw: unknown): "LOW" | "MEDIUM" | "HIGH" | "CRITICAL" | "UNKNOWN" {
  if (typeof raw !== "string") return "UNKNOWN";
  const s = raw.toLowerCase();
  if (["critical", "sev1", "p0"].includes(s)) return "CRITICAL";
  if (["high", "sev2", "p1"].includes(s)) return "HIGH";
  if (["medium", "sev3", "p2"].includes(s)) return "MEDIUM";
  if (["low", "sev4", "p3"].includes(s)) return "LOW";
  return "UNKNOWN";
}

export async function getHealth(): Promise<NormalizedHealth> {
  const res = await http.get("/api/health");
  const parsed = healthResponseSchema.safeParse(res.data);

  if (!parsed.success) {
    return {
      score: 0,
      narrative:
        "Health endpoint returned an unexpected shape. See the console for raw payload.",
      topRisks: []
    };
  }

  const d = parsed.data;
  const score = d.score ?? d.health_score ?? 0;
  const narrative = d.summary ?? d.narrative ?? "Overall data contract health summary.";
  const topRisks = d.top_risks ?? d.risks ?? [];
  return { score: clamp01to100(score), narrative, topRisks: topRisks.slice(0, 3) };
}

export type ExecutiveLlmBrief = {
  narrative: string;
  risks: string[];
  actions: string[];
  generatedAt?: string;
};

export async function getExecutiveLlmSummary(): Promise<ExecutiveLlmBrief> {
  const res = await http.get("/api/executive-llm-summary");
  const parsed = executiveLlmSummarySchema.safeParse(res.data);
  if (!parsed.success) {
    return {
      narrative: "AI summary endpoint returned an unexpected shape.",
      risks: [],
      actions: []
    };
  }
  const d = parsed.data;
  return {
    narrative: d.narrative ?? "",
    risks: d.risks ?? [],
    actions: d.actions ?? [],
    generatedAt: d.generated_at
  };
}

export type DemoContractResult = {
  yaml: string;
  clauseCount: number;
  confidenceClause?: unknown;
  confidenceClausePresent: boolean;
};

export async function postGenerateContract(): Promise<DemoContractResult> {
  const res = await http.post("/api/generate-contract", {});
  const parsed = demoGenerateContractSchema.safeParse(res.data);
  if (!parsed.success) {
    throw new Error("Unexpected response from /generate-contract");
  }
  const d = parsed.data;
  return {
    yaml: d.yaml,
    clauseCount: d.clause_count ?? 0,
    confidenceClause: d.highlight_confidence_clause?.clause,
    confidenceClausePresent: d.highlight_confidence_clause?.present ?? false
  };
}

export type DemoCheckRow = {
  name: string;
  result: "PASS" | "FAIL";
  severity?: string;
  recordsFailing?: number;
  message?: string;
};

export async function postRunValidation(): Promise<{ checks: DemoCheckRow[] }> {
  const res = await http.post("/api/run-validation", {});
  const parsed = demoRunValidationSchema.safeParse(res.data);
  if (!parsed.success) throw new Error("Unexpected response from /run-validation");
  const checks = (parsed.data.checks ?? []).map((c) => ({
    name: c.name,
    result: (c.result?.toUpperCase() === "PASS" ? "PASS" : "FAIL") as "PASS" | "FAIL",
    severity: c.severity,
    recordsFailing: c.records_failing,
    message: c.message
  }));
  return { checks };
}

export async function postRunAttribution(): Promise<{
  lineage: string[];
  commitHash?: string | null;
  author?: string | null;
  blastRadius: string[];
}> {
  const res = await http.post("/api/run-attribution", {});
  const parsed = demoRunAttributionSchema.safeParse(res.data);
  if (!parsed.success) throw new Error("Unexpected response from /run-attribution");
  return {
    lineage: parsed.data.lineage ?? [],
    commitHash: parsed.data.commit_hash ?? null,
    author: parsed.data.author ?? null,
    blastRadius: parsed.data.blast_radius ?? []
  };
}

export async function postSchemaEvolution(): Promise<{
  breakingChange: boolean;
  classification: string;
  migrationReport: string;
  keyActions: string[];
  riskLevel?: string;
}> {
  const res = await http.post("/api/schema-evolution", {});
  const parsed = demoSchemaEvolutionSchema.safeParse(res.data);
  if (!parsed.success) throw new Error("Unexpected response from /schema-evolution");
  return {
    breakingChange: parsed.data.breaking_change ?? false,
    classification: parsed.data.classification ?? "UNKNOWN",
    migrationReport: parsed.data.migration_report ?? "",
    keyActions: parsed.data.key_actions ?? [],
    riskLevel: parsed.data.risk_level
  };
}

export async function postAiExtensions(): Promise<{
  embeddingDriftScore: number;
  promptValidation: string;
  schemaViolationRate: number;
  explanation?: string | null;
  recommendedActions: string[];
}> {
  const res = await http.post("/api/ai-extensions", { refresh: false });
  const parsed = demoAiExtensionsSchema.safeParse(res.data);
  if (!parsed.success) throw new Error("Unexpected response from /ai-extensions");
  return {
    embeddingDriftScore: parsed.data.embedding_drift_score ?? 0,
    promptValidation: parsed.data.prompt_validation ?? "UNKNOWN",
    schemaViolationRate: parsed.data.schema_violation_rate ?? 0,
    explanation: parsed.data.explanation ?? null,
    recommendedActions: parsed.data.recommended_actions ?? []
  };
}

export async function postGenerateReport(): Promise<{
  dataHealthScore: number;
  topViolations: string[];
  narrative?: string | null;
}> {
  const res = await http.post("/api/generate-report", { refresh: false });
  const parsed = demoGenerateReportSchema.safeParse(res.data);
  if (!parsed.success) throw new Error("Unexpected response from /generate-report");
  return {
    dataHealthScore: parsed.data.data_health_score ?? 0,
    topViolations: parsed.data.top_violations ?? [],
    narrative: parsed.data.narrative ?? null
  };
}

export async function generateFinalReportPdf(): Promise<Blob> {
  /**
   * Optional backend integration:
   * - If your backend returns a PDF directly: `GET /api/report/pdf`
   * - If it triggers generation then returns PDF: replace with `POST /api/report/generate`
   */
  const res = await http.get("/api/report/pdf", { responseType: "blob" });
  return res.data as Blob;
}

export async function getContractStatus(): Promise<NormalizedContractMap> {
  const res = await http.get("/api/contract-status");
  const parsed = contractStatusResponseSchema.safeParse(res.data);

  if (!parsed.success) {
    return { nodes: [], edges: [] };
  }

  const d = parsed.data;
  const nodes = (d.nodes ?? []).map((n) => ({
    id: n.id,
    label: n.label ?? n.id,
    group: n.group
  }));

  const rawEdges = d.edges ?? d.links ?? [];
  const edges = rawEdges
    .map((e: any, idx: number) => {
    const source = (e.source ?? e.from ?? "").toString();
    const target = (e.target ?? e.to ?? "").toString();
    const status = normalizeStatus(e.status ?? e.ok);
    const promises: string[] =
      e.promises ??
      (typeof e.contract === "object" && e.contract && "promises" in e.contract
        ? (e.contract as any).promises
        : []) ??
      [];
    const id = (e.id ?? `${source}->${target}` ?? `edge-${idx}`).toString();
    return { id, source, target, status, promises, raw: e };
  })
    .filter((e) => Boolean(e.source) && Boolean(e.target));

  const haveNodes = nodes.length > 0;
  const synthesizedNodes = haveNodes
    ? nodes
    : Array.from(
        new Set(edges.flatMap((e) => [e.source, e.target]).filter(Boolean))
      ).map((id) => ({ id, label: id }));

  return { nodes: synthesizedNodes, edges };
}

export async function getBlameChain(): Promise<NormalizedBlameChain> {
  const res = await http.get("/api/blame-chain");
  const parsed = blameChainResponseSchema.safeParse(res.data);

  if (!parsed.success) {
    return {
      breakingChange: false,
      violations: [],
      blastRadius: { affected: [] }
    };
  }

  const d = parsed.data;
  const violations = (d.violations ?? []).map((v, idx) => ({
    id: v.id ?? `v-${idx}`,
    timestamp: v.timestamp ?? v.time,
    system: v.system ?? v.dataset,
    message: v.message ?? v.clause_id ?? "Violation detected.",
    severity: normalizeSeverity(v.severity),
    downstream: v.downstream ?? [],
    raw: v
  }));

  return {
    breakingChange: d.breaking_change ?? false,
    alertText: d.alert,
    violations,
    blastRadius: {
      source: d.blast_radius?.source,
      affected: d.blast_radius?.affected ?? []
    }
  };
}

export async function getSchemaDiff(): Promise<NormalizedSchemaDiff> {
  const res = await http.get("/api/schema-diff");
  const parsed = schemaDiffResponseSchema.safeParse(res.data);

  if (!parsed.success) {
    return {
      beforeText: "",
      afterText: "",
      verdict: "UNKNOWN",
      checklist: [],
      raw: res.data
    };
  }

  const d = parsed.data;
  const verdictRaw = (d.verdict ?? d.compatibility ?? "").toString().toUpperCase();
  const verdict =
    verdictRaw.includes("SAFE") || verdictRaw.includes("PASS")
      ? "SAFE"
      : verdictRaw.includes("DANGER") || verdictRaw.includes("BREAK")
        ? "DANGEROUS"
        : "UNKNOWN";

  const checklist = d.checklist ?? d.migration_checklist ?? [];
  return {
    beforeText: stringifyMaybeYamlOrJson(d.before),
    afterText: stringifyMaybeYamlOrJson(d.after),
    verdict,
    checklist,
    raw: d
  };
}

export async function getAiDrift(): Promise<NormalizedDrift> {
  const res = await http.get("/api/ai-drift");
  const parsed = driftResponseSchema.safeParse(res.data);
  if (!parsed.success) return { score: 0 };
  const d = parsed.data;
  const score = Number(d.drift ?? d.drift_score ?? d.score ?? 0);
  return { score: clamp01to100(score), narrative: d.narrative };
}

export async function getLlmViolations(): Promise<NormalizedTrendPoint[]> {
  const res = await http.get("/api/llm-violations");
  const parsed = llmViolationsResponseSchema.safeParse(res.data);
  if (!parsed.success) return [];

  const d = parsed.data;
  const points =
    d.points ??
    (d.violations ?? []).map((v) => ({
      date: v.date ?? v.timestamp ?? "",
      count: v.count ?? v.total ?? 0,
      severity: v.severity
    }));

  return (points ?? [])
    .filter((p) => p.date)
    .map((p: any) => ({
      date: p.date,
      count: Number(p.count ?? 0),
      severity: p.severity
    }));
}
