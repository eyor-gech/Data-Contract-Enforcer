export type NormalizedHealth = {
  score: number;
  narrative: string;
  topRisks: string[];
};

export type NormalizedContractMap = {
  nodes: { id: string; label: string; group?: string }[];
  edges: {
    id: string;
    source: string;
    target: string;
    status: "OK" | "BROKEN" | "UNKNOWN";
    promises: string[];
    raw: unknown;
  }[];
};

export type NormalizedViolation = {
  id: string;
  timestamp?: string;
  system?: string;
  message: string;
  severity: "LOW" | "MEDIUM" | "HIGH" | "CRITICAL" | "UNKNOWN";
  downstream: string[];
  raw: unknown;
};

export type NormalizedBlameChain = {
  breakingChange: boolean;
  alertText?: string;
  violations: NormalizedViolation[];
  blastRadius: { source?: string; affected: string[] };
};

export type NormalizedSchemaDiff = {
  beforeText: string;
  afterText: string;
  verdict: "SAFE" | "DANGEROUS" | "UNKNOWN";
  checklist: string[];
  raw: unknown;
};

export type NormalizedDrift = {
  score: number;
  narrative?: string;
};

export type NormalizedTrendPoint = {
  date: string;
  count: number;
  severity?: string;
};

