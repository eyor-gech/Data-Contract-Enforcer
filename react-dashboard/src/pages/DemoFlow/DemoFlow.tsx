import React from "react";
import { useMutation } from "@tanstack/react-query";
import {
  Alert,
  Box,
  Button,
  Card,
  CardContent,
  Chip,
  Divider,
  Grid,
  LinearProgress,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Typography
} from "@mui/material";
import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import AutoFixHighIcon from "@mui/icons-material/AutoFixHigh";
import CheckCircleIcon from "@mui/icons-material/CheckCircle";

import PageShell from "../../components/PageShell";
import MetricGauge from "../../components/MetricGauge";
import StatusPill from "../../components/StatusPill";
import {
  postAiExtensions,
  postGenerateContract,
  postGenerateReport,
  postRunAttribution,
  postRunValidation,
  postSchemaEvolution
} from "../../api/endpoints";

function CodePanel({ text, label }: { text: string; label: string }) {
  return (
    <Box>
      <Typography variant="subtitle2" sx={{ fontWeight: 800, mb: 1 }}>
        {label}
      </Typography>
      <Box
        component="pre"
        sx={{
          m: 0,
          p: 1.5,
          bgcolor: "#0B1020",
          color: "#E6E8F0",
          borderRadius: 2,
          overflow: "auto",
          maxHeight: 340,
          fontSize: 12,
          lineHeight: 1.45
        }}
      >
        {text}
      </Box>
    </Box>
  );
}

function StepHeader({
  step,
  title,
  rubricHighlights,
  running,
  done,
  onRun,
  runLabel = "Run"
}: {
  step: number;
  title: string;
  rubricHighlights: string[];
  running: boolean;
  done: boolean;
  onRun: () => void;
  runLabel?: string;
}) {
  return (
    <Box sx={{ display: "flex", alignItems: "flex-start", gap: 2 }}>
      <Box sx={{ flex: 1 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 1, flexWrap: "wrap" }}>
          <Typography variant="h6" sx={{ fontWeight: 900 }}>
            Step {step}: {title}
          </Typography>
          {done ? (
            <Chip
              icon={<CheckCircleIcon />}
              label="Completed"
              color="success"
              size="small"
              variant="outlined"
            />
          ) : null}
        </Box>
        <Stack direction="row" useFlexGap flexWrap="wrap" gap={1} sx={{ mt: 1 }}>
          {rubricHighlights.map((h) => (
            <Chip key={h} label={h} size="small" variant="outlined" />
          ))}
        </Stack>
      </Box>
      <Button
        variant="contained"
        startIcon={<PlayArrowIcon />}
        onClick={onRun}
        disabled={running}
      >
        {running ? "Running…" : runLabel}
      </Button>
    </Box>
  );
}

export default function DemoFlow() {
  const [contractYaml, setContractYaml] = React.useState<string | null>(null);
  const [clauseCount, setClauseCount] = React.useState<number | null>(null);
  const [confidenceClausePresent, setConfidenceClausePresent] = React.useState<boolean | null>(
    null
  );
  const [confidenceClauseText, setConfidenceClauseText] = React.useState<string | null>(null);

  const [validationChecks, setValidationChecks] = React.useState<
    { name: string; result: "PASS" | "FAIL"; severity?: string; recordsFailing?: number }[]
  >([]);

  const [lineage, setLineage] = React.useState<string[]>([]);
  const [commitHash, setCommitHash] = React.useState<string | null>(null);
  const [author, setAuthor] = React.useState<string | null>(null);
  const [blastRadius, setBlastRadius] = React.useState<string[]>([]);

  const [breakingChange, setBreakingChange] = React.useState<boolean | null>(null);
  const [classification, setClassification] = React.useState<string | null>(null);
  const [migrationReport, setMigrationReport] = React.useState<string | null>(null);
  const [migrationActions, setMigrationActions] = React.useState<string[]>([]);

  const [embeddingDriftScore, setEmbeddingDriftScore] = React.useState<number | null>(null);
  const [promptValidation, setPromptValidation] = React.useState<string | null>(null);
  const [schemaViolationRate, setSchemaViolationRate] = React.useState<number | null>(null);
  const [aiExplanation, setAiExplanation] = React.useState<string | null>(null);
  const [aiActions, setAiActions] = React.useState<string[]>([]);

  const [healthScore, setHealthScore] = React.useState<number | null>(null);
  const [topViolations, setTopViolations] = React.useState<string[]>([]);
  const [reportNarrative, setReportNarrative] = React.useState<string | null>(null);

  const generateContract = useMutation({ mutationFn: postGenerateContract });
  const runValidation = useMutation({ mutationFn: postRunValidation });
  const runAttribution = useMutation({ mutationFn: postRunAttribution });
  const schemaEvolution = useMutation({ mutationFn: postSchemaEvolution });
  const aiExtensions = useMutation({ mutationFn: postAiExtensions });
  const generateReport = useMutation({ mutationFn: postGenerateReport });

  const [fullDemoRunning, setFullDemoRunning] = React.useState(false);
  const [fullDemoError, setFullDemoError] = React.useState<string | null>(null);

  const done1 = Boolean(contractYaml);
  const done2 = validationChecks.length > 0;
  const done3 = lineage.length > 0 && (commitHash || author) && blastRadius.length > 0;
  const done4 = breakingChange !== null && Boolean(classification) && Boolean(migrationReport);
  const done5 =
    embeddingDriftScore !== null && Boolean(promptValidation) && schemaViolationRate !== null;
  const done6 = healthScore !== null && topViolations.length > 0;

  async function runStep1() {
    const r = await generateContract.mutateAsync();
    setContractYaml(r.yaml);
    setClauseCount(r.clauseCount);
    setConfidenceClausePresent(r.confidenceClausePresent);
    setConfidenceClauseText(
      r.confidenceClause ? JSON.stringify(r.confidenceClause, null, 2) : null
    );
  }

  async function runStep2() {
    const r = await runValidation.mutateAsync();
    setValidationChecks(r.checks);
  }

  async function runStep3() {
    const r = await runAttribution.mutateAsync();
    setLineage(r.lineage);
    setCommitHash(r.commitHash ?? null);
    setAuthor(r.author ?? null);
    setBlastRadius(r.blastRadius);
  }

  async function runStep4() {
    const r = await schemaEvolution.mutateAsync();
    setBreakingChange(r.breakingChange);
    setClassification(r.classification);
    setMigrationReport(r.migrationReport);
    setMigrationActions(r.keyActions);
  }

  async function runStep5() {
    const r = await aiExtensions.mutateAsync();
    setEmbeddingDriftScore(r.embeddingDriftScore);
    setPromptValidation(r.promptValidation);
    setSchemaViolationRate(r.schemaViolationRate);
    setAiExplanation(r.explanation ?? null);
    setAiActions(r.recommendedActions);
  }

  async function runStep6() {
    const r = await generateReport.mutateAsync();
    setHealthScore(r.dataHealthScore);
    setTopViolations(r.topViolations);
    setReportNarrative(r.narrative ?? null);
  }

  async function runFullDemo() {
    setFullDemoRunning(true);
    setFullDemoError(null);
    try {
      if (!done1) await runStep1();
      if (!done2) await runStep2();
      if (!done3) await runStep3();
      if (!done4) await runStep4();
      if (!done5) await runStep5();
      if (!done6) await runStep6();
    } catch (e) {
      setFullDemoError(String((e as any)?.message ?? e));
    } finally {
      setFullDemoRunning(false);
    }
  }

  const llmHint = (
    <Alert severity="info" icon={<AutoFixHighIcon />}>
      For the best demo experience, set `OPENROUTER_API_KEY` in `adapter_api/.env` so the app can
      generate plain-language summaries.
    </Alert>
  );

  return (
    <>
      <PageShell
        title="Guided Demo (6 steps)"
        subtitle="A linear, executive-friendly walkthrough that proves rubric requirements with one click per step."
        right={
          <Button
            variant="contained"
            startIcon={<PlayArrowIcon />}
            onClick={runFullDemo}
            disabled={fullDemoRunning}
          >
            {fullDemoRunning ? "Running full demo…" : "Run Full Demo"}
          </Button>
        }
      />

      {fullDemoRunning ? <LinearProgress sx={{ mb: 2 }} /> : null}
      {fullDemoError ? (
        <Alert severity="error" sx={{ mb: 2 }}>
          Full demo failed: {fullDemoError}
        </Alert>
      ) : null}

      <Box sx={{ mb: 2 }}>{llmHint}</Box>

      <Grid container spacing={2}>
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <StepHeader
                step={1}
                title="Contract Generation"
                rubricHighlights={["≥ 8 clauses", "Confidence clause highlighted", "YAML shown"]}
                running={generateContract.isPending}
                done={done1}
                onRun={() => runStep1()}
                runLabel="Generate Contract"
              />

              <Divider sx={{ my: 2 }} />

              {generateContract.isError ? (
                <Alert severity="error">
                  {String((generateContract.error as any)?.message ?? generateContract.error)}
                </Alert>
              ) : null}

              {contractYaml ? (
                <Grid container spacing={2}>
                  <Grid item xs={12} md={7}>
                    <CodePanel text={contractYaml} label="Generated Contract (YAML)" />
                  </Grid>
                  <Grid item xs={12} md={5}>
                    <Typography variant="subtitle2" sx={{ fontWeight: 800 }}>
                      Rubric checks
                    </Typography>
                    <Stack gap={1} sx={{ mt: 1 }}>
                      <Alert severity={clauseCount && clauseCount >= 8 ? "success" : "warning"}>
                        Clause count: <b>{clauseCount ?? "—"}</b> (target ≥ 8)
                      </Alert>
                      <Alert severity={confidenceClausePresent ? "success" : "warning"}>
                        Confidence clause present: <b>{confidenceClausePresent ? "YES" : "NO"}</b>
                      </Alert>
                    </Stack>
                    {confidenceClauseText ? (
                      <Box sx={{ mt: 2 }}>
                        <CodePanel text={confidenceClauseText} label="Highlighted confidence clause" />
                      </Box>
                    ) : null}
                  </Grid>
                </Grid>
              ) : (
                <Typography variant="body2" color="text.secondary">
                  Click “Generate Contract” to produce YAML from real data.
                </Typography>
              )}
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12}>
          <Card>
            <CardContent>
              <StepHeader
                step={2}
                title="Violation Detection"
                rubricHighlights={["FAIL on confidence_range", "Severity shown", "records_failing shown"]}
                running={runValidation.isPending}
                done={done2}
                onRun={() => runStep2()}
                runLabel="Run Validation"
              />
              <Divider sx={{ my: 2 }} />
              {runValidation.isError ? (
                <Alert severity="error">
                  {String((runValidation.error as any)?.message ?? runValidation.error)}
                </Alert>
              ) : null}
              {validationChecks.length ? (
                <Table size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell sx={{ fontWeight: 800 }}>Check</TableCell>
                      <TableCell sx={{ fontWeight: 800 }}>Result</TableCell>
                      <TableCell sx={{ fontWeight: 800 }}>Severity</TableCell>
                      <TableCell sx={{ fontWeight: 800 }}>Records failing</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {validationChecks.map((c) => {
                      const highlight = c.name === "confidence_range" && c.result === "FAIL";
                      return (
                        <TableRow key={c.name} sx={highlight ? { bgcolor: "rgba(180,35,24,0.08)" } : undefined}>
                          <TableCell sx={{ fontWeight: highlight ? 900 : 500 }}>{c.name}</TableCell>
                          <TableCell>
                            <StatusPill status={c.result === "PASS" ? "OK" : "BROKEN"} />
                          </TableCell>
                          <TableCell>{c.severity ?? "—"}</TableCell>
                          <TableCell>{c.recordsFailing ?? 0}</TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              ) : (
                <Typography variant="body2" color="text.secondary">
                  Click “Run Validation” to detect contract violations.
                </Typography>
              )}
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12}>
          <Card>
            <CardContent>
              <StepHeader
                step={3}
                title="Blame Chain"
                rubricHighlights={["Commit hash + author", "Blast radius ≥ 1", "Lineage shown"]}
                running={runAttribution.isPending}
                done={done3}
                onRun={() => runStep3()}
                runLabel="Run Attribution"
              />
              <Divider sx={{ my: 2 }} />
              {runAttribution.isError ? (
                <Alert severity="error">
                  {String((runAttribution.error as any)?.message ?? runAttribution.error)}
                </Alert>
              ) : null}
              {lineage.length ? (
                <Grid container spacing={2}>
                  <Grid item xs={12} md={7}>
                    <Typography variant="subtitle2" sx={{ fontWeight: 800, mb: 1 }}>
                      Lineage flow
                    </Typography>
                    <Stack direction="row" useFlexGap flexWrap="wrap" gap={1} alignItems="center">
                      {lineage.map((n, idx) => (
                        <React.Fragment key={`${n}-${idx}`}>
                          <Chip label={n} />
                          {idx < lineage.length - 1 ? <Typography color="text.secondary">→</Typography> : null}
                        </React.Fragment>
                      ))}
                    </Stack>
                  </Grid>
                  <Grid item xs={12} md={5}>
                    <Typography variant="subtitle2" sx={{ fontWeight: 800 }}>
                      Attribution
                    </Typography>
                    <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                      Commit: <b>{commitHash ?? "—"}</b>
                      <br />
                      Author: <b>{author ?? "—"}</b>
                    </Typography>
                    <Divider sx={{ my: 1.5 }} />
                    <Typography variant="subtitle2" sx={{ fontWeight: 800, mb: 1 }}>
                      Blast radius
                    </Typography>
                    <Stack direction="row" useFlexGap flexWrap="wrap" gap={1}>
                      {blastRadius.map((b) => (
                        <Chip key={b} label={b} />
                      ))}
                    </Stack>
                  </Grid>
                </Grid>
              ) : (
                <Typography variant="body2" color="text.secondary">
                  Click “Run Attribution” to show blame chain and blast radius.
                </Typography>
              )}
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12}>
          <Card>
            <CardContent>
              <StepHeader
                step={4}
                title="Schema Evolution"
                rubricHighlights={["Breaking change badge", "Classification shown", "Migration report visible"]}
                running={schemaEvolution.isPending}
                done={done4}
                onRun={() => runStep4()}
                runLabel="Analyze Schema Changes"
              />
              <Divider sx={{ my: 2 }} />
              {schemaEvolution.isError ? (
                <Alert severity="error">
                  {String((schemaEvolution.error as any)?.message ?? schemaEvolution.error)}
                </Alert>
              ) : null}
              {breakingChange !== null ? (
                <Grid container spacing={2}>
                  <Grid item xs={12} md={4}>
                    <Alert severity={breakingChange ? "error" : "success"}>
                      Breaking change: <b>{breakingChange ? "YES" : "NO"}</b>
                    </Alert>
                    <Box sx={{ mt: 1 }}>
                      <Chip label={`Classification: ${classification ?? "—"}`} />
                    </Box>
                    {migrationActions.length ? (
                      <Box sx={{ mt: 2 }}>
                        <Typography variant="subtitle2" sx={{ fontWeight: 800, mb: 1 }}>
                          Key actions
                        </Typography>
                        <Stack gap={1}>
                          {migrationActions.slice(0, 5).map((a) => (
                            <Alert key={a} severity="info">
                              {a}
                            </Alert>
                          ))}
                        </Stack>
                      </Box>
                    ) : null}
                  </Grid>
                  <Grid item xs={12} md={8}>
                    <Typography variant="subtitle2" sx={{ fontWeight: 800, mb: 1 }}>
                      Migration impact report (plain English)
                    </Typography>
                    <Typography variant="body1" color="text.secondary">
                      {migrationReport}
                    </Typography>
                  </Grid>
                </Grid>
              ) : (
                <Typography variant="body2" color="text.secondary">
                  Click “Analyze Schema Changes” to produce a breaking-change classification and migration report.
                </Typography>
              )}
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12}>
          <Card>
            <CardContent>
              <StepHeader
                step={5}
                title="AI Extensions"
                rubricHighlights={["Drift score shown", "Prompt validation shown", "Schema violation rate shown"]}
                running={aiExtensions.isPending}
                done={done5}
                onRun={() => runStep5()}
                runLabel="Run AI Analysis"
              />
              <Divider sx={{ my: 2 }} />
              {aiExtensions.isError ? (
                <Alert severity="error">
                  {String((aiExtensions.error as any)?.message ?? aiExtensions.error)}
                </Alert>
              ) : null}

              {embeddingDriftScore !== null ? (
                <Grid container spacing={2}>
                  <Grid item xs={12} md={4}>
                    <MetricGauge
                      value={Math.max(0, Math.min(100, Math.round((embeddingDriftScore ?? 0) * 50)))}
                      label="Drift Meter"
                      subtitle="Higher means closer to (or over) threshold"
                    />
                  </Grid>
                  <Grid item xs={12} md={8}>
                    <Stack direction="row" useFlexGap flexWrap="wrap" gap={1} sx={{ mb: 1.5 }}>
                      <Chip label={`Embedding drift score: ${embeddingDriftScore.toFixed(2)}`} />
                      <Chip
                        label={`Prompt validation: ${promptValidation ?? "—"}`}
                        color={promptValidation === "PASS" ? "success" : promptValidation === "FAIL" ? "error" : "default"}
                        variant={promptValidation === "PASS" || promptValidation === "FAIL" ? "filled" : "outlined"}
                      />
                      <Chip label={`Schema violation rate: ${(((schemaViolationRate ?? 0) * 100).toFixed(2))}%`} />
                    </Stack>
                    {aiExplanation ? (
                      <Alert severity="info">{aiExplanation}</Alert>
                    ) : (
                      <Typography variant="body2" color="text.secondary">
                        No AI explanation returned (set `OPENROUTER_API_KEY` to enable).
                      </Typography>
                    )}
                    {aiActions.length ? (
                      <Box sx={{ mt: 1.5 }}>
                        <Typography variant="subtitle2" sx={{ fontWeight: 800, mb: 1 }}>
                          Recommended actions
                        </Typography>
                        <Stack gap={1}>
                          {aiActions.slice(0, 3).map((a) => (
                            <Alert key={a} severity="success">
                              {a}
                            </Alert>
                          ))}
                        </Stack>
                      </Box>
                    ) : null}
                  </Grid>
                </Grid>
              ) : (
                <Typography variant="body2" color="text.secondary">
                  Click “Run AI Analysis” to compute drift + validation + schema violation rates with plain-language interpretation.
                </Typography>
              )}
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12}>
          <Card>
            <CardContent>
              <StepHeader
                step={6}
                title="Enforcer Report"
                rubricHighlights={["Health score (0–100)", "Top 3 violations in plain English"]}
                running={generateReport.isPending}
                done={done6}
                onRun={() => runStep6()}
                runLabel="Generate Final Report"
              />
              <Divider sx={{ my: 2 }} />
              {generateReport.isError ? (
                <Alert severity="error">
                  {String((generateReport.error as any)?.message ?? generateReport.error)}
                </Alert>
              ) : null}

              {healthScore !== null ? (
                <Grid container spacing={2}>
                  <Grid item xs={12} md={4}>
                    <MetricGauge value={healthScore} label="Data Health Score" subtitle="0–100" />
                  </Grid>
                  <Grid item xs={12} md={8}>
                    {reportNarrative ? <Alert severity="info">{reportNarrative}</Alert> : null}
                    <Typography variant="subtitle2" sx={{ fontWeight: 800, mt: 2, mb: 1 }}>
                      Top violations (plain English)
                    </Typography>
                    <Stack gap={1}>
                      {topViolations.slice(0, 3).map((v) => (
                        <Alert key={v} severity="warning">
                          {v}
                        </Alert>
                      ))}
                    </Stack>
                  </Grid>
                </Grid>
              ) : (
                <Typography variant="body2" color="text.secondary">
                  Click “Generate Final Report” to compute score and translate violations into business risk language.
                </Typography>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </>
  );
}
