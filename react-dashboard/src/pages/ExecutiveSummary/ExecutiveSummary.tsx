import React from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import {
  Box,
  Button,
  Card,
  CardContent,
  Divider,
  Grid,
  List,
  ListItem,
  ListItemText,
  Typography
} from "@mui/material";
import PictureAsPdfIcon from "@mui/icons-material/PictureAsPdf";
import AutoAwesomeIcon from "@mui/icons-material/AutoAwesome";

import PageShell from "../../components/PageShell";
import AsyncState from "../../components/AsyncState";
import MetricGauge from "../../components/MetricGauge";
import { downloadBlob } from "../../api/download";
import { generateFinalReportPdf, getExecutiveLlmSummary, getHealth } from "../../api/endpoints";

/**
 * Phase: Executive Summary (Health Monitor)
 * APIs:
 * - GET `/api/health`
 * - GET `/api/report/pdf` (optional) to download a final PDF report
 */
export default function ExecutiveSummary() {
  const healthQuery = useQuery({ queryKey: ["health"], queryFn: getHealth });
  const llmMutation = useMutation({ mutationFn: getExecutiveLlmSummary });

  const [pdfLoading, setPdfLoading] = React.useState(false);
  const [pdfError, setPdfError] = React.useState<string | null>(null);

  async function onDownloadPdf() {
    setPdfError(null);
    setPdfLoading(true);
    try {
      const blob = await generateFinalReportPdf();
      downloadBlob(blob, "data-contract-enforcer-report.pdf");
    } catch (e) {
      setPdfError(String((e as any)?.message ?? e));
    } finally {
      setPdfLoading(false);
    }
  }

  return (
    <>
      <PageShell
        title="Executive Summary"
        subtitle="A single-page view of data contract health, business risk, and report generation."
        right={
          <Button
            variant="contained"
            startIcon={<PictureAsPdfIcon />}
            onClick={onDownloadPdf}
            disabled={pdfLoading}
          >
            {pdfLoading ? "Generating…" : "Generate Final PDF"}
          </Button>
        }
      />

      {pdfError ? (
        <Typography color="error" variant="body2" sx={{ mb: 2 }}>
          PDF generation failed: {pdfError}
        </Typography>
      ) : null}

      <Card>
        <CardContent>
          <AsyncState
            loading={healthQuery.isLoading}
            error={healthQuery.error}
            onRetry={() => healthQuery.refetch()}
            loadingLabel="Loading health score…"
          >
            <Grid container spacing={3} alignItems="stretch">
              <Grid item xs={12} md={4}>
                <MetricGauge value={healthQuery.data?.score ?? 0} label="Health Score" subtitle="Overall pipeline contract health" />
              </Grid>

              <Grid item xs={12} md={8}>
                <Typography variant="subtitle1" sx={{ fontWeight: 800 }}>
                  Plain-language summary
                </Typography>
                <Typography variant="body1" color="text.secondary" sx={{ mt: 1 }}>
                  {healthQuery.data?.narrative ??
                    "Summary narrative will appear here once `/api/health` is connected."}
                </Typography>

                <Divider sx={{ my: 2 }} />

                <Typography variant="subtitle1" sx={{ fontWeight: 800 }}>
                  Top business risks (next 7–30 days)
                </Typography>

                <Box sx={{ mt: 1 }}>
                  {healthQuery.data?.topRisks?.length ? (
                    <List dense>
                      {healthQuery.data.topRisks.map((r) => (
                        <ListItem key={r} disablePadding sx={{ py: 0.25 }}>
                          <ListItemText primary={r} />
                        </ListItem>
                      ))}
                    </List>
                  ) : (
                    <Typography variant="body2" color="text.secondary">
                      No risk items returned by the API.
                    </Typography>
                  )}
                </Box>
              </Grid>
            </Grid>
          </AsyncState>
        </CardContent>
      </Card>

      <Card sx={{ mt: 2 }}>
        <CardContent>
          <Box sx={{ display: "flex", alignItems: "flex-start", gap: 2 }}>
            <Box sx={{ flex: 1 }}>
              <Typography variant="subtitle1" sx={{ fontWeight: 800 }}>
                AI Executive Brief (OpenRouter)
              </Typography>
              <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                Generates a board-ready summary from the current enforcement evidence. Requires `OPENROUTER_API_KEY` in `adapter_api/.env`.
              </Typography>
            </Box>
            <Button
              variant="outlined"
              startIcon={<AutoAwesomeIcon />}
              onClick={() => llmMutation.mutate()}
              disabled={llmMutation.isPending}
            >
              {llmMutation.isPending ? "Generating…" : "Generate Brief"}
            </Button>
          </Box>

          {llmMutation.isError ? (
            <Typography color="error" variant="body2" sx={{ mt: 1.5 }}>
              {String((llmMutation.error as any)?.response?.data?.detail ?? (llmMutation.error as any)?.message ?? llmMutation.error)}
            </Typography>
          ) : null}

          {llmMutation.data ? (
            <>
              <Divider sx={{ my: 2 }} />
              <Typography variant="subtitle2" sx={{ fontWeight: 800 }}>
                Narrative
              </Typography>
              <Typography variant="body1" color="text.secondary" sx={{ mt: 0.75 }}>
                {llmMutation.data.narrative || "No narrative returned."}
              </Typography>

              <Grid container spacing={2} sx={{ mt: 1 }}>
                <Grid item xs={12} md={6}>
                  <Typography variant="subtitle2" sx={{ fontWeight: 800, mb: 0.5 }}>
                    Top risks
                  </Typography>
                  {llmMutation.data.risks.length ? (
                    <List dense>
                      {llmMutation.data.risks.slice(0, 3).map((r) => (
                        <ListItem key={r} disablePadding sx={{ py: 0.25 }}>
                          <ListItemText primary={r} />
                        </ListItem>
                      ))}
                    </List>
                  ) : (
                    <Typography variant="body2" color="text.secondary">
                      No risks returned.
                    </Typography>
                  )}
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="subtitle2" sx={{ fontWeight: 800, mb: 0.5 }}>
                    Recommended actions
                  </Typography>
                  {llmMutation.data.actions.length ? (
                    <List dense>
                      {llmMutation.data.actions.slice(0, 3).map((a) => (
                        <ListItem key={a} disablePadding sx={{ py: 0.25 }}>
                          <ListItemText primary={a} />
                        </ListItem>
                      ))}
                    </List>
                  ) : (
                    <Typography variant="body2" color="text.secondary">
                      No actions returned.
                    </Typography>
                  )}
                </Grid>
              </Grid>

              <Typography variant="caption" color="text.secondary" sx={{ display: "block", mt: 1.5 }}>
                {llmMutation.data.model ? `Model: ${llmMutation.data.model}. ` : ""}
                {llmMutation.data.generatedAt ? `Generated: ${llmMutation.data.generatedAt}.` : ""}
              </Typography>
            </>
          ) : null}
        </CardContent>
      </Card>
    </>
  );
}
