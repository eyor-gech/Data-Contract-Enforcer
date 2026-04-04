import React from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, Grid, Typography } from "@mui/material";

import PageShell from "../../components/PageShell";
import AsyncState from "../../components/AsyncState";
import MetricGauge from "../../components/MetricGauge";
import TrendLineChart from "../../components/TrendLineChart";
import { getAiDrift, getLlmViolations } from "../../api/endpoints";

/**
 * Phase: AI Quality Lab (Phase 4)
 * APIs:
 * - GET `/api/ai-drift`
 * - GET `/api/llm-violations`
 */
export default function AIQualityLab() {
  const driftQuery = useQuery({ queryKey: ["ai-drift"], queryFn: getAiDrift });
  const trendQuery = useQuery({ queryKey: ["llm-violations"], queryFn: getLlmViolations });

  return (
    <>
      <PageShell
        title="AI Quality Lab"
        subtitle="A simple view of model drift risk and trend signals from LLM output contract violations."
      />

      <Grid container spacing={2}>
        <Grid item xs={12} lg={4}>
          <Card sx={{ height: "100%" }}>
            <CardContent>
              <AsyncState
                loading={driftQuery.isLoading}
                error={driftQuery.error}
                onRetry={() => driftQuery.refetch()}
                loadingLabel="Loading AI drift…"
              >
                <MetricGauge
                  value={driftQuery.data?.score ?? 0}
                  label="Drift Meter"
                  subtitle={driftQuery.data?.narrative ?? "Higher means greater risk of model quality drift."}
                />
              </AsyncState>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} lg={8}>
          <Card sx={{ height: "100%" }}>
            <CardContent>
              <AsyncState
                loading={trendQuery.isLoading}
                error={trendQuery.error}
                onRetry={() => trendQuery.refetch()}
                loadingLabel="Loading LLM violation trend…"
              >
                <TrendLineChart
                  title="LLM Output Contract Violations"
                  subtitle="Trend signal for quality regressions and schema/policy drift."
                  data={trendQuery.data ?? []}
                />
              </AsyncState>
              {(trendQuery.data ?? []).length === 0 ? (
                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                  No trend points returned by the API.
                </Typography>
              ) : null}
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </>
  );
}

