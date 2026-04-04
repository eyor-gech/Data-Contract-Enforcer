import React from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, Grid, Typography } from "@mui/material";

import PageShell from "../../components/PageShell";
import AsyncState from "../../components/AsyncState";
import BreakingChangeBanner from "../../components/BreakingChangeBanner";
import ViolationsTimeline from "../../components/ViolationsTimeline";
import BlastRadiusPanel from "../../components/BlastRadiusPanel";
import { getBlameChain } from "../../api/endpoints";

/**
 * Phase: Detective’s Booth (Phase 2)
 * API:
 * - GET `/api/blame-chain`
 */
export default function DetectiveBooth() {
  const blameQuery = useQuery({ queryKey: ["blame-chain"], queryFn: getBlameChain });

  return (
    <>
      <PageShell
        title="Detective’s Booth"
        subtitle="When something breaks, this view explains what happened, where it started, and who is impacted."
      />

      <Card sx={{ mb: 2 }}>
        <CardContent>
          <AsyncState
            loading={blameQuery.isLoading}
            error={blameQuery.error}
            onRetry={() => blameQuery.refetch()}
            loadingLabel="Loading blame chain…"
          >
            <BreakingChangeBanner
              breaking={blameQuery.data?.breakingChange ?? false}
              text={blameQuery.data?.alertText}
            />
          </AsyncState>
        </CardContent>
      </Card>

      <Grid container spacing={2}>
        <Grid item xs={12} lg={7}>
          <Card>
            <CardContent>
              <Typography variant="subtitle1" sx={{ fontWeight: 800, mb: 1 }}>
                Violation Timeline
              </Typography>
              <AsyncState
                loading={blameQuery.isLoading}
                error={blameQuery.error}
                onRetry={() => blameQuery.refetch()}
              >
                <ViolationsTimeline items={blameQuery.data?.violations ?? []} />
              </AsyncState>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} lg={5}>
          <AsyncState
            loading={blameQuery.isLoading}
            error={blameQuery.error}
            onRetry={() => blameQuery.refetch()}
          >
            <BlastRadiusPanel
              source={blameQuery.data?.blastRadius.source}
              affected={blameQuery.data?.blastRadius.affected ?? []}
            />
          </AsyncState>
        </Grid>
      </Grid>
    </>
  );
}

