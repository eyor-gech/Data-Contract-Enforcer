import React from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Card,
  CardContent,
  Divider,
  Grid,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Typography
} from "@mui/material";
import CheckCircleOutlineIcon from "@mui/icons-material/CheckCircleOutline";

import PageShell from "../../components/PageShell";
import AsyncState from "../../components/AsyncState";
import StatusPill from "../../components/StatusPill";
import SchemaDiffViewer from "../../components/SchemaDiffViewer";
import { getSchemaDiff } from "../../api/endpoints";

/**
 * Phase: Change Management View (Phase 3)
 * API:
 * - GET `/api/schema-diff`
 */
export default function SchemaEvolution() {
  const diffQuery = useQuery({ queryKey: ["schema-diff"], queryFn: getSchemaDiff });

  return (
    <>
      <PageShell
        title="Change Management"
        subtitle="A board-ready view of schema changes, compatibility verdict, and the migration checklist."
      />

      <Grid container spacing={2}>
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <AsyncState
                loading={diffQuery.isLoading}
                error={diffQuery.error}
                onRetry={() => diffQuery.refetch()}
                loadingLabel="Loading schema evolution…"
              >
                <Grid container spacing={2} alignItems="center" sx={{ mb: 1 }}>
                  <Grid item xs={12} md={6}>
                    <Typography variant="subtitle1" sx={{ fontWeight: 800 }}>
                      Compatibility verdict
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      SAFE means low risk; DANGEROUS means downstream breakage likely.
                    </Typography>
                  </Grid>
                  <Grid item xs={12} md={6} sx={{ display: "flex", justifyContent: { xs: "flex-start", md: "flex-end" } }}>
                    <StatusPill status={diffQuery.data?.verdict ?? "UNKNOWN"} />
                  </Grid>
                </Grid>

                <Divider sx={{ my: 2 }} />

                <SchemaDiffViewer beforeText={diffQuery.data?.beforeText ?? ""} afterText={diffQuery.data?.afterText ?? ""} />
              </AsyncState>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Typography variant="subtitle1" sx={{ fontWeight: 800 }}>
                Migration checklist
              </Typography>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 1.5 }}>
                Concrete steps to safely deploy the schema change.
              </Typography>

              <AsyncState
                loading={diffQuery.isLoading}
                error={diffQuery.error}
                onRetry={() => diffQuery.refetch()}
              >
                {(diffQuery.data?.checklist ?? []).length === 0 ? (
                  <Typography variant="body2">No checklist items returned by the API.</Typography>
                ) : (
                  <List dense>
                    {diffQuery.data!.checklist.map((c) => (
                      <ListItem key={c}>
                        <ListItemIcon sx={{ minWidth: 32 }}>
                          <CheckCircleOutlineIcon fontSize="small" color="success" />
                        </ListItemIcon>
                        <ListItemText primary={c} />
                      </ListItem>
                    ))}
                  </List>
                )}
              </AsyncState>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </>
  );
}

