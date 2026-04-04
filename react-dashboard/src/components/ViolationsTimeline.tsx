import React from "react";
import {
  Timeline,
  TimelineConnector,
  TimelineContent,
  TimelineDot,
  TimelineItem,
  TimelineOppositeContent,
  TimelineSeparator
} from "@mui/lab";
import { Box, Chip, Typography } from "@mui/material";
import type { NormalizedViolation } from "../api/types";

function dotColor(severity: NormalizedViolation["severity"]) {
  switch (severity) {
    case "CRITICAL":
    case "HIGH":
      return "#B42318";
    case "MEDIUM":
      return "#B54708";
    case "LOW":
      return "#1A7F37";
    default:
      return "#667085";
  }
}

/**
 * Phase 2 timeline visualization for `/api/blame-chain`.
 * Focuses on “what happened and when” in non-technical language.
 */
export default function ViolationsTimeline({ items }: { items: NormalizedViolation[] }) {
  if (items.length === 0) {
    return <Typography variant="body2">No violations to display.</Typography>;
  }

  return (
    <Timeline position="right" sx={{ p: 0, m: 0 }}>
      {items.map((v, idx) => (
        <TimelineItem key={v.id}>
          <TimelineOppositeContent sx={{ flex: 0.22, pr: 2, pt: 1 }}>
            <Typography variant="caption" color="text.secondary">
              {v.timestamp ?? "—"}
            </Typography>
          </TimelineOppositeContent>
          <TimelineSeparator>
            <TimelineDot sx={{ bgcolor: dotColor(v.severity) }} />
            {idx < items.length - 1 ? <TimelineConnector /> : null}
          </TimelineSeparator>
          <TimelineContent sx={{ pt: 0.7, pb: 2 }}>
            <Box sx={{ display: "flex", gap: 1, alignItems: "center", flexWrap: "wrap" }}>
              {v.system ? <Chip size="small" label={v.system} variant="outlined" /> : null}
              <Chip
                size="small"
                label={v.severity}
                sx={{
                  bgcolor: `${dotColor(v.severity)}20`,
                  color: dotColor(v.severity),
                  borderColor: `${dotColor(v.severity)}55`
                }}
                variant="outlined"
              />
            </Box>
            <Typography sx={{ mt: 0.5 }}>{v.message}</Typography>
            {v.downstream.length > 0 ? (
              <Typography variant="body2" color="text.secondary" sx={{ mt: 0.25 }}>
                Downstream: {v.downstream.join(", ")}
              </Typography>
            ) : null}
          </TimelineContent>
        </TimelineItem>
      ))}
    </Timeline>
  );
}
