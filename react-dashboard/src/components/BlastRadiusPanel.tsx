import React from "react";
import { Box, Card, CardContent, Chip, Stack, Typography } from "@mui/material";

type Props = {
  source?: string;
  affected: string[];
};

/**
 * Executive-friendly “blast radius” view: highlights which downstream systems are impacted.
 */
export default function BlastRadiusPanel({ source, affected }: Props) {
  return (
    <Card>
      <CardContent>
        <Typography variant="subtitle1" sx={{ fontWeight: 800, mb: 0.5 }}>
          Blast Radius
        </Typography>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 1.5 }}>
          {source
            ? `Impact originates from ${source}.`
            : "Impact originates upstream in the pipeline."}
        </Typography>
        {affected.length === 0 ? (
          <Typography variant="body2">No downstream systems reported as affected.</Typography>
        ) : (
          <Stack direction="row" useFlexGap flexWrap="wrap" gap={1}>
            {affected.map((a) => (
              <Chip key={a} label={a} />
            ))}
          </Stack>
        )}
      </CardContent>
    </Card>
  );
}

