import React from "react";
import { Alert, Box, Typography } from "@mui/material";

type Props = {
  breaking: boolean;
  text?: string;
};

/**
 * Phase 2 “Detective’s Booth” headline: whether a breaking change was detected.
 */
export default function BreakingChangeBanner({ breaking, text }: Props) {
  if (!breaking) {
    return (
      <Alert severity="success">
        No breaking change detected. Data contracts appear compatible across the pipeline.
      </Alert>
    );
  }

  return (
    <Alert severity="error">
      <Box>
        <Typography sx={{ fontWeight: 800 }}>Breaking change detected</Typography>
        <Typography variant="body2">
          {text ??
            "Downstream systems are at risk. Review the timeline and blast radius below."}
        </Typography>
      </Box>
    </Alert>
  );
}

