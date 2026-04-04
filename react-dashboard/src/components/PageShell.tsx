import React from "react";
import { Box, Typography } from "@mui/material";

type Props = {
  title: string;
  subtitle?: string;
  right?: React.ReactNode;
};

/**
 * Consistent page header for an executive narrative.
 * Use `subtitle` for a plain-language “what this means” line.
 */
export default function PageShell({ title, subtitle, right }: Props) {
  return (
    <Box sx={{ mb: 2.5 }}>
      <Box sx={{ display: "flex", gap: 2, alignItems: "flex-start" }}>
        <Box sx={{ flex: 1 }}>
          <Typography variant="h4" sx={{ fontWeight: 800, lineHeight: 1.1 }}>
            {title}
          </Typography>
          {subtitle ? (
            <Typography variant="body1" color="text.secondary" sx={{ mt: 0.75 }}>
              {subtitle}
            </Typography>
          ) : null}
        </Box>
        {right ? <Box sx={{ pt: 0.5 }}>{right}</Box> : null}
      </Box>
    </Box>
  );
}
