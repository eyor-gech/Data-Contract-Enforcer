import React from "react";
import { Box, Typography } from "@mui/material";
import ReactDiffViewer from "react-diff-viewer-continued";

type Props = {
  beforeText: string;
  afterText: string;
};

/**
 * Color-coded before/after schema diff for Phase 3 (Change Management).
 * Expects text payloads from `/api/schema-diff` (JSON/YAML are both fine).
 */
export default function SchemaDiffViewer({ beforeText, afterText }: Props) {
  return (
    <Box>
      <Typography variant="subtitle1" sx={{ fontWeight: 800, mb: 1 }}>
        Schema Diff (Before → After)
      </Typography>
      <ReactDiffViewer
        oldValue={beforeText}
        newValue={afterText}
        splitView
        showDiffOnly={false}
        leftTitle="Before"
        rightTitle="After"
        styles={{
          variables: {
            light: {
              diffViewerBackground: "#fff",
              addedBackground: "rgba(26, 127, 55, 0.12)",
              addedGutterBackground: "rgba(26, 127, 55, 0.08)",
              removedBackground: "rgba(180, 35, 24, 0.12)",
              removedGutterBackground: "rgba(180, 35, 24, 0.08)"
            }
          }
        }}
      />
    </Box>
  );
}

