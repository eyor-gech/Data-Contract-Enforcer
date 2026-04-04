import React from "react";
import { Alert, Box, Button, CircularProgress, Typography } from "@mui/material";

type Props = {
  loading?: boolean;
  error?: unknown;
  onRetry?: () => void;
  children: React.ReactNode;
  loadingLabel?: string;
};

/**
 * Standardized loading/error wrapper used across all `/api/*` calls.
 * Keeps the experience calm and demo-friendly.
 */
export default function AsyncState({
  loading,
  error,
  onRetry,
  children,
  loadingLabel = "Loading…"
}: Props) {
  if (loading) {
    return (
      <Box sx={{ py: 6, display: "grid", placeItems: "center" }}>
        <CircularProgress />
        <Typography variant="body2" color="text.secondary" sx={{ mt: 1.5 }}>
          {loadingLabel}
        </Typography>
      </Box>
    );
  }

  if (error) {
    return (
      <Box sx={{ py: 3 }}>
        <Alert
          severity="error"
          action={
            onRetry ? (
              <Button color="inherit" size="small" onClick={onRetry}>
                Retry
              </Button>
            ) : undefined
          }
        >
          We couldn’t load this section from the backend API.
        </Alert>
        <Typography variant="caption" color="text.secondary" sx={{ display: "block", mt: 1 }}>
          {String((error as any)?.message ?? error)}
        </Typography>
      </Box>
    );
  }

  return <>{children}</>;
}

