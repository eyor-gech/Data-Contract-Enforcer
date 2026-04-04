import React from "react";
import { ThemeProvider, createTheme } from "@mui/material/styles";

/**
 * Central theme to keep the UI consistent and executive-friendly.
 * Uses subdued surfaces, strong typography, and clear status colors.
 */
const theme = createTheme({
  palette: {
    mode: "light",
    primary: { main: "#1F5EFF" },
    success: { main: "#1A7F37" },
    warning: { main: "#B54708" },
    error: { main: "#B42318" },
    background: { default: "#F7F8FA", paper: "#FFFFFF" }
  },
  shape: {
    borderRadius: 14
  },
  typography: {
    fontFamily: [
      "Inter",
      "system-ui",
      "-apple-system",
      "Segoe UI",
      "Roboto",
      "Arial",
      "sans-serif"
    ].join(",")
  },
  components: {
    MuiCard: {
      styleOverrides: {
        root: {
          border: "1px solid rgba(0,0,0,0.06)"
        }
      }
    }
  }
});

export function AppThemeProvider({ children }: { children: React.ReactNode }) {
  return <ThemeProvider theme={theme}>{children}</ThemeProvider>;
}

