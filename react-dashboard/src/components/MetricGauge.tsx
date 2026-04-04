import React from "react";
import { Box, Typography, useTheme } from "@mui/material";

type Props = {
  value: number; // 0–100
  label: string;
  subtitle?: string;
};

function polarToCartesian(cx: number, cy: number, r: number, angleDeg: number) {
  const angleRad = ((angleDeg - 90) * Math.PI) / 180.0;
  return { x: cx + r * Math.cos(angleRad), y: cy + r * Math.sin(angleRad) };
}

function describeArc(cx: number, cy: number, r: number, startAngle: number, endAngle: number) {
  const start = polarToCartesian(cx, cy, r, endAngle);
  const end = polarToCartesian(cx, cy, r, startAngle);
  const largeArcFlag = endAngle - startAngle <= 180 ? "0" : "1";
  return `M ${start.x} ${start.y} A ${r} ${r} 0 ${largeArcFlag} 0 ${end.x} ${end.y}`;
}

/**
 * Lightweight SVG gauge (no charting dependency) for executive “health” and “drift”.
 */
export default function MetricGauge({ value, label, subtitle }: Props) {
  const theme = useTheme();
  const score = Number.isFinite(value) ? Math.max(0, Math.min(100, value)) : 0;
  const start = 210;
  const end = 510;
  const sweep = start + ((end - start) * score) / 100;
  const strokeColor =
    score >= 85 ? theme.palette.success.main : score >= 65 ? theme.palette.warning.main : theme.palette.error.main;

  return (
    <Box sx={{ display: "grid", placeItems: "center" }}>
      <Box sx={{ position: "relative", width: 220, height: 150 }}>
        <svg width="220" height="150" viewBox="0 0 220 150">
          <path
            d={describeArc(110, 120, 90, start, end)}
            fill="none"
            stroke={theme.palette.grey[200]}
            strokeWidth="14"
            strokeLinecap="round"
          />
          <path
            d={describeArc(110, 120, 90, start, sweep)}
            fill="none"
            stroke={strokeColor}
            strokeWidth="14"
            strokeLinecap="round"
          />
        </svg>
        <Box sx={{ position: "absolute", inset: 0, display: "grid", placeItems: "center" }}>
          <Box sx={{ textAlign: "center", mt: 2 }}>
            <Typography variant="h3" sx={{ fontWeight: 800, lineHeight: 1 }}>
              {Math.round(score)}
            </Typography>
            <Typography variant="caption" color="text.secondary">
              / 100
            </Typography>
          </Box>
        </Box>
      </Box>
      <Typography variant="subtitle1" sx={{ fontWeight: 700, mt: 0.5 }}>
        {label}
      </Typography>
      {subtitle ? (
        <Typography variant="body2" color="text.secondary" sx={{ textAlign: "center" }}>
          {subtitle}
        </Typography>
      ) : null}
    </Box>
  );
}
