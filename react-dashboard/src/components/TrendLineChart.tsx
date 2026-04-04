import React from "react";
import { Box, Typography } from "@mui/material";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import type { NormalizedTrendPoint } from "../api/types";

type Props = {
  title: string;
  subtitle?: string;
  data: NormalizedTrendPoint[];
};

export default function TrendLineChart({ title, subtitle, data }: Props) {
  return (
    <Box sx={{ width: "100%" }}>
      <Typography variant="subtitle1" sx={{ fontWeight: 800 }}>
        {title}
      </Typography>
      {subtitle ? (
        <Typography variant="body2" color="text.secondary" sx={{ mb: 1.5 }}>
          {subtitle}
        </Typography>
      ) : (
        <Box sx={{ height: 8 }} />
      )}
      <Box sx={{ width: "100%", height: 260 }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="date" tick={{ fontSize: 12 }} />
            <YAxis tick={{ fontSize: 12 }} allowDecimals={false} />
            <Tooltip />
            <Line type="monotone" dataKey="count" stroke="#1F5EFF" strokeWidth={3} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </Box>
    </Box>
  );
}

