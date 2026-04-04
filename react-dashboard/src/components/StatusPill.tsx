import React from "react";
import { Chip } from "@mui/material";

export type StatusPillVariant = "OK" | "BROKEN" | "UNKNOWN" | "SAFE" | "DANGEROUS";

const map = (status: StatusPillVariant) => {
  switch (status) {
    case "OK":
    case "SAFE":
      return { label: status, color: "success" as const };
    case "BROKEN":
    case "DANGEROUS":
      return { label: status, color: "error" as const };
    default:
      return { label: status, color: "default" as const };
  }
};

export default function StatusPill({ status }: { status: StatusPillVariant }) {
  const m = map(status);
  return <Chip size="small" label={m.label} color={m.color} variant={m.color === "default" ? "outlined" : "filled"} />;
}

