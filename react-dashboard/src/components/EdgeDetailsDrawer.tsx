import React from "react";
import {
  Box,
  Divider,
  Drawer,
  IconButton,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Typography
} from "@mui/material";
import CloseIcon from "@mui/icons-material/Close";
import CheckCircleOutlineIcon from "@mui/icons-material/CheckCircleOutline";

import StatusPill from "./StatusPill";
import type { NormalizedContractMap } from "../api/types";

type Edge = NormalizedContractMap["edges"][number];

type Props = {
  open: boolean;
  edge?: Edge;
  onClose: () => void;
};

/**
 * Shows contract “promises” (human-readable clauses) for a clicked data flow edge.
 */
export default function EdgeDetailsDrawer({ open, edge, onClose }: Props) {
  return (
    <Drawer anchor="right" open={open} onClose={onClose}>
      <Box sx={{ width: 420, p: 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
          <Typography variant="h6" sx={{ fontWeight: 800, flex: 1 }}>
            Contract Promise
          </Typography>
          <IconButton onClick={onClose}>
            <CloseIcon />
          </IconButton>
        </Box>

        {edge ? (
          <>
            <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
              {edge.source} → {edge.target}
            </Typography>
            <Box sx={{ mt: 1 }}>
              <StatusPill status={edge.status} />
            </Box>
            <Divider sx={{ my: 2 }} />

            <Typography variant="subtitle2" sx={{ fontWeight: 800, mb: 1 }}>
              Promises (plain language)
            </Typography>
            {edge.promises.length === 0 ? (
              <Typography variant="body2">
                No promise text provided by the API for this edge.
              </Typography>
            ) : (
              <List dense>
                {edge.promises.map((p, idx) => (
                  <ListItem key={`${idx}-${p}`}>
                    <ListItemIcon sx={{ minWidth: 32 }}>
                      <CheckCircleOutlineIcon fontSize="small" color="success" />
                    </ListItemIcon>
                    <ListItemText primary={p} />
                  </ListItem>
                ))}
              </List>
            )}
          </>
        ) : (
          <Typography variant="body2" sx={{ mt: 2 }}>
            Select an arrow in the map to view its contract promise.
          </Typography>
        )}
      </Box>
    </Drawer>
  );
}

