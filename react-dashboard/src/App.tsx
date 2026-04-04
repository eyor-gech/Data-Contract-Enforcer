import React from "react";
import { Navigate, Route, Routes, useLocation, useNavigate } from "react-router-dom";
import {
  AppBar,
  Box,
  Container,
  Tab,
  Tabs,
  Toolbar,
  Typography
} from "@mui/material";

import ExecutiveSummary from "./pages/ExecutiveSummary/ExecutiveSummary";
import DemoFlow from "./pages/DemoFlow/DemoFlow";
import InteractiveDataMap from "./pages/InteractiveDataMap/InteractiveDataMap";
import DetectiveBooth from "./pages/DetectiveBooth/DetectiveBooth";
import SchemaEvolution from "./pages/SchemaEvolution/SchemaEvolution";
import AIQualityLab from "./pages/AIQualityLab/AIQualityLab";

const tabs = [
  { label: "Guided Demo", path: "/demo" },
  { label: "Executive Summary", path: "/executive" },
  { label: "Interactive Data Map", path: "/map" },
  { label: "Detective’s Booth", path: "/detective" },
  { label: "Change Management", path: "/change" },
  { label: "AI Quality Lab", path: "/ai" }
] as const;

export default function App() {
  const location = useLocation();
  const navigate = useNavigate();

  const currentTab = React.useMemo(() => {
    const match = tabs.find((t) => location.pathname.startsWith(t.path));
    return match?.path ?? false;
  }, [location.pathname]);

  return (
    <Box sx={{ minHeight: "100vh", bgcolor: "background.default" }}>
      <AppBar position="sticky" elevation={0} color="transparent">
        <Toolbar sx={{ borderBottom: 1, borderColor: "divider" }}>
          <Box sx={{ flex: 1 }}>
            <Typography variant="h6" sx={{ fontWeight: 700 }}>
              Data Contract Enforcer
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Executive dashboard across Phases 0–4
            </Typography>
          </Box>
          <Tabs
            value={currentTab}
            onChange={(_, next) => navigate(next)}
            textColor="primary"
            indicatorColor="primary"
            sx={{ minHeight: 40 }}
          >
            {tabs.map((t) => (
              <Tab key={t.path} value={t.path} label={t.label} sx={{ minHeight: 40 }} />
            ))}
          </Tabs>
        </Toolbar>
      </AppBar>

      <Container maxWidth="xl" sx={{ py: 3 }}>
        <Routes>
          <Route path="/" element={<Navigate to="/demo" replace />} />
          <Route path="/demo" element={<DemoFlow />} />
          <Route path="/executive" element={<ExecutiveSummary />} />
          <Route path="/map" element={<InteractiveDataMap />} />
          <Route path="/detective" element={<DetectiveBooth />} />
          <Route path="/change" element={<SchemaEvolution />} />
          <Route path="/ai" element={<AIQualityLab />} />
          <Route path="*" element={<Navigate to="/executive" replace />} />
        </Routes>
      </Container>
    </Box>
  );
}
