import React from "react";
import { useQuery } from "@tanstack/react-query";
import dagre from "dagre";
import ReactFlow, {
  Background,
  Controls,
  Edge,
  MarkerType,
  Node,
  OnEdgesChange,
  OnNodesChange,
  applyEdgeChanges,
  applyNodeChanges
} from "reactflow";
import { Box, Card, CardContent, Grid, Typography } from "@mui/material";

import PageShell from "../../components/PageShell";
import AsyncState from "../../components/AsyncState";
import EdgeDetailsDrawer from "../../components/EdgeDetailsDrawer";
import { getContractStatus } from "../../api/endpoints";
import type { NormalizedContractMap } from "../../api/types";

const nodeWidth = 220;
const nodeHeight = 58;

function layoutGraph(nodes: Node[], edges: Edge[]) {
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: "LR", nodesep: 40, ranksep: 80 });

  nodes.forEach((n) => g.setNode(n.id, { width: nodeWidth, height: nodeHeight }));
  edges.forEach((e) => g.setEdge(e.source, e.target));

  dagre.layout(g);

  const out = nodes.map((n) => {
    const pos = g.node(n.id);
    return {
      ...n,
      position: { x: pos.x - nodeWidth / 2, y: pos.y - nodeHeight / 2 }
    };
  });

  return out;
}

function edgeStroke(status: NormalizedContractMap["edges"][number]["status"]) {
  switch (status) {
    case "OK":
      return "#1A7F37";
    case "BROKEN":
      return "#B42318";
    default:
      return "#667085";
  }
}

/**
 * Phase: Interactive Data Map (Phase 0 & 1)
 * API:
 * - GET `/api/contract-status`
 */
export default function InteractiveDataMap() {
  const mapQuery = useQuery({ queryKey: ["contract-status"], queryFn: getContractStatus });

  const [nodes, setNodes] = React.useState<Node[]>([]);
  const [edges, setEdges] = React.useState<Edge[]>([]);
  const [selectedEdgeId, setSelectedEdgeId] = React.useState<string | null>(null);

  React.useEffect(() => {
    if (!mapQuery.data) return;

    const nextNodes: Node[] = mapQuery.data.nodes.map((n) => ({
      id: n.id,
      type: "default",
      data: {
        label: (
          <Box sx={{ px: 1.5, py: 1 }}>
            <Typography variant="subtitle2" sx={{ fontWeight: 800 }}>
              {n.label}
            </Typography>
            {n.group ? (
              <Typography variant="caption" color="text.secondary">
                {n.group}
              </Typography>
            ) : null}
          </Box>
        )
      },
      position: { x: 0, y: 0 },
      style: {
        borderRadius: 14,
        border: "1px solid rgba(0,0,0,0.08)",
        background: "white",
        width: nodeWidth,
        height: nodeHeight
      }
    }));

    const nextEdges: Edge[] = mapQuery.data.edges.map((e) => ({
      id: e.id,
      source: e.source,
      target: e.target,
      animated: e.status === "BROKEN",
      style: { stroke: edgeStroke(e.status), strokeWidth: 3 },
      markerEnd: { type: MarkerType.ArrowClosed, width: 18, height: 18, color: edgeStroke(e.status) },
      data: { raw: e.raw }
    }));

    setNodes(layoutGraph(nextNodes, nextEdges));
    setEdges(nextEdges);
  }, [mapQuery.data]);

  const onNodesChange: OnNodesChange = React.useCallback(
    (changes) => setNodes((nds) => applyNodeChanges(changes, nds)),
    []
  );

  const onEdgesChange: OnEdgesChange = React.useCallback(
    (changes) => setEdges((eds) => applyEdgeChanges(changes, eds)),
    []
  );

  const selected = React.useMemo(() => {
    if (!selectedEdgeId) return undefined;
    return mapQuery.data?.edges.find((e) => e.id === selectedEdgeId);
  }, [mapQuery.data, selectedEdgeId]);

  return (
    <>
      <PageShell
        title="Interactive Data Map"
        subtitle="A live view of how data moves across systems, with contracts shown as green (kept) or red (broken) promises."
      />

      <Grid container spacing={2}>
        <Grid item xs={12} lg={9}>
          <Card>
            <CardContent>
              <AsyncState
                loading={mapQuery.isLoading}
                error={mapQuery.error}
                onRetry={() => mapQuery.refetch()}
                loadingLabel="Loading data map…"
              >
                <Box sx={{ height: 560, borderRadius: 2, overflow: "hidden" }}>
                  <ReactFlow
                    nodes={nodes}
                    edges={edges}
                    onNodesChange={onNodesChange}
                    onEdgesChange={onEdgesChange}
                    onEdgeClick={(_, edge) => setSelectedEdgeId(edge.id)}
                    fitView
                  >
                    <Background />
                    <Controls />
                  </ReactFlow>
                </Box>
              </AsyncState>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} lg={3}>
          <Card>
            <CardContent>
              <Typography variant="subtitle1" sx={{ fontWeight: 800 }}>
                Legend
              </Typography>
              <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                Click an arrow to see the contract promise in plain language.
              </Typography>
              <Box sx={{ mt: 2, display: "grid", gap: 1 }}>
                <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                  <Box sx={{ width: 14, height: 14, borderRadius: 99, bgcolor: "#1A7F37" }} />
                  <Typography variant="body2">Promise kept (green)</Typography>
                </Box>
                <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                  <Box sx={{ width: 14, height: 14, borderRadius: 99, bgcolor: "#B42318" }} />
                  <Typography variant="body2">Promise broken (red)</Typography>
                </Box>
                <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                  <Box sx={{ width: 14, height: 14, borderRadius: 99, bgcolor: "#667085" }} />
                  <Typography variant="body2">Unknown / not reported</Typography>
                </Box>
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      <EdgeDetailsDrawer
        open={Boolean(selectedEdgeId)}
        edge={selected}
        onClose={() => setSelectedEdgeId(null)}
      />
    </>
  );
}

