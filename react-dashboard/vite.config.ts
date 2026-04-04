import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

/**
 * Vite dev proxy to avoid CORS while developing locally.
 * - Set `VITE_API_PROXY_TARGET=http://localhost:8000` (or your backend host).
 * - In production, serve the UI behind the same origin as the backend (recommended),
 *   so requests to `/api/*` go straight through.
 */
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const proxyTarget = env.VITE_API_PROXY_TARGET || "http://localhost:8000";

  return {
    plugins: [react()],
    server: {
      port: 5173,
      strictPort: true,
      proxy: {
        "/api": {
          target: proxyTarget,
          changeOrigin: true
        }
      }
    }
  };
});

