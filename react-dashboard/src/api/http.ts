import axios from "axios";

/**
 * Axios client for the existing Python backend.
 *
 * Important: keep `baseURL` empty so `/api/*` hits the same origin in production.
 * In dev, `vite.config.ts` proxies `/api` to `VITE_API_PROXY_TARGET`.
 */
export const http = axios.create({
  baseURL: "",
  headers: {
    Accept: "application/json"
  }
});

