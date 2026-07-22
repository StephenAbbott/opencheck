import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { readFileSync } from "fs";
import { resolve } from "path";

/**
 * Read a simple KEY=VALUE env file and return the values as a plain
 * object. Used so the proxy target can be derived from ``.env.local``
 * without relying on Vite's internal ``loadEnv`` timing, which differs
 * between Vite versions when called inside a functional ``defineConfig``.
 */
function readDotEnv(filePath: string): Record<string, string> {
  try {
    const lines = readFileSync(filePath, "utf-8").split("\n");
    const out: Record<string, string> = {};
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;
      const eq = trimmed.indexOf("=");
      if (eq === -1) continue;
      out[trimmed.slice(0, eq).trim()] = trimmed.slice(eq + 1).trim();
    }
    return out;
  } catch {
    return {};
  }
}

// Proxy target resolution — precedence:
//   1. OS / docker-compose env var (VITE_API_BASE_URL)
//   2. .env.local in this directory
//   3. Hard fallback
const localEnv = readDotEnv(resolve(__dirname, ".env.local"));
const backendTarget: string =
  process.env.VITE_API_BASE_URL ??
  localEnv.VITE_API_BASE_URL ??
  "http://localhost:8000";

// One entry per backend path prefix. Vite matches by prefix, so "/lookup" also
// covers /lookup-stream and /lookup-source, "/expand" covers /expand-layer, and
// "/export" covers /export/pdf. Everything the frontend's API client fetches
// must appear here or the dev server serves index.html (→ "Unexpected token '<'").
const proxyRoutes: Record<string, { target: string; changeOrigin: boolean }> = {
  "/lookup":          { target: backendTarget, changeOrigin: true },
  "/sources":         { target: backendTarget, changeOrigin: true },
  "/search":          { target: backendTarget, changeOrigin: true },
  "/deepen":          { target: backendTarget, changeOrigin: true },
  "/expand":          { target: backendTarget, changeOrigin: true },
  "/export":          { target: backendTarget, changeOrigin: true },
  "/health":          { target: backendTarget, changeOrigin: true },
  "/stream":          { target: backendTarget, changeOrigin: true },
  "/subsidiaries":    { target: backendTarget, changeOrigin: true },
  "/history":         { target: backendTarget, changeOrigin: true },
  "/securities":      { target: backendTarget, changeOrigin: true },
  "/nz-associations": { target: backendTarget, changeOrigin: true },
  "/person-check":    { target: backendTarget, changeOrigin: true },
  "/narrative":       { target: backendTarget, changeOrigin: true },
  "/license-matrix":  { target: backendTarget, changeOrigin: true },
};

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    host: true,
    proxy: proxyRoutes,
  },
});
