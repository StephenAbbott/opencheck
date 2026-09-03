// `vitest/config` rather than `vite`: it is Vite's own defineConfig
// widened with the `test` block below, so one file configures the dev server
// and the suite (Phase 168).
import { defineConfig } from "vitest/config";
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
  "/source-health":   { target: backendTarget, changeOrigin: true },
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
  "/person-":         { target: backendTarget, changeOrigin: true },  // /person-check + /person-appointments
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
  preview: {
    port: 4173,
    host: true,
    // Vite defaults `preview.proxy` to `server.proxy`, and inheriting it here
    // makes `vite preview` the one thing it is not supposed to be: unlike
    // production. The deployed SPA is a static site that talks to the API by
    // absolute URL (VITE_API_BASE_URL), with no proxy in front — and with the
    // dev proxy in place, `/sources` is answered by the backend's JSON
    // inventory, so the SPA route of that name cannot be reached at all.
    // Empty, not inherited (Phase 168).
    proxy: {},
  },
  test: {
    // Two environments on purpose (Phase 168). The `lib/` suite is logic-only
    // and runs in `node`, where it is fast and cannot accidentally depend on a
    // DOM; component tests get `jsdom`, which costs ~1s of setup per file and
    // is worth it only where a claim lives in the markup. The glob is the
    // whole rule: name a test `.test.tsx` and you get a DOM, name it
    // `.test.ts` and you do not.
    environmentMatchGlobs: [["**/*.test.tsx", "jsdom"]],
    setupFiles: ["./src/test/setup.ts"],
    // e2e/ is Playwright's; vitest must not try to run those specs.
    exclude: ["e2e/**", "node_modules/**", "dist/**"],
  },
});
