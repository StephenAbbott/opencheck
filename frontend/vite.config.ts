import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";
import { copyFileSync, cpSync, existsSync, readFileSync, statSync } from "fs";
import { resolve } from "path";

/**
 * Copy bods-dagre's bundled assets into ``public/`` so they're served
 * at the root URL.
 *
 * Two assets are copies:
 *
 * 1. ``public/bods-dagre.js`` — the library bundle itself. The library
 *    is published as a UMD bundle that assigns to ``var BODSDagre``,
 *    but its package.json declares ``"type": "module"``. When Vite
 *    imports it as ESM the ``var`` is module-scoped and never reaches
 *    ``window``. We side-step that by serving the file as a static
 *    asset and loading it via a classic ``<script>`` tag in
 *    ``index.html`` — which evaluates as a non-module script and lets
 *    ``var BODSDagre`` land on the global scope as the library
 *    expects.
 * 2. ``public/bods-dagre-images/`` — the bundled flag + entity icons.
 *    The library expects an ``imagesPath`` argument when calling
 *    ``draw``; we point it at ``/bods-dagre-images``. The asset folder
 *    totals ~3 MB so we keep it out of git and copy it at build start.
 */
function copyBodsDagreAssets(): Plugin {
  const dist = resolve(__dirname, "node_modules/@openownership/bods-dagre/dist");
  const jsSrc = resolve(dist, "bods-dagre.js");
  const jsDst = resolve(__dirname, "public/bods-dagre.js");
  const imgSrc = resolve(dist, "images");
  const imgDst = resolve(__dirname, "public/bods-dagre-images");
  return {
    name: "copy-bods-dagre-assets",
    buildStart() {
      if (!existsSync(dist)) {
        this.warn(`bods-dagre dist not found at ${dist}`);
        return;
      }
      // Library bundle.
      if (existsSync(jsSrc)) {
        const jsDstNewer =
          existsSync(jsDst) &&
          statSync(jsDst).mtimeMs >= statSync(jsSrc).mtimeMs;
        if (!jsDstNewer) {
          copyFileSync(jsSrc, jsDst);
        }
      }
      // Image directory.
      if (existsSync(imgSrc)) {
        const imgDstNewer =
          existsSync(imgDst) &&
          statSync(imgDst).mtimeMs >= statSync(imgSrc).mtimeMs;
        if (!imgDstNewer) {
          cpSync(imgSrc, imgDst, { recursive: true });
        }
      }
    },
  };
}

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
// Read from .env.local explicitly so the value is available at config
// load time regardless of Vite version or defineConfig form used.
const localEnv = readDotEnv(resolve(__dirname, ".env.local"));
const backendTarget: string =
  process.env.VITE_API_BASE_URL ??
  localEnv.VITE_API_BASE_URL ??
  "http://localhost:8000";

// Route every backend path through the dev server so the browser always
// uses a same-origin relative URL.  This fixes "Load failed" when
// testing from a phone or any non-localhost device: the browser calls
// /lookup (same host as the page), Vite forwards server-side.
const proxyRoutes: Record<string, { target: string; changeOrigin: boolean }> = {
  "/lookup":  { target: backendTarget, changeOrigin: true },
  "/sources": { target: backendTarget, changeOrigin: true },
  "/search":  { target: backendTarget, changeOrigin: true },
  "/deepen":  { target: backendTarget, changeOrigin: true },
  "/export":  { target: backendTarget, changeOrigin: true },
  "/health":  { target: backendTarget, changeOrigin: true },
  "/stream":  { target: backendTarget, changeOrigin: true },
};

export default defineConfig({
  plugins: [react(), copyBodsDagreAssets()],
  server: {
    port: 5173,
    host: true,
    proxy: proxyRoutes,
  },
});
