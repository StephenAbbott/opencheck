import { defineConfig, loadEnv, type Plugin } from "vite";
import react from "@vitejs/plugin-react";
import { copyFileSync, cpSync, existsSync, statSync } from "fs";
import { resolve } from "path";

/**
 * Copy bods-dagre's bundled assets into ``public/`` so they're served
 * at the root URL.
 *
 * Two assets are copied:
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

export default defineConfig(({ mode }) => {
  // ``VITE_API_BASE_URL`` doubles as the server-side proxy target so that
  // the browser can call relative paths like ``/lookup`` regardless of
  // which device or network it's on.  Precedence:
  //   1. OS env var (set by docker-compose or CI)
  //   2. .env.local / .env file via loadEnv
  //   3. Fallback to local default
  const fileEnv = loadEnv(mode, process.cwd(), "");
  const backendTarget =
    process.env.VITE_API_BASE_URL ??
    fileEnv.VITE_API_BASE_URL ??
    "http://localhost:8000";

  // All backend routes that the browser must be able to reach.
  // Vite proxies each one server-side so the browser always uses a
  // same-origin relative URL and never touches the backend port directly.
  const proxyTarget = { target: backendTarget, changeOrigin: true };
  const proxy: Record<string, typeof proxyTarget> = Object.fromEntries(
    ["/lookup", "/sources", "/search", "/deepen", "/export", "/health", "/stream"].map(
      (p) => [p, proxyTarget]
    )
  );

  return {
    plugins: [react(), copyBodsDagreAssets()],
    server: {
      port: 5173,
      host: true,
      proxy,
    },
  };
});
