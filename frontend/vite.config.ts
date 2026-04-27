import { defineConfig, type Plugin } from "vite";
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

export default defineConfig({
  plugins: [react(), copyBodsDagreAssets()],
  server: {
    port: 5173,
    host: true,
  },
});
