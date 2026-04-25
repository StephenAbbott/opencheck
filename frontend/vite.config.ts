import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";
import { cpSync, existsSync, statSync } from "fs";
import { resolve } from "path";

/**
 * Copy bods-dagre's bundled images (flags + entity icons) into
 * ``public/bods-dagre-images`` so they're served at the root URL.
 *
 * The library expects an ``imagesPath`` argument when calling ``draw``.
 * We point it at ``/bods-dagre-images``. The assets total ~3 MB so we
 * keep them out of git and copy them at build start (or whenever the
 * source is newer than the destination).
 */
function copyBodsDagreImages(): Plugin {
  const src = resolve(
    __dirname,
    "node_modules/@openownership/bods-dagre/dist/images"
  );
  const dst = resolve(__dirname, "public/bods-dagre-images");
  return {
    name: "copy-bods-dagre-images",
    buildStart() {
      if (!existsSync(src)) {
        // No npm install yet — fail loudly rather than silently.
        this.warn(`bods-dagre images not found at ${src}`);
        return;
      }
      const dstNewer =
        existsSync(dst) && statSync(dst).mtimeMs >= statSync(src).mtimeMs;
      if (!dstNewer) {
        cpSync(src, dst, { recursive: true });
      }
    },
  };
}

export default defineConfig({
  plugins: [react(), copyBodsDagreImages()],
  server: {
    port: 5173,
    host: true,
  },
});
