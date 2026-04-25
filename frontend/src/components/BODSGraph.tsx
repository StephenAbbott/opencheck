import { useEffect, useRef } from "react";
import { draw } from "@openownership/bods-dagre";

/**
 * Renders a BODS v0.4 statement bundle as a directed ownership graph
 * using @openownership/bods-dagre.
 *
 * The library mutates a target ``<div>`` directly. We give it a fresh
 * container per render so React's reconciliation never fights with the
 * SVG dagre injects.
 */
export default function BODSGraph({
  statements,
}: {
  statements: unknown[];
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    // Clear any previous render before re-drawing.
    el.innerHTML = "";
    if (statements.length === 0) return;

    try {
      // The third arg is the URL prefix where the library finds its
      // bundled flag + icon SVGs. The vite plugin copies those into
      // ``public/bods-dagre-images`` at build start.
      draw(statements, el, "/bods-dagre-images");
    } catch (err) {
      // The library throws when it doesn't recognise a statement
      // shape. Surface the error in-place rather than crashing the app.
      el.innerHTML = `<p class="text-xs text-red-600 p-2">
        BODS graph render failed: ${escapeHtml(String(err))}
      </p>`;
    }
  }, [statements]);

  if (statements.length === 0) {
    return (
      <p className="text-xs text-slate-400 italic">
        No BODS statements to visualise.
      </p>
    );
  }

  return (
    <div className="bg-white border border-slate-200 rounded">
      <div
        ref={containerRef}
        // The library renders an SVG that scales horizontally; cap
        // height + allow scroll for very wide chains.
        className="overflow-auto p-2"
        style={{ maxHeight: 480 }}
      />
    </div>
  );
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
