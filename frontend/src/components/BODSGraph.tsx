import { useEffect, useRef } from "react";
// The library lives at ``window.BODSDagre`` because index.html loads
// ``/bods-dagre.js`` as a classic <script> (not a module). The Vite
// plugin in vite.config.ts copies the file from node_modules into
// ``public/`` at build start. See bods-dagre.d.ts for the typings.

/**
 * Renders a BODS v0.4 statement bundle as a directed ownership graph
 * using @openownership/bods-dagre.
 *
 * The library mutates a target ``<div>`` directly. We give it a fresh
 * container per render so React's reconciliation never fights with the
 * SVG dagre injects.
 *
 * A few quirks of the upstream library:
 *
 * - It reads ``container.clientWidth`` to set the SVG's intrinsic
 *   width. If the container has zero width when ``draw`` runs (because
 *   it sits inside a not-yet-laid-out parent — e.g. a freshly-opened
 *   ``<details>``), internal popper code can throw with
 *   "Cannot read properties of undefined". We guard by deferring the
 *   draw with ``requestAnimationFrame`` so layout has settled, and we
 *   bail out (with a friendly message) when ``clientWidth`` is still 0.
 *
 * - The library probes for optional ``#zoom_in`` / ``#zoom_out`` /
 *   ``#download-svg`` / ``#download-png`` buttons by ID and wires
 *   click handlers when they exist. We render them so users get the
 *   same controls the upstream demo provides.
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

    const lib = window.BODSDagre;
    if (!lib?.draw) {
      el.innerHTML = `<p class="text-xs text-red-600 p-2">
        BODS visualisation library failed to load. Run
        <code>npm install</code> in the frontend directory and
        reload the page.
      </p>`;
      return;
    }

    // Defensive sanitiser for the bods-dagre input.
    //
    // The Open Ownership extraction script ships subgraphs walked out
    // to ``--max-hops`` from each subject. That naturally produces
    // relationship statements whose ``subject`` or ``interestedParty``
    // points at an entity sitting just past the walk boundary — those
    // dangling references make dagre throw
    // ``Invalid argument expected string``.
    //
    // We:
    //   1. Drop statements without a ``statementId`` (or non-string).
    //   2. Build the set of valid entity / person statement ids.
    //   3. Drop relationships whose ``subject`` or
    //      ``interestedParty`` doesn't reference an in-bundle id.
    const sanitised = sanitiseBundle(statements);
    if (sanitised.length === 0) {
      el.innerHTML = `<p class="text-xs text-oo-muted p-2">
        Bundle has no statements safe to visualise.
      </p>`;
      return;
    }

    // Defer until after the next paint so ``clientWidth`` is settled.
    let cancelled = false;
    const handle = requestAnimationFrame(() => {
      if (cancelled) return;
      if (el.clientWidth === 0) {
        // Parent hasn't laid out yet; render a hint rather than crashing.
        el.innerHTML = `<p class="text-xs text-slate-500 p-2">
          Container has no width yet — try expanding the deepen panel.
        </p>`;
        return;
      }
      try {
        // bods-dagre's exported ``draw`` actually destructures a single
        // options object; the README's positional signature is out of
        // date. Keys (from inspecting dist/bods-dagre.js):
        //   { data, selectedData, container, imagesPath,
        //     labelLimit, rankDir, viewProperties, useTippy }
        // ``selectedData`` defaults to the same array — without it
        // internal code reads ``undefined.something`` later.
        lib.draw({
          data: sanitised,
          selectedData: sanitised,
          container: el,
          imagesPath: "/bods-dagre-images",
          rankDir: "TB",
          useTippy: true,
        });
      } catch (err) {
        // The library throws when it doesn't recognise a statement
        // shape, or when popper hits an undefined ancestor. Surface
        // the error in-place AND log the full stack to the console
        // so we can see which line of bods-dagre.js threw.
        // eslint-disable-next-line no-console
        console.error("[BODSGraph] draw() threw:", err);
        const msg = err instanceof Error ? err.message : String(err);
        const stack = err instanceof Error && err.stack ? err.stack : "";
        el.innerHTML =
          `<div class="p-2 text-xs">
            <p class="text-red-600">BODS graph render failed: ${escapeHtml(msg)}</p>
            ${stack ? `<details class="mt-2"><summary class="text-slate-500 cursor-pointer">stack</summary><pre class="mt-1 text-[10px] whitespace-pre-wrap text-slate-600">${escapeHtml(stack)}</pre></details>` : ""}
          </div>`;
      }
    });

    return () => {
      cancelled = true;
      cancelAnimationFrame(handle);
    };
  }, [statements]);

  if (statements.length === 0) {
    return (
      <p className="text-xs text-oo-muted italic">
        No BODS statements to visualise.
      </p>
    );
  }

  return (
    <div className="bg-white border border-oo-rule rounded-oo">
      {/* Optional UI controls the library wires up by ID lookup. */}
      <div className="flex items-center justify-end gap-1 px-2 py-1 border-b border-oo-rule">
        <button
          id="zoom_out"
          type="button"
          aria-label="Zoom out"
          className="text-oo-muted hover:text-oo-blue text-sm font-mono px-2"
        >
          −
        </button>
        <button
          id="zoom_in"
          type="button"
          aria-label="Zoom in"
          className="text-oo-muted hover:text-oo-blue text-sm font-mono px-2"
        >
          +
        </button>
        <button
          id="download-svg"
          type="button"
          className="text-oo-muted hover:text-oo-blue text-[11px] font-mono px-2"
        >
          SVG
        </button>
        <button
          id="download-png"
          type="button"
          className="text-oo-muted hover:text-oo-blue text-[11px] font-mono px-2"
        >
          PNG
        </button>
      </div>
      <div
        ref={containerRef}
        // The library renders an SVG that scales horizontally; the
        // explicit width gives popper / dagre a stable measurement
        // and avoids the zero-width edge case described above. Cap
        // height + allow scroll for very wide chains.
        className="overflow-auto p-2"
        style={{ width: "100%", minWidth: 320, maxHeight: 480 }}
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

/**
 * Defensive sanitiser for BODS bundles before they reach bods-dagre.
 *
 * dagre uses statementIds as graph node ids. If any node id is
 * ``undefined`` or non-string — or if a relationship references a
 * node id that isn't in the bundle (for example, an entity statement
 * that sits one ``--max-hops`` past the walked subject) — dagre
 * throws ``Invalid argument expected string``. We strip those before
 * draw() to keep the visualisation rendering whatever's whole.
 */
function sanitiseBundle(input: unknown[]): unknown[] {
  // First pass: filter to statements with a string statementId.
  const wellTyped: Record<string, unknown>[] = [];
  for (const raw of input) {
    if (!raw || typeof raw !== "object") continue;
    const stmt = raw as Record<string, unknown>;
    if (typeof stmt.statementId === "string" && stmt.statementId.length > 0) {
      wellTyped.push(stmt);
    }
  }

  // Build the set of in-bundle node ids — entities + persons only;
  // relationships are edges, not nodes.
  const nodeIds = new Set<string>();
  for (const stmt of wellTyped) {
    const recordType = stmt.recordType ?? stmt.statementType;
    if (recordType === "entity" || recordType === "person") {
      nodeIds.add(stmt.statementId as string);
    }
  }

  // bods-dagre runs ``compare-versions`` on each statement's
  // ``publicationDetails.bodsVersion``, which throws
  // ``TypeError: Invalid argument expected string`` when the value
  // isn't a string. SQLite stores ``0.4`` as a REAL by default, so
  // bundles produced by the extractor sometimes carry a number here.
  // Normalise every occurrence to a string up front.
  for (const stmt of wellTyped) {
    const pub = stmt.publicationDetails as
      | Record<string, unknown>
      | undefined;
    if (pub && pub.bodsVersion !== undefined && pub.bodsVersion !== null) {
      pub.bodsVersion = String(pub.bodsVersion);
    }
  }

  // Second pass: normalise and filter.
  //
  // For RELATIONSHIP statements:
  //   - Drop those whose subject or interestedParty points outside the bundle.
  //   - bods-dagre expects recordDetails.subject and recordDetails.interestedParty
  //     as *string* statementIds.
  //   - BODS v0.4 (OpenCheck mapper): bare strings already — pass through as-is.
  //   - Legacy / Open Ownership extraction: objects like
  //       { describedByEntityStatement: "<id>" }
  //     — flatten to strings so bods-dagre draws the edge.
  //
  // For ENTITY and PERSON statements:
  //   - bods-dagre v0.4 resolves graph edges with:
  //       data.find(d => d.recordId === edgeSource)
  //     where edgeSource is the relationship's subject/interestedParty string.
  //     If statementId and recordId differ, the lookup returns undefined and
  //     bods-dagre shows "Unknown" placeholders. We normalise recordId =
  //     statementId so the lookup always succeeds.
  //   - bods-dagre reads ``recordDetails.jurisdiction.code`` for flags, but
  //     BODS v0.4 uses ``incorporatedInJurisdiction``. Copy the field so flags
  //     appear.
  const normalised: unknown[] = [];
  for (const stmt of wellTyped) {
    const recordType = stmt.recordType ?? stmt.statementType;

    if (recordType === "relationship") {
      const rd =
        (stmt.recordDetails as Record<string, unknown> | undefined) ?? {};

      // Resolve subject — bare string (v0.4) or wrapped object (legacy).
      const rawSubject = rd.subject;
      const subjectId: string | undefined =
        typeof rawSubject === "string"
          ? rawSubject
          : typeof rawSubject === "object" && rawSubject !== null
          ? ((rawSubject as Record<string, unknown>).describedByEntityStatement as string | undefined) ??
            ((rawSubject as Record<string, unknown>).describedByPersonStatement as string | undefined)
          : undefined;

      // Resolve interestedParty — bare string (v0.4) or wrapped object (legacy).
      const rawIP = rd.interestedParty;
      const interestedId: string | undefined =
        typeof rawIP === "string"
          ? rawIP
          : typeof rawIP === "object" && rawIP !== null
          ? ((rawIP as Record<string, unknown>).describedByEntityStatement as string | undefined) ??
            ((rawIP as Record<string, unknown>).describedByPersonStatement as string | undefined)
          : undefined;

      // Drop dangling references — they'd produce orphan edges.
      if (!subjectId || !nodeIds.has(subjectId)) continue;
      if (!interestedId || !nodeIds.has(interestedId)) continue;

      // Ensure bare-string format so bods-dagre draws the edge.
      normalised.push({
        ...stmt,
        recordDetails: {
          ...rd,
          subject: subjectId,
          interestedParty: interestedId,
        },
      });
    } else if (recordType === "entity") {
      const rd =
        (stmt.recordDetails as Record<string, unknown> | undefined) ?? {};
      // Ensure recordId === statementId (needed for bods-dagre lookup) and
      // copy incorporatedInJurisdiction → jurisdiction for flag display.
      normalised.push({
        ...stmt,
        recordId: stmt.statementId,
        recordDetails: rd.incorporatedInJurisdiction && !rd.jurisdiction
          ? { ...rd, jurisdiction: rd.incorporatedInJurisdiction }
          : rd,
      });
    } else if (recordType === "person" || recordType === "personStatement") {
      // Ensure recordId === statementId for bods-dagre lookup.
      normalised.push({ ...stmt, recordId: stmt.statementId });
    } else {
      normalised.push(stmt);
    }
  }
  return normalised;
}
