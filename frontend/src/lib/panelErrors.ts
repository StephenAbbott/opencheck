/**
 * Failures in the panels that fetch outside the lookup pipeline (Phase 124).
 *
 * `SecuritiesSection` and `SubsidiaryNetwork` call `/securities` and
 * `/subsidiaries` — separate endpoints that are not in `_dispatch`, emit no
 * `source_error`, are not in `sources_applicable` and are not covered by the
 * replay cache. Their failures were therefore invisible to every surface that
 * reports completeness: the verdict sentence, the coverage count and the
 * degraded-screens notice all exclude them by construction.
 *
 * `SecuritiesSection` was the worse of the two. Its catch swallowed the
 * rejection with no state write at all, so `meta` stayed null and the whole
 * section returned null — indistinguishable on screen from "this entity has no
 * securities". What disappeared includes the sanctioned-securities banner,
 * which the component's own docstring calls always-visible by design. A
 * transport failure silently converted an adverse finding into an absence: the
 * one place OpenCheck's "absence is not evidence of absence" principle was not
 * being applied.
 *
 * **This is deliberately a separate channel from `degradedSources`.** The
 * backend emits `signals` and `degraded_sources` on one event so the verdict
 * sentence and the coverage count are provably consistent with each other
 * (`build_verdict(merged, degraded_dicts)`); `onRiskSignals` also overwrites
 * the list wholesale, so a locally-injected record would be erased when the
 * event lands. Merging client-side failures in would let the count disagree
 * with a sentence that knows nothing about them — the exact failure that design
 * forbids. And the vocabularies do not match: a `/securities` failure is not a
 * derived risk check and suppresses no `RELATED_*` code.
 *
 * So these are reported alongside, in their own words, and the reader is told
 * what could not be checked rather than being shown a section that quietly
 * is not there.
 */

export type PanelId = "securities" | "subsidiaries";

export interface PanelError {
  panel: PanelId;
  /** What the reader now cannot rely on. Not a risk code — a plain statement. */
  missing: string;
  /** The failure, already made human. Never a raw `String(e)`. */
  detail: string;
}

const PANEL_LABEL: Record<PanelId, string> = {
  securities: "Listed securities",
  subsidiaries: "Subsidiary network",
};

const PANEL_MISSING: Record<PanelId, string> = {
  securities:
    "whether this entity has securities on a sanctions list — the check did not run",
  subsidiaries: "the GLEIF subsidiary network for this entity",
};

export function panelLabel(panel: PanelId): string {
  return PANEL_LABEL[panel];
}

/**
 * A thrown value into something a reader can act on.
 *
 * `SubsidiaryNetwork` rendered `String(e)`, so a reader saw
 * `Error: 500 Internal Server Error`. The status is worth keeping — it
 * distinguishes "try again" from "this will keep failing" — but it belongs in
 * a sentence, not as the sentence.
 */
export function describeFetchFailure(e: unknown): string {
  const raw = e instanceof Error ? e.message : String(e);
  const status = raw.match(/\b(\d{3})\b/)?.[1];
  if (status === "429") return "the service rate-limited the request";
  if (status === "404") return "the service had no record to return";
  if (status && status.startsWith("5")) return `the service errored (HTTP ${status})`;
  if (status) return `the request was refused (HTTP ${status})`;
  return "the request could not be completed";
}

export function panelError(panel: PanelId, e: unknown): PanelError {
  return { panel, missing: PANEL_MISSING[panel], detail: describeFetchFailure(e) };
}

/**
 * Phase 145: `/securities` no longer 500s when GLEIF is unavailable — it
 * returns 200 with `isin_list_available: false` and the sanctioned overlay (a
 * local index on the backend, no network) still applied. Two degraded shapes
 * reach this module rather than the section's own notice:
 *
 * - A deployment with no sanctions index configured has nothing left to show,
 *   so the section reports through here — the check genuinely did not run,
 *   exactly what the old 500 was (accidentally) telling the reader.
 * - A later page of the drawer failing leaves the sanctions banner standing,
 *   so the fixed PANEL_MISSING copy ("the check did not run") would be false;
 *   this names what is actually missing instead.
 */
export function securitiesOverlayUnavailable(): PanelError {
  return {
    panel: "securities",
    missing: PANEL_MISSING.securities,
    detail: "GLEIF could not be queried and this deployment has no sanctions index",
  };
}

export function securitiesPageUnavailable(): PanelError {
  return {
    panel: "securities",
    missing: "the rest of this entity's ISIN list — the next page could not be fetched",
    detail: "GLEIF is rate-limiting the ISIN list; try again shortly",
  };
}

/** One entry per panel — a retry that fails again replaces, never appends. */
export function mergePanelError(list: PanelError[], next: PanelError): PanelError[] {
  return [...list.filter((p) => p.panel !== next.panel), next];
}

export function clearPanelError(list: PanelError[], panel: PanelId): PanelError[] {
  return list.filter((p) => p.panel !== panel);
}
