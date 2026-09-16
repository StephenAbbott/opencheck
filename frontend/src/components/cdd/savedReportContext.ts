/**
 * The saved-report context (Phase 217).
 *
 * A saved report renders through the same components as a live check. Most of
 * them fold over the replayed events and need to know nothing. A few fetch
 * something live when opened — a source's Visualise / Statements / JSON drawer
 * calls `/deepen`, New Zealand associations call their own route — and on a
 * saved page each of those must show what was saved or say it was not, never
 * today's record. They are nested several components deep inside the source
 * cards, so the saved payload reaches them through context rather than
 * through props threaded past components that do not care.
 *
 * `null` outside a saved report: every consumer's live behaviour is unchanged.
 */

import { createContext, useContext } from "react";
import type { DeepenResponse } from "../../lib/api";

export interface SavedReportContextValue {
  /** The saved mapping for one result, or null when it was not deepened. */
  deepen: (sourceId: string, hitId: string) => DeepenResponse | null;
}

export const SavedReportContext = createContext<SavedReportContextValue | null>(null);

export function useSavedReport(): SavedReportContextValue | null {
  return useContext(SavedReportContext);
}
