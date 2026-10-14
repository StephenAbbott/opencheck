import { useEffect, useSyncExternalStore } from "react";
import { Button } from "./Button";
import {
  getNewVersionAvailable,
  startVersionWatch,
  subscribeNewVersion,
} from "../../lib/buildVersion";

/** The banner's sentence — exported so the test pins the words, not the markup. */
export const NEW_VERSION_TEXT =
  "A new version of OpenCheck is available. This page keeps working, but a tab you have not opened yet may not load until you reload.";

/**
 * "A new version is available" (Phase 331).
 *
 * Starts the version watch (`lib/buildVersion.ts`) and shows one line with a
 * reload once the server serves a different build from this page's. Polite,
 * not an alert: nothing has failed yet, and the reader may be mid-report.
 * Renders nothing in development and until a newer build is seen.
 */
export function NewVersionBanner() {
  const available = useSyncExternalStore(subscribeNewVersion, getNewVersionAvailable, () => false);
  useEffect(() => startVersionWatch(), []);
  if (!available) return null;
  return (
    <div
      role="status"
      data-testid="new-version-banner"
      className="border-b border-oo-info-border bg-oo-info-bg px-4 py-2 sm:px-7"
    >
      <div className="mx-auto flex max-w-oo-page flex-wrap items-center gap-x-3 gap-y-1.5">
        <p className="text-oo-small text-oo-info-text">{NEW_VERSION_TEXT}</p>
        <Button variant="secondary" size="sm" onClick={() => window.location.reload()}>
          Reload now
        </Button>
      </div>
    </div>
  );
}
