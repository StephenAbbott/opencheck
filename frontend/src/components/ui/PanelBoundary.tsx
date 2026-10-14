import { Component, useSyncExternalStore, type ErrorInfo, type ReactNode } from "react";
import { Button } from "./Button";
import PanelSection from "./PanelSection";
import { isChunkLoadError } from "../../lib/chunkLoad";
import {
  checkForNewVersion,
  getNewVersionAvailable,
  subscribeNewVersion,
} from "../../lib/buildVersion";

/**
 * The loading line a lazy mode panel shows while its chunk arrives — one
 * component, so the six tabs cannot word it six ways.
 */
export function PanelLoading({ label }: { label: string }) {
  return (
    <PanelSection>
      <p className="text-oo-small text-oo-muted italic">Loading {label}…</p>
    </PanelSection>
  );
}

/** What the boundary is round: a mode tab ("the History tab"), or a part of
 *  the page named on its own ("the ownership graph", "the person report"). */
export type BoundaryKind = "tab" | "part";

/**
 * The sentence a failed panel shows (Phase 331). Pure, so the three cases are
 * pinned by the component test without a browser.
 *
 * A chunk that did not load and a panel that threw are different failures and
 * read differently: the first is the network or a deploy and a reload fixes
 * it; the second is a bug, and a reload only may. When the server is known to
 * serve a newer build, the sentence says that instead of guessing.
 */
export function boundaryMessage(
  label: string,
  kind: BoundaryKind,
  cause: "chunk" | "render",
  newVersion: boolean,
): string {
  const thing = kind === "tab" ? `The ${label} tab` : `The ${label}`;
  const rest = "The rest of this report is unaffected.";
  if (cause === "chunk" && newVersion) {
    return (
      `OpenCheck has been updated since this page was opened, and ${thing.charAt(0).toLowerCase()}` +
      `${thing.slice(1)} belongs to the new version. ${rest} Reload the page to load it.`
    );
  }
  if (cause === "chunk") {
    return (
      `${thing} could not be loaded — the connection may have dropped, or OpenCheck may ` +
      `have been updated since this page was opened. ${rest} Reloading the page usually fixes this.`
    );
  }
  return `${thing} hit an error and could not be shown. ${rest} Reloading the page may help.`;
}

function BoundaryFailure({
  label,
  kind,
  cause,
  compact,
}: {
  label: string;
  kind: BoundaryKind;
  cause: "chunk" | "render";
  compact: boolean;
}) {
  const newVersion = useSyncExternalStore(subscribeNewVersion, getNewVersionAvailable, () => false);
  const body = (
    <div role="alert" className="rounded-oo border border-oo-warn-border bg-oo-warn-bg px-4 py-3">
      <p className="text-oo-small text-oo-warn-text">
        {boundaryMessage(label, kind, cause, newVersion)}
      </p>
      <Button variant="warn" size="sm" className="mt-2" onClick={() => window.location.reload()}>
        Reload the page
      </Button>
    </div>
  );
  return compact ? body : <PanelSection>{body}</PanelSection>;
}

/**
 * An error boundary around one lazy part of the page (Phase 246; Phase 331).
 *
 * Every check mode, the person report and the ownership graph are lazy
 * chunks. Before Phase 246, a chunk that failed to load — most often a tab
 * opened on a page served before a deploy, whose chunk hashes no longer
 * exist — threw out of `<Suspense>` with nothing to catch it, and React
 * unmounted the whole app: the subject, the verdict and every other tab went
 * blank with it. The same happened for a panel that threw while rendering.
 * Now the failure stays inside the part that failed, and it says so rather
 * than showing nothing, which would read as "nothing to show".
 *
 * Phase 331: the chunks behind it are `lazyWithRetry`, so a single failed
 * load is retried before this ever renders; a chunk failure here asks the
 * server which build it serves, so the sentence can say OpenCheck was
 * updated; and the boundary now also wraps the person report and the two
 * lazy graph sites (`compact`, no band of its own).
 */
export class PanelBoundary extends Component<
  { label: string; kind?: BoundaryKind; compact?: boolean; children: ReactNode },
  { failed: boolean; cause: "chunk" | "render" }
> {
  state: { failed: boolean; cause: "chunk" | "render" } = { failed: false, cause: "render" };

  static getDerivedStateFromError(error: unknown): { failed: boolean; cause: "chunk" | "render" } {
    return { failed: true, cause: isChunkLoadError(error) ? "chunk" : "render" };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    // Kept in the console for whoever is debugging; the reader gets a sentence.
    console.warn(`The ${this.props.label} failed to render`, error, info.componentStack);
    if (isChunkLoadError(error)) void checkForNewVersion();
  }

  render(): ReactNode {
    if (!this.state.failed) return this.props.children;
    return (
      <BoundaryFailure
        label={this.props.label}
        kind={this.props.kind ?? "tab"}
        cause={this.state.cause}
        compact={Boolean(this.props.compact)}
      />
    );
  }
}
