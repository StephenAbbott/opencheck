import { Component, type ErrorInfo, type ReactNode } from "react";
import { Button } from "./Button";
import PanelSection from "./PanelSection";

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

/**
 * An error boundary around one lazy mode panel (Phase 246).
 *
 * Every check mode is a lazy chunk. Before this, a chunk that failed to load —
 * most often a tab opened on a page served before a deploy, whose chunk hashes
 * no longer exist — threw out of `<Suspense>` with nothing to catch it, and
 * React unmounted the whole app: the subject, the verdict and every other tab
 * went blank with it. The same happened for a panel that threw while
 * rendering. Now the failure stays inside the tab, and the tab says so rather
 * than showing nothing, which would read as "nothing to show". A reload is
 * the offered fix because that is what fetches the current chunks.
 */
export class PanelBoundary extends Component<
  { label: string; children: ReactNode },
  { failed: boolean }
> {
  state = { failed: false };

  static getDerivedStateFromError(): { failed: boolean } {
    return { failed: true };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    // Kept in the console for whoever is debugging; the reader gets a sentence.
    console.warn(`The ${this.props.label} tab failed to render`, error, info.componentStack);
  }

  render(): ReactNode {
    if (!this.state.failed) return this.props.children;
    return (
      <PanelSection>
        <div role="alert" className="rounded-oo border border-oo-warn-border bg-oo-warn-bg px-4 py-3">
          <p className="text-oo-small text-oo-warn-text">
            The {this.props.label} tab could not be loaded. The rest of this report
            is unaffected. Reloading the page usually fixes this — OpenCheck may
            have been updated since the page was opened.
          </p>
          <Button variant="warn" size="sm" className="mt-2" onClick={() => window.location.reload()}>
            Reload the page
          </Button>
        </div>
      </PanelSection>
    );
  }
}
