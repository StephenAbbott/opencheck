/**
 * BtsCard / BtsBadge — the two primitives the long-form pages are built from.
 *
 * Lifted out of `App.tsx` in Phase 168, unchanged. It had no business being
 * there: both the About page and the API page are made of these, and a shared
 * primitive that lives inside the page that happens to import it first is
 * how a design system grows a second copy of itself.
 */

import type React from "react";

// ---------------------------------------------------------------------
// Behind the Scenes page (Phase 5)
// Explains OpenCheck's architecture, standards spine, and GODIN thesis.
// ---------------------------------------------------------------------

export function BtsCard({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div className="bg-white border border-oo-rule rounded-oo p-6">
      <h3 className="font-head font-bold text-[17px] text-oo-ink mb-3 leading-snug">
        {title}
      </h3>
      {children}
    </div>
  );
}

export function BtsBadge({ children }: { children: React.ReactNode }) {
  return (
    <span className="inline-block font-mono text-[10px] bg-oo-bg border border-oo-rule rounded px-1.5 py-0.5 text-oo-ink mr-1 mb-1">
      {children}
    </span>
  );
}
