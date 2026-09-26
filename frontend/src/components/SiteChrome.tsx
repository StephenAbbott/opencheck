import { useState } from "react";
import type { View } from "../lib/views";
import { NAV_ITEMS } from "../lib/views";
import { OpenCheckIcon } from "./icons";

/*
 * The site's chrome — the header banner (logo, nav, the constant search
 * field) and the GODIN ribbon + footer. Moved out of App.tsx in Phase 246,
 * unchanged except that the header field keeps its own value: App gets the
 * submitted text through `onSearch` and decides what it is (a pasted LEI runs
 * the lookup, anything else is a company-name search).
 */

export function SiteHeader({
  view,
  onNavigate,
  onHome,
  searchPanelsCollapsed,
  onOpenSearchPanel,
  onSearch,
}: {
  view: View;
  onNavigate: (v: View) => void;
  /** Back to a fresh homepage — App's `resetToHome`. */
  onHome: () => void;
  /** A report is on screen and the search panel is folded away (Phase 245). */
  searchPanelsCollapsed: boolean;
  onOpenSearchPanel: () => void;
  onSearch: (query: string) => void;
}) {
  /** The header field's own value, kept apart from the name-search panel's
   *  so typing in one does not rewrite the other under the reader. */
  const [headerQuery, setHeaderQuery] = useState("");
  function submit(e: React.FormEvent) {
    e.preventDefault();
    const q = headerQuery.trim();
    if (!q) return;
    setHeaderQuery("");
    onSearch(q);
  }
  return (
    <>
      {/*
       * Header — full-width dark banner, BO design system.
       * Decorative blue radial gradient sits top-right (rgba 61,48,212,0.28)
       * fading to transparent. Inline style because Tailwind doesn't
       * have a clean utility for offset radial gradients.
       */}
      <header
        // Phase 241: no `overflow-hidden`. The gradient is a background, which
        // the box clips already; what the overflow clipped was the nav, which
        // at 320px read "Fea" and stayed focusable. The nav wraps instead.
        className="relative bg-oo-navy text-white px-4 sm:px-10 lg:px-16 py-3 sm:py-4"
        role="banner"
        style={{
          backgroundImage:
            "radial-gradient(circle 500px at calc(100% + 80px) -80px, rgba(61, 48, 212, 0.28), transparent)",
        }}
      >
        <div className="max-w-oo-page mx-auto relative">
          <div className="flex items-center justify-between gap-4">
            {/* On mobile the search field is hidden, so this group is the only
                child of the row and the nav ended up crowded against the
                wordmark with the whole right half of the banner empty. Full
                width with the two ends pushed apart puts the mark at one edge
                and the links at the other; from `md` the search field takes
                the right-hand end and this reverts to sitting beside the
                mark.
                Phase 251: below `sm` the mark, gaps and search button are
                tightened so mark + search + three links fit one line from
                360px (on a 390px iPhone they needed 369px of 358). At 320px
                the row still wraps rather than clipping. */}
            <div className="flex flex-wrap items-center gap-x-2.5 sm:gap-x-4 gap-y-1 w-full justify-between md:w-auto md:justify-start">
              <button
                type="button"
                onClick={onHome}
                aria-label="OpenCheck — back to homepage"
                className="flex items-center gap-2 sm:gap-2.5 hover:opacity-80 transition-opacity text-left"
              >
                <OpenCheckIcon className="h-6 sm:h-7 w-auto flex-shrink-0" />
                <span className="font-head font-bold text-white leading-tight text-lg sm:text-xl">
                  Open<span className="text-oo-mark-line">Check</span>
                </span>
              </button>
            <nav aria-label="Site navigation" className="flex flex-wrap items-center gap-x-2.5 sm:gap-x-5">
              {/* Phones have no header field (it needs ~300px), so on a report
                  this is the search entry point — the row that used to sit
                  under the header is gone (Phase 245). */}
              {searchPanelsCollapsed && (
                <button
                  type="button"
                  onClick={onOpenSearchPanel}
                  aria-label="Search for another company or person"
                  className="md:hidden inline-flex min-h-[44px] min-w-[32px] items-center justify-center text-white/80 hover:text-white"
                >
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                    strokeWidth="2" strokeLinecap="round" aria-hidden="true">
                    <circle cx="11" cy="11" r="7" /><path d="m20 20-3.5-3.5" />
                  </svg>
                </button>
              )}
              {NAV_ITEMS.map((item) => {
                const current = view === item.view;
                return (
                  <button
                    key={item.view}
                    type="button"
                    onClick={() => onNavigate(item.view)}
                    aria-current={current ? "page" : undefined}
                    className={`min-h-[44px] text-oo-small transition-colors ${
                      current
                        ? "text-white font-medium border-b-2 border-oo-mark-line"
                        : "text-white/80 hover:text-white"
                    }`}
                  >
                    {item.label}
                  </button>
                );
              })}
            </nav>
            </div>
            {/* The constant search field. On a report page the tabbed panel is
                collapsed to a single prompt row, which left the header — the
                one piece of chrome present on every page — with no way to
                start a search from. The source counts that used to sit here
                are said in the hero and listed in full on /sources; a stat
                does not need to be in the banner of a report about a company.

                It handles the two things a header field can honestly handle:
                a pasted LEI runs straight through, anything else goes to the
                company-name search. National ID and person search stay in the
                full panel, which "More search options" beside it reopens. */}
            <div className="hidden md:flex items-center gap-4">
            <form
              onSubmit={submit}
              role="search"
              aria-label="Search for a company"
              // Phase 241: a visible focus ring (oo.mark.line on navy, 9.4:1). The
              // input's own outline is off, and a 1.19:1 fill change was the
              // only cue that the field had focus.
              className="hidden md:flex items-center gap-2 rounded-oo border border-white/25 bg-white/10 focus-within:bg-white/15 focus-within:border-white/45 focus-within:ring-2 focus-within:ring-oo-mark-line px-3 py-1.5 min-w-[300px] transition-colors"
            >
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                strokeWidth="2" strokeLinecap="round" aria-hidden="true" className="text-white/70 shrink-0">
                <circle cx="11" cy="11" r="7" /><path d="m20 20-3.5-3.5" />
              </svg>
              <label htmlFor="oo-header-search" className="sr-only">
                Company name or LEI
              </label>
              <input
                id="oo-header-search"
                type="text"
                value={headerQuery}
                onChange={(e) => setHeaderQuery(e.target.value)}
                placeholder="Company name or LEI"
                className="min-w-0 flex-1 bg-transparent text-oo-small text-white placeholder:text-white/60 focus:outline-none"
              />
              {/* Enter submits; the button is for assistive tech that
                  lists buttons, and takes no Tab stop while invisible. */}
              <button type="submit" className="sr-only" tabIndex={-1}>
                Search
              </button>
            </form>
            {/* National ID and person search need the full panel. On a report
                it is folded away; this reopens it (Phase 245). */}
            {searchPanelsCollapsed && (
              <button
                type="button"
                onClick={onOpenSearchPanel}
                className="hidden md:inline-flex shrink-0 min-h-[44px] items-center text-oo-small text-white/80 underline-offset-2 hover:text-white hover:underline"
              >
                More search options
              </button>
            )}
            </div>
          </div>
        </div>
      </header>
    </>
  );
}

export function SiteFooter({ onNavigate }: { onNavigate: (v: View) => void }) {
  return (
    <>
      {/* GODIN ribbon — permanent attribution banner. */}
      <aside
        aria-label="GODIN — Global Open Data Integration Network"
        className="px-6 sm:px-10 lg:px-16 py-4 text-white/90 text-oo-small leading-[1.6]"
        style={{
          background:
            "linear-gradient(90deg, rgb(7, 116, 95) 0%, rgb(11, 110, 92) 100%)",
        }}
      >
        <div className="max-w-oo-page mx-auto flex flex-wrap items-center gap-x-4 gap-y-2">
          <a
            href="https://godin.gleif.org/"
            target="_blank"
            rel="noreferrer"
            aria-label="GODIN — Global Open Data Integration Network (opens in new tab)"
          >
            <img
              src="https://godin.gleif.org/images/512/14456540/GODINRGBColourWide.png"
              alt="GODIN logo"
              className="h-8 w-auto"
              style={{ filter: "brightness(0) invert(1)" }}
            />
          </a>
          <p className="flex-1 min-w-0">
            OpenCheck is built on open data and open standards from{" "}
            <a
              href="https://godin.gleif.org/"
              target="_blank"
              rel="noreferrer"
              className="underline underline-offset-2 font-medium hover:text-white"
            >
              GODIN members
            </a>{" "}
            and others, and demonstrates the kind of interoperability GODIN
            exists to enable.{" "}
            <button
              type="button"
              onClick={() => onNavigate("behind")}
              className="underline underline-offset-2 font-medium hover:text-white"
            >
              How it works →
            </button>
          </p>
        </div>
      </aside>

      <footer className="border-t border-oo-rule bg-oo-bg px-6 sm:px-10 lg:px-16 pt-8 pb-6">
        <div className="max-w-oo-page mx-auto">
          {/* Two-column grid: brand + tagline left, link groups right */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-8 sm:gap-4">
            {/* Left: brand + tagline */}
            <div>
              <div className="font-head font-bold text-oo-lead text-oo-ink">
                Open<span className="text-oo-blue">Check</span>
              </div>
              <p className="mt-2 text-oo-meta text-oo-muted leading-relaxed max-w-[220px]">
                Customer due diligence checks powered by the Legal Entity
                Identifier and open standards.
              </p>
            </div>
            {/* Right: link groups */}
            <div className="flex gap-10 sm:justify-end">
              <div>
                <h3 className="font-body text-[10px] font-medium tracking-widest uppercase text-oo-muted mb-3">
                  Project
                </h3>
                <a
                  href="/api"
                  onClick={(e) => { e.preventDefault(); onNavigate("api"); }}
                  className="block font-mono text-oo-meta text-oo-blue hover:text-oo-burst mb-2"
                >
                  API
                </a>
                <a
                  href="/changelog"
                  onClick={(e) => { e.preventDefault(); onNavigate("changelog"); }}
                  className="block font-mono text-oo-meta text-oo-blue hover:text-oo-burst mb-2"
                >
                  Changelog
                </a>
                <a
                  href="https://github.com/StephenAbbott/opencheck"
                  target="_blank"
                  rel="noreferrer"
                  className="block font-mono text-oo-meta text-oo-blue hover:text-oo-burst mb-2"
                >
                  GitHub
                </a>
                <a
                  href="/sources"
                  onClick={(e) => { e.preventDefault(); onNavigate("sources"); }}
                  className="block font-mono text-oo-meta text-oo-blue hover:text-oo-burst mb-2"
                >
                  Sources
                </a>
                <a
                  href="/watchlist"
                  onClick={(e) => { e.preventDefault(); onNavigate("watchlist"); }}
                  className="block font-mono text-oo-meta text-oo-blue hover:text-oo-burst mb-2"
                >
                  Watchlist
                </a>
                <a
                  href="/features"
                  onClick={(e) => { e.preventDefault(); onNavigate("features"); }}
                  className="block font-mono text-oo-meta text-oo-blue hover:text-oo-burst mb-2"
                >
                  Features
                </a>
                <a
                  href="/about"
                  onClick={(e) => { e.preventDefault(); onNavigate("behind"); }}
                  className="block font-mono text-oo-meta text-oo-blue hover:text-oo-burst"
                >
                  About
                </a>
              </div>
              <div>
                <h3 className="font-body text-[10px] font-medium tracking-widest uppercase text-oo-muted mb-3">
                  Legal
                </h3>
                <a
                  href="https://github.com/StephenAbbott/opencheck?tab=License-1-ov-file"
                  target="_blank"
                  rel="noreferrer"
                  className="block font-mono text-oo-meta text-oo-blue hover:text-oo-burst mb-2"
                >
                  MIT licence
                </a>
                <a
                  href="https://github.com/StephenAbbott/opencheck/blob/main/ATTRIBUTIONS.md"
                  target="_blank"
                  rel="noreferrer"
                  className="block font-mono text-oo-meta text-oo-blue hover:text-oo-burst"
                >
                  ATTRIBUTIONS.md
                </a>
              </div>
            </div>
          </div>
          {/* Bottom strip */}
          <div className="mt-8 pt-4 border-t border-oo-rule text-[11px] text-oo-muted font-mono">
            Third-party data is licensed per source — see{" "}
            <a
              href="https://github.com/StephenAbbott/opencheck/blob/main/ATTRIBUTIONS.md"
              target="_blank"
              rel="noreferrer"
              className="text-oo-blue underline underline-offset-2 hover:text-oo-burst"
            >
              ATTRIBUTIONS.md
            </a>{" "}
            for details.
          </div>
        </div>
      </footer>
    </>
  );
}
