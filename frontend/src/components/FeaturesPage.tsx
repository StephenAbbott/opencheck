/**
 * `/features` — what OpenCheck can do, in one page.
 *
 * The shape is an index and a stack: every feature is listed once in a rail
 * that stays on screen, and once in full below. That is deliberate — the page
 * has two readers. One arrives asking "what is this thing", and needs the
 * whole capability set visible without scrolling; the other arrives from a
 * link or a talk wanting one feature, and needs `/features#time-machine` to
 * land on it. A stack alone fails the first; a grid of cards alone fails the
 * second, because six equal cards give every feature the same two lines.
 *
 * Three things worth knowing before editing:
 *
 * - **The content is in `lib/features.ts`**, not here. The logic-only suite
 *   pins the copy (the two-sentence rule, one call to action each, unique
 *   anchors); this file is pinned by `FeaturesPage.test.tsx` for the things
 *   only markup can be wrong about — six sections, six index links, each link
 *   addressing its own section, one image per feature with real alt text.
 *   Adding a feature therefore means editing `lib/features.ts` and dropping a
 *   PNG in `public/features/`. Nothing here changes.
 *
 * - **The calls to action are plain `<a href>`.** Every one is a URL the app
 *   already understands (`/?lei=…&mode=…`, `/batch`), so they are shareable,
 *   right-clickable, and cannot fall out of step with App's state machine the
 *   way an `onClick` into `lookupLei` would. The cost is a full page load,
 *   which on a page whose whole purpose is to send the reader somewhere else
 *   is the right trade.
 *
 * - **Scroll-spy is progressive.** `IntersectionObserver` marks the rail
 *   entry for whichever section is in view. Where it is unavailable the rail
 *   is a plain list of working anchors, which is the whole feature minus the
 *   highlight — so the observer is never load-bearing.
 */

import { useEffect, useState } from "react";

import { FEATURES, ledeSentence } from "../lib/features";
import { SectionLabel } from "./ui";
import { FeatureMark } from "./ui/FeatureMark";

export function FeaturesPage({
  /** From `/sources`; `null` until it arrives. See `ledeSentence`. */
  sourceCount = null,
}: {
  sourceCount?: number | null;
} = {}) {
  const [active, setActive] = useState<string | null>(FEATURES[0]?.id ?? null);

  useEffect(() => {
    if (typeof IntersectionObserver === "undefined") return;
    const seen = new Map<string, number>();
    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          seen.set(entry.target.id, entry.isIntersecting ? entry.intersectionRatio : 0);
        }
        // The rail names one section, so ties are broken by document order —
        // FEATURES order — rather than by whichever entry arrived last.
        let best: string | null = null;
        let bestRatio = 0;
        for (const f of FEATURES) {
          const ratio = seen.get(f.id) ?? 0;
          if (ratio > bestRatio) {
            best = f.id;
            bestRatio = ratio;
          }
        }
        if (best) setActive(best);
      },
      // A band across the middle of the viewport: a section counts as "the
      // one being read" when it is there, not when its top edge crosses zero.
      { rootMargin: "-20% 0px -55% 0px", threshold: [0, 0.25, 0.5, 1] },
    );
    for (const f of FEATURES) {
      const el = document.getElementById(f.id);
      if (el) observer.observe(el);
    }
    return () => observer.disconnect();
  }, []);

  return (
    <section aria-labelledby="features-heading">
      <h2
        id="features-heading"
        className="font-head font-bold text-[clamp(1.35rem,3vw,1.8rem)] text-oo-ink mb-2 leading-tight"
      >
        What OpenCheck can do
      </h2>
      <p className="text-oo-body leading-[1.75] text-oo-muted mb-8 max-w-2xl">
        {ledeSentence(sourceCount)}
      </p>

      <div className="lg:flex lg:gap-10 lg:items-start">
        {/*
          One nav, two presentations. On phones it is a horizontal strip that
          scrolls; from `lg` it is a sticky column. The alternative — two navs
          behind `lg:hidden` / `hidden lg:block` — is two lists of the same
          links for a screen reader to read out, which is the accessibility
          bug the Phase 124 sweep spent its time removing.
        */}
        <nav
          aria-label="Features on this page"
          className="mb-7 lg:mb-0 lg:sticky lg:top-6 lg:w-[248px] lg:flex-shrink-0"
        >
          <SectionLabel as="h3" className="mb-3 lg:ml-2.5">
            On this page
          </SectionLabel>
          <ul className="flex gap-2.5 overflow-x-auto pb-1 -mx-1 px-1 lg:mx-0 lg:px-0 lg:flex-col lg:gap-0.5 lg:overflow-visible lg:pb-0">
            {FEATURES.map((f) => {
              const current = active === f.id;
              return (
                <li key={f.id} className="flex-shrink-0 lg:flex-shrink">
                  <a
                    href={`#${f.id}`}
                    aria-current={current ? "true" : undefined}
                    className={`flex items-center gap-3 min-h-[44px] rounded-full lg:rounded-oo px-3.5 py-1.5 lg:px-2.5 border transition-colors ${
                      current
                        ? "bg-oo-soft border-oo-softBorder"
                        : "bg-white border-oo-rule lg:bg-transparent lg:border-transparent hover:bg-oo-soft"
                    }`}
                  >
                    <FeatureMark icon={f.icon} accent={f.accent} glyph={f.glyph} size={30} />
                    <span className="text-oo-small font-medium text-oo-ink leading-tight whitespace-nowrap lg:whitespace-normal">
                      {f.name}
                    </span>
                  </a>
                </li>
              );
            })}
          </ul>
        </nav>

        <div className="flex-1 min-w-0 flex flex-col gap-5 sm:gap-6">
          {FEATURES.map((f) => (
            <article
              key={f.id}
              id={f.id}
              // `scroll-mt` keeps an anchored section clear of the sticky
              // rail's top offset; without it `/features#time-machine` lands
              // with the heading under the viewport edge.
              className="scroll-mt-6 bg-white border border-oo-rule rounded-oo p-5 sm:p-6"
              aria-labelledby={`${f.id}-name`}
            >
              <div className="flex items-center gap-3.5 mb-3.5">
                <FeatureMark
                  icon={f.icon}
                  accent={f.accent}
                  glyph={f.glyph}
                  size={48}
                  className="hidden sm:inline-flex"
                />
                <FeatureMark
                  icon={f.icon}
                  accent={f.accent}
                  glyph={f.glyph}
                  size={40}
                  className="sm:hidden"
                />
                <div className="min-w-0">
                  <SectionLabel as="p" className="mb-0.5">
                    {f.kind}
                  </SectionLabel>
                  <h3
                    id={`${f.id}-name`}
                    className="font-head font-bold text-oo-head sm:text-oo-title text-oo-ink leading-tight"
                  >
                    {f.name}
                  </h3>
                </div>
              </div>

              <p className="text-oo-small sm:text-oo-body leading-[1.75] text-oo-muted mb-4">
                {f.description}
              </p>

              <img
                src={f.image.src}
                alt={f.image.alt}
                width={f.image.width}
                height={f.image.height}
                loading="lazy"
                decoding="async"
                className="w-full h-auto rounded-oo border border-oo-rule bg-oo-bg"
              />

              <a
                href={f.cta.href}
                className="mt-4 inline-flex items-center gap-2 min-h-[44px] text-oo-small font-medium text-oo-blue hover:text-oo-burst"
              >
                {f.cta.label}
                <svg
                  width="14"
                  height="14"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2.2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  aria-hidden="true"
                  className="flex-shrink-0"
                >
                  <path d="M5 12h14" />
                  <path d="m12 5 7 7-7 7" />
                </svg>
              </a>
            </article>
          ))}
        </div>
      </div>

      <p className="mt-8 text-oo-small leading-[1.75] text-oo-muted max-w-2xl">
        Every check runs against open data and open standards. The full source
        list, with licence and current health, is on{" "}
        <a href="/sources" className="underline text-oo-blue hover:text-oo-burst">
          the sources page
        </a>
        ; how it all fits together is on{" "}
        <a href="/about" className="underline text-oo-blue hover:text-oo-burst">
          the about page
        </a>
        .
      </p>
    </section>
  );
}
