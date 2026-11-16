/**
 * The homepage panels below the search — examples, new features, how it works.
 *
 * Lifted out of `App.tsx` in Phase 168, unchanged. It had no business being
 * there: they are what the homepage *is*, and none of them reads a single piece
 * of App's state: the picker takes an `onPick`; New Features (BatchInvite
 * until Phase 344) and How It Works take nothing at all.
 */

import {
  StepBridgeIcon,
  StepKeyIcon,
  StepNetworkIcon,
  StepShieldIcon,
} from "./icons";
import { RiskChip, RISK_PRESENTATION } from "./risk/RiskChip";
import { buttonClasses } from "./ui";
import {
  cardChips,
  EXAMPLE_VIEW_ANSWERS,
  EXAMPLE_VIEWS,
  exampleViewHref,
  moreFindingsLabel,
} from "../lib/exampleCards";
import type { CheckMode } from "../lib/checkMode";
import { MODE_TABS } from "./cdd/ModeTabs";
import { Icon } from "./ui/Icon";
import { NEW_FEATURES, NEW_FEATURE_TONES } from "../lib/newFeatures";

/**
 * Curated demo subjects. Since Phase 94 these are live lookups like any
 * other LEI (Rosneft and Taqa Bratani also have a pre-extracted Open
 * Ownership BODS bundle on disk under ``data/cache/bods_data/``). The list
 * is small + opinionated; users can paste any other LEI into the input.
 *
 * ``signals`` are every risk code production returns for the subject,
 * checked by hand (see the note above ``EXAMPLE_LEIS``), so the picker
 * cards show what the check finds before the user clicks.
 */
export interface ExampleSignal {
  code: string;
  confidence: "high" | "medium" | "low";
}

export interface ExampleLei {
  lei: string;
  name: string;
  hint?: string;
  signals?: ExampleSignal[];
}

// Signals shown on the picker cards. These are CLAIMS ABOUT PRODUCTION
// OUTPUT — verify against production (`/lookup?lei=…&refresh=true` on
// api.opencheck.world), never by reading the card back. Since Phase 277 the
// weekly findings regression compares every card here against production
// (`card_drift`, docs/findings-regression.md), and each curated subject has a
// golden file in backend/findings_golden/ that must be added or removed with
// its card.
//
// Last verified 2026-09-25 against production, after Phase 247 deployed.
// Each list is EVERY distinct risk code production returned for that
// subject, at the highest confidence it reached — the card draws the worst
// three and counts the rest (`lib/exampleCards.ts`, Phase 248), so the order
// here does not matter and nothing is left out to make a card fit. Risk
// findings only: context codes (NON_EU_JURISDICTION,
// GLEIF_REPORTING_EXCEPTION, RELATED_PEP_SUBJECT_ROLE) sit under "Structural
// context" on the results page, which a bare chip strip cannot convey —
// `exampleCards.test.ts` fails on one here.
//
// What changed on 2026-09-25: Rosneft listed 2 of its 14 codes; Ørsted gained
// STATE_CONTROLLED (Phase 240); Eli Lilly's OFFSHORE_LEAKS is medium since the
// Phase 237 gates, and RELATED_EXPORT_RISK no longer fires; Ørsted's
// RELATED_PEP is now one board member (Julia King) — the other six are
// context since Phase 247.
//
// Phase 284 (3 Oct 2026): since Phase 282 the related-party screens pick the
// same parties on every run, so codes that used to come and go are stable,
// and these cards list them as production returns them: Eli Lilly's
// RELATED_PEP is high (Katherine Baicker) and RELATED_DEBARMENT medium;
// Rosneft's RELATED_PEP is high (Igor Sechin) and RELATED_EXPORT_RISK high
// (RAO Rosneftegazstroy); Ørsted's OFFSHORE_LEAKS is medium (its related
// party "Goldman Sachs" matching ICIJ entity records "Goldman Sachs & Co"). Taqa Bratani's card is unchanged:
// Phase 284's LEI-first ranking puts Taweelah Asia Power Company back in its
// screened set.
//
// Phase 282 (3 Oct 2026): Eli Lilly's OFFSHORE_LEAKS is low. Its only ICIJ
// hit is a person match (Juan R. Luciano ↔ ROJAS JUAN LUCIANO) with nothing
// to corroborate it, and an uncorroborated ICIJ person match is low now.
//
// Phase 312 (8 Oct 2026): Equinor ASA replaces BP P.L.C. as the first card.
// Its two codes were read from production on 8 Oct 2026: STATE_CONTROLLED
// medium (Norway holds 67% through the Ministry of Trade, Industry and
// Fisheries, per OpenSanctions and Wikidata) and RELATED_PEP high (a board
// member on OpenAleph's Norway state-owned-enterprise leadership list). BP
// stays in the weekly findings regression as an anchor (its golden file
// still exercises the Phase 237 ICIJ name-match gates), just not on a card.
//
// Phase 170 removed COMPLEX_OWNERSHIP_LAYERS from BP, Rosneft, Eesti Energia
// and Ørsted: every one of those chips was a V through the subject counted as
// three layers above it. None of the six curated subjects reaches three
// layers; finding an example with a real layered chain is an open follow-up.
//
// The curated-example views ticket (16 Nov 2026): four cards, not six. Each
// card now carries a tile for every view of the report, so Equinor ASA and
// Eli Lilly and Company came off (Stephen's choice) to give the tiles room.
// Both stay in the weekly findings regression as anchors, as BP did.
export const EXAMPLE_LEIS: ExampleLei[] = [
  {
    lei: "253400JT3MQWNDKMJE44",
    name: "Rosneft",
    hint: "Russian state oil",
    signals: [
      { code: "SANCTIONED", confidence: "high" },
      { code: "RELATED_SANCTIONED", confidence: "high" },
      { code: "SANCTIONED_SECURITY", confidence: "high" },
      { code: "RELATED_SANCTIONS_CONTROLLED", confidence: "high" },
      { code: "EXPORT_CONTROLLED", confidence: "high" },
      { code: "RELATED_EXPORT_CONTROLLED", confidence: "high" },
      { code: "RELATED_EXPORT_RISK", confidence: "high" },
      { code: "DEBARMENT", confidence: "high" },
      { code: "RELATED_DEBARMENT", confidence: "high" },
      { code: "EU_HIGH_RISK_THIRD_COUNTRY", confidence: "high" },
      { code: "EU_TAX_NON_COOPERATIVE", confidence: "medium" },
      { code: "RELATED_SANCTIONS_LINKED", confidence: "high" },
      { code: "RELATED_EXPORT_CONTROL_LINKED", confidence: "high" },
      { code: "EXPORT_RISK", confidence: "medium" },
      { code: "STATE_CONTROLLED", confidence: "medium" },
      { code: "RELATED_PEP", confidence: "high" },
    ],
  },
  {
    lei: "213800E11LI1SCETU492",
    name: "Taqa Bratani Limited",
    hint: "UAE-owned UK oil & gas",
    signals: [
      { code: "RELATED_SANCTIONS_CONTROLLED", confidence: "high" },
      { code: "RELATED_EXPORT_CONTROL_LINKED", confidence: "high" },
    ],
  },
  {
    lei: "5493005044RTLQ5RZU70",
    name: "Eesti Energia AS",
    hint: "Estonian energy company",
    signals: [
      { code: "RELATED_PEP", confidence: "medium" },
      { code: "STATE_CONTROLLED", confidence: "medium" },
    ],
  },
  {
    lei: "W9NG6WMZIYEU8VEDOG48",
    name: "Ørsted A/S",
    hint: "Danish offshore energy company",
    signals: [
      { code: "RELATED_PEP", confidence: "medium" },
      { code: "STATE_CONTROLLED", confidence: "medium" },
      { code: "OFFSHORE_LEAKS", confidence: "medium" },
    ],
  },
];

// ---------------------------------------------------------------------
// Small layout primitives — design system "eyebrow" labels & dividers
// ---------------------------------------------------------------------

/**
 * Small uppercase section heading per BO design system: 10–11px,
 * weight 600, letter-spacing 0.12em, muted grey, with a hairline
 * bottom border that lines up the section visually.
 */
export function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <h2 className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted border-b border-oo-rule pb-2 mb-4">
      {children}
    </h2>
  );
}

/** A plain left click; anything else (new tab, new window) is the browser's. */
function isPlainClick(e: React.MouseEvent): boolean {
  return e.button === 0 && !e.metaKey && !e.ctrlKey && !e.shiftKey && !e.altKey;
}

/**
 * The curated examples (the curated-example views ticket, 16 Nov 2026).
 *
 * Each card names the company and its findings, then offers every view of
 * the report as a tile — the tab's own icon, name and accent, plus what the
 * view answers — so the homepage shows the six views before anyone has run
 * a check. Each tile is a real link to the report's shareable URL
 * (`exampleViewHref`), so it can be opened in a new tab or copied; a plain
 * click stays in the app and runs the lookup straight into that view.
 *
 * The tile stacks its icon above its name: side by side, "BackgroundCheck"
 * overflowed a third of a card at the page's 1100px width.
 *
 * One card per row on a phone, two from `md` up: two cards side by side on a
 * phone would leave the tile text too small to read (Stephen).
 */
export function ExampleLeiPicker({
  onPick,
  disabled,
}: {
  onPick: (lei: string, mode: CheckMode) => void;
  disabled: boolean;
}) {
  const views = EXAMPLE_VIEWS.map((id) => MODE_TABS.find((t) => t.id === id)!);
  return (
    <section className="mb-10">
      <SectionLabel>Try a curated example</SectionLabel>
      <ul className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {EXAMPLE_LEIS.map((ex) => {
          const { shown, more } = cardChips(ex.signals);
          return (
            <li
              key={ex.lei}
              className="bg-white border border-oo-rule rounded-oo p-4 sm:p-5"
              aria-labelledby={`example-${ex.lei}`}
            >
              <h3
                id={`example-${ex.lei}`}
                className="font-head text-oo-stat font-bold text-oo-ink leading-tight"
              >
                {ex.name}
              </h3>
              {ex.hint && <div className="text-oo-small text-oo-muted mt-0.5">{ex.hint}</div>}
              {shown.length > 0 && (
                <div className="flex flex-wrap items-center gap-1 mt-2">
                  {shown.map((sig) => (
                    <RiskChip
                      key={sig.code}
                      signal={{
                        code: sig.code,
                        confidence: sig.confidence,
                        source_id: "",
                        hit_id: "",
                        summary: RISK_PRESENTATION[sig.code]?.label ?? sig.code,
                        evidence: {},
                      }}
                      compact
                      interactive={false}
                    />
                  ))}
                  {more > 0 && (
                    <span className="text-oo-meta text-oo-muted">{moreFindingsLabel(more)}</span>
                  )}
                </div>
              )}
              <ul
                className="grid grid-cols-2 sm:grid-cols-3 gap-2 mt-4"
                aria-label={`Open ${ex.name} in a view`}
              >
                {views.map((view) => (
                  <li key={view.id}>
                    <a
                      href={exampleViewHref(ex.lei, view.id)}
                      aria-disabled={disabled || undefined}
                      onClick={(e) => {
                        if (!isPlainClick(e)) return;
                        e.preventDefault();
                        if (!disabled) onPick(ex.lei, view.id);
                      }}
                      className={
                        "group flex h-full flex-col items-start gap-2 rounded-oo border border-oo-rule bg-oo-bg/60 p-2.5 " +
                        "transition-colors hover:bg-white hover:shadow-oo-card focus-visible:outline focus-visible:outline-2 " +
                        "focus-visible:outline-offset-2 focus-visible:outline-oo-blue" +
                        (disabled ? " pointer-events-none opacity-50" : "")
                      }
                    >
                      <span
                        className="flex h-8 w-8 flex-none items-center justify-center rounded-md"
                        style={{ color: view.accent, backgroundColor: `${view.accent}1a` }}
                      >
                        <Icon name={view.icon} size={18} />
                      </span>
                      <span className="min-w-0 max-w-full break-words">
                        <span className="block text-oo-small font-semibold text-oo-ink leading-snug">
                          {view.label}
                        </span>
                        <span className="block text-oo-meta text-oo-muted leading-snug">
                          {EXAMPLE_VIEW_ANSWERS[view.id]}
                        </span>
                      </span>
                    </a>
                  </li>
                ))}
              </ul>
            </li>
          );
        })}
      </ul>
    </section>
  );
}

const HOW_IT_WORKS_STEPS = [
  {
    num: "1",
    accent: "#3d30d4" as const,
    icon: <StepKeyIcon className="h-[15px] w-[15px]" />,
    title: "One ID, the whole world",
    body: (
      <>
        Paste a 20-character{" "}
        <a
          href="https://www.gleif.org/en/about-lei/introducing-the-legal-entity-identifier-lei"
          target="_blank"
          rel="noreferrer"
          className="text-oo-blue underline underline-offset-2 hover:text-oo-burst"
        >
          Legal Entity Identifier
        </a>{" "}
        — the single key to 3.4 million companies worldwide.
      </>
    ),
    badges: null,
  },
  {
    num: "2",
    accent: "#3d30d4" as const,
    icon: <StepBridgeIcon className="h-[15px] w-[15px]" />,
    title: "We bridge to the registers",
    body: (
      <>
        GLEIF maps the LEI to national company numbers and{" "}
        <a
          href="https://www.gleif.org/en/newsroom/blog/transforming-data-into-opportunities-metric-of-the-month-mapping-network"
          target="_blank"
          rel="noreferrer"
          className="text-oo-blue underline underline-offset-2 hover:text-oo-burst"
        >
          cross-references
        </a>{" "}
        — so you skip the manual lookups.
      </>
    ),
    badges: null,
  },
  {
    num: "3",
    accent: "#3d30d4" as const,
    icon: <StepNetworkIcon className="h-[15px] w-[15px]" />,
    title: "72 open sources, in parallel",
    body: (
      <>
        Each source is queried with the identifier it understands, and the
        results are normalised in line with the Beneficial Ownership Data
        Standard.
      </>
    ),
    badges: null,
  },
  {
    num: "4",
    accent: "#3d30d4" as const,
    icon: <StepShieldIcon className="h-[15px] w-[15px]" />,
    title: "Risk, explained and exportable",
    body: (
      <>
        Deterministic risk signals — sanctions, flagged jurisdictions, complex
        ownership and more — track the elements due-diligence standards use to
        judge ownership complexity,
        and a plain-English AI summary explains them with every statement linked
        to its source. Take it away as an accessible PDF or raw data — or copy
        the share link, and a live summary card appears wherever you paste it.
      </>
    ),
    badges: null,
  },
] as const;

/**
 * NewFeatures — the homepage's "New features" band (Phase 344).
 *
 * It replaces BatchInvite ("Screen a list", Phase 166), which promoted one
 * workflow; batch screening keeps its section on /features. Three cards, the
 * content in `lib/newFeatures.ts`. They are built to read as a different kind
 * of thing from the curated examples above: a curated card is a white tile
 * that runs a lookup, a promo card carries an illustrated panel in its
 * feature's own token tier, a lift (`shadow-oo-card`, which the examples only
 * show on hover) and a secondary button that goes to a page. No `<button>` —
 * nothing here runs anything.
 */
export function NewFeatures() {
  return (
    <section className="mb-10">
      <SectionLabel>New features</SectionLabel>
      <ul className="grid gap-5 lg:grid-cols-3">
        {NEW_FEATURES.map((f) => {
          const tone = NEW_FEATURE_TONES[f.tone];
          return (
            <li key={f.id}>
              <article className="h-full flex flex-col bg-white border border-oo-rule rounded-oo overflow-hidden shadow-oo-card">
                <div
                  className={`flex items-center justify-center border-b h-[184px] px-4 ${tone.panel}`}
                >
                  <img
                    src={f.image}
                    alt={f.imageAlt}
                    width={320}
                    height={168}
                    loading="lazy"
                    className="max-w-full h-auto"
                  />
                </div>
                <div className="flex flex-col gap-2 p-5 flex-1">
                  <p
                    className={`text-oo-meta font-semibold uppercase tracking-oo-eyebrow ${tone.eyebrow}`}
                  >
                    New · {f.eyebrow}
                  </p>
                  <h3 className="font-head text-oo-stat font-bold text-oo-ink">
                    {f.title}
                  </h3>
                  <p className="text-oo-small text-oo-muted flex-1">{f.description}</p>
                  <a href={f.href} className={buttonClasses("secondary", "md", "self-start mt-2")}>
                    {f.cta}
                  </a>
                </div>
              </article>
            </li>
          );
        })}
      </ul>
    </section>
  );
}

export function HowItWorks() {
  return (
    <section className="mb-10 bg-white border border-oo-rule rounded-oo p-7">
      <SectionLabel>How it works</SectionLabel>
      <ol className="mt-2 max-w-2xl">
        {HOW_IT_WORKS_STEPS.map((step, i) => {
          const isLast = i === HOW_IT_WORKS_STEPS.length - 1;
          return (
            <li key={step.num} className="flex gap-5">
              {/* Left rail — circle node + connector line */}
              <div className="flex flex-col items-center flex-shrink-0" style={{ width: 28 }}>
                <div
                  className="flex items-center justify-center rounded-full text-white flex-shrink-0"
                  style={{ width: 28, height: 28, background: step.accent }}
                  aria-hidden="true"
                >
                  {step.icon}
                </div>
                {!isLast && (
                  <div
                    className="w-px flex-1 mt-1"
                    style={{ background: "#e2e5ea", minHeight: 20 }}
                  />
                )}
              </div>

              {/* Right content */}
              <div className={isLast ? "pb-0" : "pb-6"} style={{ paddingTop: 3 }}>
                <p className="font-head font-bold text-oo-body text-oo-ink leading-snug">
                  <span className="sr-only">{`Step ${step.num}: `}</span>
                  {step.title}
                </p>
                <p className="text-oo-small leading-[1.65] text-oo-muted mt-1.5">
                  {step.body}
                </p>
              </div>
            </li>
          );
        })}
      </ol>
    </section>
  );
}
