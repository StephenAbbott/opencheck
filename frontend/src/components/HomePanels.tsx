/**
 * The homepage panels below the search — examples, the list invitation, how it works.
 *
 * Lifted out of `App.tsx` in Phase 168, unchanged. It had no business being
 * there: they are what the homepage *is*, and none of them reads a single piece
 * of App's state: the picker takes an `onPick`, BatchInvite an `onOpen`,
 * How It Works nothing at all.
 */

import {
  StepBridgeIcon,
  StepKeyIcon,
  StepNetworkIcon,
  StepShieldIcon,
} from "./icons";
import { RiskChip, RISK_PRESENTATION } from "./risk/RiskChip";
import { buttonClasses } from "./ui";
import { cardChips, moreFindingsLabel } from "../lib/exampleCards";

/**
 * Curated demo subjects that have a pre-extracted Open Ownership BODS
 * bundle on disk (``data/cache/bods_data/``) — clicking any of them
 * resolves entirely offline. The list is small + opinionated; users
 * can paste any other LEI into the input.
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
// OUTPUT and nothing fails when they drift — verify against production
// (`/lookup?lei=…&refresh=true` on api.opencheck.world), never by reading
// the card back.
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
// Phase 170 removed COMPLEX_OWNERSHIP_LAYERS from BP, Rosneft, Eesti Energia
// and Ørsted: every one of those chips was a V through the subject counted as
// three layers above it. None of the six curated subjects reaches three
// layers; finding an example with a real layered chain is an open follow-up.
export const EXAMPLE_LEIS: ExampleLei[] = [
  {
    lei: "213800LH1BZH3DI6G760",
    name: "BP P.L.C.",
    hint: "UK oil major",
    signals: [
      { code: "OFFSHORE_LEAKS", confidence: "high" },
    ],
  },
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
      { code: "DEBARMENT", confidence: "high" },
      { code: "RELATED_DEBARMENT", confidence: "high" },
      { code: "EU_HIGH_RISK_THIRD_COUNTRY", confidence: "high" },
      { code: "RELATED_SANCTIONS_LINKED", confidence: "high" },
      { code: "RELATED_EXPORT_CONTROL_LINKED", confidence: "high" },
      { code: "EXPORT_RISK", confidence: "medium" },
      { code: "STATE_CONTROLLED", confidence: "medium" },
      { code: "RELATED_PEP", confidence: "medium" },
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
    ],
  },
  {
    lei: "FRDRIPF3EKNDJ2CQJL29",
    name: "Eli Lilly and Company",
    hint: "American pharmaceutical giant",
    signals: [
      { code: "RELATED_PEP", confidence: "medium" },
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

export function ExampleLeiPicker({
  onPick,
  disabled,
}: {
  onPick: (lei: string) => void;
  disabled: boolean;
}) {
  return (
    <section className="mb-10">
      <SectionLabel>Try a curated example</SectionLabel>
      <ul
        className="grid gap-3"
        // 280px min keeps three subjects per row at desktop widths,
        // stacks on narrow viewports.
        style={{ gridTemplateColumns: "repeat(auto-fill, minmax(min(100%, 280px), 1fr))" }}
      >
        {EXAMPLE_LEIS.map((ex) => (
          <li key={ex.lei} className="relative">
            <button
              type="button"
              disabled={disabled}
              onClick={() => onPick(ex.lei)}
              className="w-full text-left bg-white border border-oo-rule rounded-oo p-4 transition-shadow hover:shadow-oo-card disabled:opacity-50"
            >
              <div className="font-head text-oo-body font-bold text-oo-ink leading-tight pr-6">
                {ex.name}
              </div>
              {ex.hint && (
                <div className="text-oo-meta text-oo-muted mt-0.5">
                  {ex.hint}
                </div>
              )}
              {ex.signals && ex.signals.length > 0 && (() => {
                const { shown, more } = cardChips(ex.signals);
                return (
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
                    <span className="text-oo-meta text-oo-muted">
                      {moreFindingsLabel(more)}
                    </span>
                  )}
                </div>
                );
              })()}
            </button>
            {/* Phase 122: the Neo4j CSV download that used to sit here came
                off. It is a developer affordance on the app's headline entry
                point — an icon-only link, explained by a `title` no keyboard
                or touch user can read, offering a graph-database bundle to
                someone who has not yet run their first check. The same
                download lives in the export panel, where it is one of nine
                formats and is labelled. */}
          </li>
        ))}
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
        — the single key to 3 million+ entities worldwide.
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
    title: "49 open sources, in parallel",
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
        ownership and more — follow the EU AMLA's draft due-diligence standards,
        and a plain-English AI summary explains them with every statement linked
        to its source. Take it away as an accessible PDF or raw data — or copy
        the share link, and a live summary card appears wherever you paste it.
      </>
    ),
    badges: null,
  },
] as const;

/**
 * BatchInvite — the homepage's invitation to screen a list (Phase 166).
 *
 * It replaces ShareCardShowcase, the preview of the shareable summary card.
 * The share link is still advertised — one sentence in the last "How it
 * works" step — but the homepage's second section now promotes the thing a
 * compliance officer, a journalist or an EITI secretariat actually arrives
 * with: a list. Nothing in it is a claim about the engine, so nothing here
 * can go stale the way the rendered card could (see the hardcoded-claims
 * note in CLAUDE.md).
 */
export function BatchInvite({ onOpen }: { onOpen: () => void }) {
  return (
    <section className="mb-10 bg-white border border-oo-rule rounded-oo p-7">
      <SectionLabel>Screen a list</SectionLabel>
      <div className="mt-2 flex flex-col md:flex-row md:items-center gap-5">
        <p className="text-oo-small text-oo-muted md:max-w-xl">
          Arrived with a counterparty list, a portfolio, or every licence-holder in a
          register? Paste up to 20 LEIs and get{" "}
          <span className="text-oo-ink font-medium">one table</span> — register status, the
          verdict sentence and the signal count for each company, every row linking to its
          full report — and{" "}
          <span className="text-oo-ink font-medium">one file</span> to take away.
        </p>
        <a
          href="/batch"
          onClick={(e) => {
            e.preventDefault();
            onOpen();
          }}
          className={buttonClasses("primary", "md", "shrink-0 self-start md:self-center")}
        >
          Screen a list of companies
        </a>
      </div>
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
