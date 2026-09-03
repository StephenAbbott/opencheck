/**
 * The /sources catalogue — and, since Phase 161, whether each source is
 * answering.
 *
 * Extracted from App.tsx (where it had lived since Phase 8) because this
 * phase touches every line of it, and a view that App.tsx only mounts has no
 * business adding to its 4,100 lines. Arbitrary text sizes were migrated to
 * the named scale on the way: a new file may not carry any.
 *
 * What changed for the reader. "Supports: entity · live ready" was the only
 * status a card had, and *live ready* is configuration — a key is set — not
 * health: a source can be live-ready and refuse every request for a month,
 * and the reader who arrived from a report saying "the register did not
 * answer" learned nothing here. The weekly sweep has known the difference
 * since Phase 121; it wrote its verdict to a CI artifact and a GitHub issue.
 * The page now reads that verdict back (`/source-health`):
 *
 * - **A summary strip** under the intro: when the sweep ran and how many
 *   sources were healthy, degraded, failed and not tested.
 * - **A status line on every card** — the sweep's word, in its tone, and when
 *   it was checked — plus a *Health details* disclosure with freshness,
 *   latency, the statement count and the last eight sweeps as dots. A card
 *   with something to explain (degraded, failed, not tested, a known gap, a
 *   statement collapse) opens its details unasked; a healthy one costs a
 *   single line. This is design option C of the three drafted on 3 September
 *   2026: everything on one page, but the healthy majority stays quiet.
 *
 * When no sweep has published (the API says `available: false`) the page
 * renders exactly as it did before: the strip and the status lines are
 * absent, not "unknown". Absence of a verdict is not a verdict.
 */

import { useQuery } from "@tanstack/react-query";
import { useId, useState } from "react";

import { fetchSourceHealth, type SourceInfo } from "../lib/api";
import {
  cardHealth,
  healthSummary,
  STATUS_DOT,
  STATUS_WORD,
  type CardHealth,
} from "../lib/sourceHealth";
import { Chip, SectionLabel } from "./ui";

/** A note is set in its status's text colour — the same hue as the chip. */
const NOTE_COLOUR: Record<CardHealth["status"], string> = {
  ok: "text-oo-ink",
  degraded: "text-oo-info-text",
  fail: "text-oo-warn-text",
  skipped: "text-oo-muted",
};

const SWEEP_URL = "https://github.com/StephenAbbott/opencheck/actions/workflows/source-health.yml";

// LicenseChip is also defined in SourceBucketCard for use inside DeepenBlock;
// this copy is the catalogue's.
function LicenseChip({ license }: { license: string }) {
  const nc = license.toLowerCase().includes("nc");
  return (
    <Chip tone={nc ? "warn" : "ok"} className="font-mono">
      {license}
    </Chip>
  );
}

export function SourcesPage({
  sources,
  loading,
}: {
  sources: SourceInfo[] | undefined;
  loading: boolean;
}) {
  const healthQuery = useQuery({
    queryKey: ["source-health"],
    queryFn: () => fetchSourceHealth(),
    staleTime: 60 * 60 * 1000,
  });
  const health = healthQuery.data;
  const summary = healthSummary(health);

  return (
    <section>
      <SectionLabel as="h2" className="border-b border-oo-rule pb-2 mb-4">
        About the sources
      </SectionLabel>
      <p className="text-oo-body leading-relaxed text-oo-muted mb-6 max-w-2xl">
        OpenCheck queries the open data sources below. GLEIF is the entry point
        — the LEI acts as a connector across the rest. Each source ships its
        data under its own license; non-commercial sources propagate that
        obligation through the export bundle.
      </p>

      {summary && (
        <div className="bg-white border border-oo-rule rounded-oo px-5 py-3 mb-6 flex flex-wrap items-center gap-x-5 gap-y-2">
          <SectionLabel as="p" className="mr-1">
            Last sweep · {summary.sweptAt}
          </SectionLabel>
          <dl className="contents">
            {(["ok", "degraded", "fail", "skipped"] as const).map((status) => (
              <div key={status} className="flex items-baseline gap-1.5">
                <span aria-hidden="true" className={`inline-block w-2 h-2 rounded-full ${STATUS_DOT[status]}`} />
                <dd className="font-head font-bold text-oo-head text-oo-ink m-0">{summary.counts[status]}</dd>
                <dt className="text-oo-small text-oo-muted">{STATUS_WORD[status].toLowerCase()}</dt>
              </div>
            ))}
          </dl>
          <a
            href={SWEEP_URL}
            target="_blank"
            rel="noreferrer"
            className="ml-auto text-oo-small font-semibold text-oo-blue hover:underline underline-offset-2"
          >
            Weekly sweep on GitHub →
          </a>
          {summary.staleNote && (
            <p className="basis-full m-0 text-oo-meta text-oo-muted">{summary.staleNote}</p>
          )}
        </div>
      )}

      {loading && <p className="text-oo-muted">Loading…</p>}
      {sources && (
        <ul
          className="grid gap-6"
          // 480px min as per the BO design library card grid spec.
          style={{ gridTemplateColumns: "repeat(auto-fill, minmax(min(100%, 480px), 1fr))" }}
        >
          {[...sources]
            .sort((a, b) => a.name.localeCompare(b.name))
            .map((s, i) => (
              <li
                key={s.id}
                className={`bg-white border rounded-oo p-6 text-oo-body transition-shadow hover:shadow-oo-card ${
                  s.category === "esg" ? "border-emerald-200" : "border-oo-rule"
                }`}
              >
                <div className="flex items-baseline gap-3 mb-1 flex-wrap">
                  <span className="font-mono text-oo-meta tracking-wider text-oo-blue">
                    {String(i + 1).padStart(2, "0")}
                  </span>
                  <a
                    href={s.homepage}
                    target="_blank"
                    rel="noreferrer"
                    className="font-head text-oo-head font-bold text-oo-ink leading-tight hover:underline underline-offset-2"
                  >
                    {s.name}
                  </a>
                  <div className="ml-auto flex items-center gap-2">
                    {s.category === "esg" && (
                      <Chip tone="ok" className="uppercase tracking-wide font-semibold">
                        ESG
                      </Chip>
                    )}
                    <LicenseChip license={s.license} />
                  </div>
                </div>
                {s.description && (
                  <p className="text-oo-small leading-relaxed text-oo-muted mt-2">{s.description}</p>
                )}
                <p className="text-oo-meta font-mono mt-3 text-oo-muted">
                  Supports: {s.supports.join(", ")} ·{" "}
                  {s.live_available ? "live ready" : "placeholder data"}
                </p>
                <CardStatus health={cardHealth(s.id, health, s.name)} />
              </li>
            ))}
        </ul>
      )}
    </section>
  );
}

/**
 * The status line and its disclosure. Renders nothing for a source the
 * sweep does not know — not "unknown", nothing: on a page where every other
 * card carries a verdict, a blank is the honest shape for "no verdict".
 */
function CardStatus({ health }: { health: CardHealth | null }) {
  const [open, setOpen] = useState<boolean | null>(null);
  const panelId = useId();
  if (!health) return null;
  const isOpen = open ?? health.openByDefault;
  return (
    <div className="mt-4 pt-3 border-t border-oo-rule">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
        <Chip tone={health.tone} className="font-semibold">
          <span aria-hidden="true" className={`inline-block w-1.5 h-1.5 rounded-full ${STATUS_DOT[health.status]}`} />
          {health.word}
        </Chip>
        <span className="text-oo-meta text-oo-muted">{health.checked}</span>
        <button
          type="button"
          aria-expanded={isOpen}
          aria-controls={panelId}
          onClick={() => setOpen(!isOpen)}
          className="ml-auto inline-flex items-center gap-1 text-oo-small font-semibold text-oo-blue hover:underline underline-offset-2"
        >
          Health details
          <svg
            width="12"
            height="12"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2.5"
            strokeLinecap="round"
            strokeLinejoin="round"
            aria-hidden="true"
            className={isOpen ? "rotate-180" : ""}
          >
            <path d="m6 9 6 6 6-6" />
          </svg>
        </button>
      </div>
      <div id={panelId} hidden={!isOpen} className="mt-3 rounded-oo border border-oo-rule bg-oo-bg px-4 py-3">
        <dl className="grid gap-x-6 gap-y-1 text-oo-meta sm:grid-cols-2 m-0">
          <div className="flex items-baseline gap-2">
            <dt className="text-oo-muted whitespace-nowrap">Last {health.history.length === 1 ? "sweep" : `${health.history.length} sweeps`}</dt>
            <dd className="m-0 flex items-center gap-1" aria-label={health.history.map((h) => STATUS_WORD[h]).join(", ")}>
              {health.history.map((h, i) => (
                <span key={i} aria-hidden="true" className={`inline-block w-2 h-2 rounded-full ${STATUS_DOT[h]}`} />
              ))}
            </dd>
          </div>
          {health.rows.map((r) => (
            <div key={r.label} className="flex items-baseline gap-2">
              <dt className="text-oo-muted whitespace-nowrap">{r.label}</dt>
              <dd className="m-0 text-oo-ink">{r.value}</dd>
            </div>
          ))}
        </dl>
        {health.notes.map((note) => (
          <p key={note} className={`mt-2 mb-0 text-oo-meta ${NOTE_COLOUR[health.status]}`}>
            {note}
          </p>
        ))}
      </div>
    </div>
  );
}
