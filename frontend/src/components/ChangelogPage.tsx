import type { ReactNode } from "react";
import statusData from "../lib/statusMarkdown.json";
import {
  CHANGELOG_SHOWN,
  STATUS_URL,
  changelogView,
  commitUrl,
  parseStatusMarkdown,
} from "../lib/changelog";
import { utcDate } from "../lib/savedReport";

/**
 * Changelog — generated at build time from the repo's development-history table
 * (docs/status.md). Every merged pull request gets a phase row there: the stamp
 * workflow writes it on `main` at merge time (`.phases/pending/`, Phase 347), and
 * the next deploy rebuilds this page from it.
 *
 * The page shows the latest CHANGELOG_SHOWN phases only, under the total count
 * and the date the page was last built, and links to docs/status.md for the
 * rest (Stephen, 23 Nov 2026: no pagination — GitHub already holds the full
 * record with search and permanent links).
 *
 * Every card has the same shape whichever era its row was written in: the phase
 * number, ONE sentence in bold as the heading, the sentence after it as a
 * summary, and links to the commit(s) that shipped it (see lib/changelog.ts).
 */

const STATUS = statusData as { markdown: string; generatedAt?: string };
const VIEW = changelogView(parseStatusMarkdown(STATUS.markdown));
/** `generatedAt` is written by the prebuild hook on every deploy and by the
 * stamp when a phase merges, so it is when this page last changed. */
const UPDATED = STATUS.generatedAt ? utcDate(STATUS.generatedAt) : null;

function HistoryLink({ children }: { children: ReactNode }) {
  return (
    <a
      href={STATUS_URL}
      target="_blank"
      rel="noreferrer"
      className="underline text-oo-blue hover:text-oo-burst"
    >
      {children}
      <span className="sr-only"> (opens in new tab)</span>
    </a>
  );
}

export function ChangelogPage() {
  return (
    <section aria-labelledby="changelog-heading">
      <div className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2">
        What's new
      </div>
      <h2
        id="changelog-heading"
        className="font-head font-bold text-[clamp(1.35rem,3vw,1.8rem)] text-oo-ink mb-2 leading-tight"
      >
        Changelog
      </h2>
      <p className="text-oo-body leading-[1.75] text-oo-muted mb-3 max-w-2xl">
        What's new in OpenCheck, newest first. Each pull request is given the
        next phase number when it merges and is recorded in the project's{" "}
        <HistoryLink>development history</HistoryLink>; it appears here once
        that release is deployed. Each entry links to the commit that shipped it.
      </p>
      <p
        data-testid="changelog-facts"
        className="text-oo-small text-oo-ink mb-6 max-w-2xl"
      >
        <strong className="font-semibold">
          {VIEW.phaseCount.toLocaleString("en-GB")} phases shipped
        </strong>
        {UPDATED && <> · last updated {UPDATED}</>}
        {VIEW.hidden > 0 && <> · showing the latest {VIEW.latest.length}</>}
      </p>

      <ol className="space-y-3">
        {VIEW.latest.map((e, i) => (
          <li key={e.phase}>
            <article className="bg-white border border-oo-rule rounded-oo p-5">
              <div className="flex items-center gap-2">
                <span className="font-mono text-oo-meta tracking-oo-eyebrow uppercase text-oo-blue">
                  Phase {e.phase}
                </span>
                {i === 0 && (
                  <span className="text-oo-meta font-semibold tracking-oo-eyebrow uppercase text-oo-ink bg-oo-bg border border-oo-rule rounded px-1.5 py-px leading-none">
                    Latest
                  </span>
                )}
              </div>
              {/* One sentence, always: the row's bold headline (or, on a
                  pre-Phase-106 row, the short title before its em-dash).
                  Before, the heading ran to the row's first em-dash, so a
                  stamped row with a dash deep in its narrative showed a
                  paragraph in bold and a row without one showed no summary. */}
              <h3 className="font-head font-bold text-oo-lead text-oo-ink leading-snug mt-1 [overflow-wrap:anywhere]">
                {e.title}
              </h3>
              {/* Like the heading above: a status row can carry a long unbreakable
                  token (a filename, an identifier), and at 320px the summary
                  otherwise scrolls the page sideways — row 356's
                  `ettevotja_rekvisiidid__kandevalised_isikud.json.zip` did. */}
              {e.summary && (
                <p className="text-oo-small text-oo-muted mt-1.5 leading-[1.65] [overflow-wrap:anywhere]">
                  {e.summary}
                </p>
              )}
              {e.commits.length > 0 && (
                <div className="mt-2.5 flex flex-wrap items-center gap-1.5">
                  {e.commits.map((h) => (
                    <a
                      key={h}
                      href={commitUrl(h)}
                      target="_blank"
                      rel="noreferrer"
                      className="font-mono text-oo-meta text-oo-blue border border-oo-rule rounded px-1.5 py-0.5 hover:bg-oo-bg transition-colors"
                    >
                      {h}
                      {/* A bare 7-character hash is not a link name, and this
                          was the one new-tab link in the app without the
                          convention's "(opens in new tab)". */}
                      <span className="sr-only">
                        {" "}— view this commit on GitHub (opens in new tab)
                      </span>
                    </a>
                  ))}
                </div>
              )}
            </article>
          </li>
        ))}
      </ol>

      {VIEW.hidden > 0 && (
        <p className="text-oo-body text-oo-muted mt-6 max-w-2xl">
          That is the latest {CHANGELOG_SHOWN}.{" "}
          <HistoryLink>
            Read all {VIEW.phaseCount.toLocaleString("en-GB")} phases in the
            full development history on GitHub
          </HistoryLink>
          .
        </p>
      )}
    </section>
  );
}
