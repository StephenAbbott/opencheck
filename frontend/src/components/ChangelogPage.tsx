import statusData from "../lib/statusMarkdown.json";
import { parseStatusMarkdown, commitUrl, REPO_URL } from "../lib/changelog";

/**
 * Changelog — generated at build time from the repo's development-history table
 * (docs/status.md). An update appears here only if it earned a phase row, so the
 * status table is the editorial gate. Each card shows the phase title, the first
 * sentence of the update, and links to the commit(s) that shipped it.
 */

const ENTRIES = parseStatusMarkdown((statusData as { markdown: string }).markdown);

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
      <p className="text-[14px] leading-[1.75] text-oo-muted mb-6 max-w-2xl">
        Notable updates to OpenCheck, newest first. Generated from the project's{" "}
        <a
          href={`${REPO_URL}/blob/main/docs/status.md`}
          target="_blank"
          rel="noreferrer"
          className="underline text-oo-blue hover:text-oo-burst"
        >
          development history
        </a>{" "}
        — an item appears here only when it was significant enough to earn a phase
        entry. Each links to the commit(s) that shipped it.
      </p>

      <ol className="space-y-3">
        {ENTRIES.map((e) => (
          <li key={e.phase}>
            <article className="bg-white border border-oo-rule rounded-oo p-5">
              <div className="flex items-baseline gap-3 flex-wrap">
                <span className="font-mono text-[11px] tracking-oo-eyebrow uppercase text-oo-blue shrink-0">
                  Phase {e.phase}
                </span>
                <h3 className="font-head font-bold text-[15px] text-oo-ink leading-snug">
                  {e.title}
                </h3>
              </div>
              {e.summary && (
                <p className="text-[13px] text-oo-muted mt-1.5 leading-[1.65]">
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
                      title={`View commit ${h} on GitHub`}
                      className="font-mono text-[11px] text-oo-blue border border-oo-rule rounded px-1.5 py-0.5 hover:bg-oo-bg transition-colors"
                    >
                      {h}
                    </a>
                  ))}
                </div>
              )}
            </article>
          </li>
        ))}
      </ol>
    </section>
  );
}
