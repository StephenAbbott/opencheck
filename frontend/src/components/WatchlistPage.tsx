/**
 * `/watchlist` — the companies this browser watches, and what changed
 * (Phase 215).
 *
 * The page is the front of a delta-driven loop, and it says so: a company
 * on the list is re-checked only when GLEIF's Golden Copy delta,
 * OpenSanctions' entity delta or (for an Estonian company) the daily diff of
 * the e-Business Register's open data names it. Nothing here polls a
 * register. The
 * honesty line under the list (`tierSentence`) states what each tier is
 * actually doing on this instance — the mirror's watermark and how many
 * records the last delta changed — so "3.4 million LEI records; 16,000
 * changed today; one of them was yours" is a description, not a slogan.
 *
 * The token comes from `?token=` (a shared or bookmarked link) or from this
 * browser's storage; a URL token is kept for next time. It is a capability:
 * the page says so beside the feed address. A held token the instance no
 * longer knows is forgotten and the page says so; a URL token it does not
 * know is an error with a retry, since the link may simply be wrong.
 *
 * Three surfaces: the watched companies (baseline, last check, re-check
 * now, remove); the log, newest first, each entry opening with the tier
 * that fired and closing with how many sources were reached; and the Atom
 * address with a copy button.
 */

import { useCallback, useEffect, useMemo, useState } from "react";

import { getWatchlist, isUnknownWatchlist, recheckWatch, removeWatch, setWatchContext } from "../lib/api";
import { trackEvent } from "../lib/analytics";
import { sourceLabel } from "../lib/vocab";
import {
  checkedSentence,
  CONTEXT_FIELD_LABELS,
  CONTEXT_FORM_NOTE,
  describeChange,
  disagreementSentence,
  expectationSentence,
  hasContext,
  RATIONALE_MAX,
  reportIdFromInput,
  reviewChip,
  gleifLogLines,
  entryHeadline,
  registerChangeLines,
  feedHelp,
  filterEntries,
  filterFromLocation,
  forgetToken,
  GRADED_TODAY_NOTE,
  hiddenByFilterSentence,
  materialityChip,
  MATERIALITY_LABELS,
  MATERIALITY_SENTENCES,
  MATERIALITY_TIERS,
  readToken,
  sortEntries,
  storeFilter,
  storeToken,
  tierChip,
  type ChangeLogFilter,
  tierSentence,
  tokenFromLocation,
  triggerSentence,
  UNKNOWN_LIST_PAGE_NOTICE,
  type Watch,
  type WatchContextPatch,
  type WatchEntry,
  type WatchlistPayload,
} from "../lib/watchlist";
import { Button, Chip, SectionLabel, sectionLabelClasses } from "./ui";
import { RISK_PRESENTATION } from "./risk/RiskChip";

type Phase = "loading" | "ready" | "none" | "error";

function day(iso: string | null | undefined): string {
  return iso ? iso.slice(0, 10) : "—";
}

export default function WatchlistPage({
  sourceNames,
  onOpen,
}: {
  sourceNames?: Record<string, string>;
  onOpen?: (lei: string) => void;
}) {
  const [token, setToken] = useState<string | null>(() => tokenFromLocation(window.location.search));
  const [data, setData] = useState<WatchlistPayload | null>(null);
  const [phase, setPhase] = useState<Phase>("loading");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const [filter, setFilter] = useState<ChangeLogFilter>(() => filterFromLocation(window.location.search));
  const chooseFilter = (next: ChangeLogFilter) => {
    setFilter(next);
    storeFilter(next);
    const url = new URL(window.location.href);
    if (next === "material") url.searchParams.set("show", "material");
    else url.searchParams.delete("show");
    window.history.replaceState(null, "", url.toString());
  };

  const label = useCallback((id: string) => sourceLabel(id, sourceNames), [sourceNames]);

  useEffect(() => {
    if (!token) {
      setPhase("none");
      return;
    }
    let cancelled = false;
    getWatchlist(token)
      .then((d) => {
        if (cancelled) return;
        storeToken(token);
        setData(d);
        setPhase("ready");
      })
      .catch((err) => {
        if (cancelled) return;
        const fromUrl = new URLSearchParams(window.location.search).get("token")?.trim() === token;
        if (!fromUrl && readToken() === token && isUnknownWatchlist(err)) {
          // The browser's own token names no list here: drop it rather
          // than carry a dead capability, and say so.
          forgetToken();
          setError(UNKNOWN_LIST_PAGE_NOTICE);
          setPhase("none");
          return;
        }
        setError(err instanceof Error ? err.message : "Could not open the watchlist.");
        setPhase(readToken() === token ? "error" : "none");
      });
    return () => {
      cancelled = true;
    };
  }, [token]);

  const recheck = async (lei: string) => {
    if (!token) return;
    setBusy(lei);
    setNotice(null);
    setError(null);
    try {
      const res = await recheckWatch(token, lei);
      trackEvent("watch_recheck");
      setData(res);
      const n = res.result.changes.length;
      setNotice(
        n
          ? `${res.result.legal_name || lei}: ${n} change${n === 1 ? "" : "s"} found — see the log.`
          : `${res.result.legal_name || lei}: re-checked just now, nothing changed.`,
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : "The re-check could not run.");
    } finally {
      setBusy(null);
    }
  };

  const remove = async (lei: string) => {
    if (!token) return;
    setBusy(lei);
    setError(null);
    try {
      setData(await removeWatch(token, lei));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not remove this company.");
    } finally {
      setBusy(null);
    }
  };

  // pKYC stage B: what the watcher expected. The backend validates and
  // compares; a refusal names the field and is shown inside the form.
  const saveContext = async (lei: string, patch: WatchContextPatch): Promise<string | null> => {
    if (!token) return "No watchlist is open.";
    setBusy(lei);
    try {
      const res = await setWatchContext(token, lei, patch);
      trackEvent("watch_context");
      setData(res);
      return null;
    } catch (err) {
      return err instanceof Error ? err.message : "Could not save the context.";
    } finally {
      setBusy(null);
    }
  };

  const copyFeed = () => {
    if (!data) return;
    navigator.clipboard?.writeText(data.feed_url);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1500);
  };

  const allEntries = useMemo(() => (data ? sortEntries(data.entries) : []), [data]);
  const entries = useMemo(() => filterEntries(allEntries, filter), [allEntries, filter]);
  const hiddenNote = hiddenByFilterSentence(allEntries.length, entries.length);
  const honesty = useMemo(() => tierSentence(data?.tiers ?? null), [data]);

  return (
    <div className="max-w-oo-page mx-auto">
      <section className="mb-6 bg-white border border-oo-rule rounded-oo p-7">
        <h1 className={sectionLabelClasses("muted")}>Watchlist</h1>
        <p className="mt-2 max-w-2xl text-oo-small text-oo-muted">
          The companies this browser watches. Each is re-checked only when GLEIF or OpenSanctions
          publish a change to its record, or — for an Estonian company — when the e-Business
          Register's daily data shows one; the re-run then fetches every source a normal check
          would, and what differed is logged below and in the feed. No account, no email.
        </p>

        {phase === "none" && (
          <p className="mt-4 text-oo-small text-oo-ink" data-testid="watchlist-empty">
            Nothing is watched yet. Open any company&apos;s report and press{" "}
            <span className="font-medium">Watch for changes</span>.
            {error && <span className="block mt-1 text-oo-warn-text">{error}</span>}
          </p>
        )}
        {phase === "loading" && (
          <p className="mt-4 text-oo-meta text-oo-muted" role="status">
            Opening the watchlist…
          </p>
        )}
        {phase === "error" && (
          <div role="alert" className="mt-4 rounded-oo border border-oo-warn-border bg-oo-warn-bg px-4 py-3 text-oo-small text-oo-warn-text">
            {error}
            <Button variant="secondary" size="sm" className="ml-3" onClick={() => setToken(readToken())}>
              Try again
            </Button>
          </div>
        )}

        {data && (
          <>
            <div className="mt-4 flex flex-wrap items-center gap-3">
              <code className="font-mono text-oo-meta text-oo-ink break-all" data-testid="feed-url">
                {data.feed_url}
              </code>
              <Button variant="secondary" size="sm" onClick={copyFeed}>
                {copied ? "Copied" : "Copy feed address"}
              </Button>
              <a href={data.feed_url} className="text-oo-meta text-oo-blue underline-offset-2 hover:underline">
                Open the Atom feed
              </a>
            </div>
            <p className="mt-1 text-oo-meta text-oo-muted">{feedHelp()}</p>
            <span role="status" className="sr-only">
              {copied ? "Feed address copied" : ""}
            </span>
          </>
        )}
      </section>

      {error && phase === "ready" && (
        <div role="alert" className="mb-6 rounded-oo border border-oo-warn-border bg-oo-warn-bg px-4 py-3 text-oo-small text-oo-warn-text">
          {error}
        </div>
      )}
      {notice && (
        <p role="status" className="mb-6 rounded-oo border border-oo-rule bg-white px-4 py-3 text-oo-small text-oo-ink">
          {notice}
        </p>
      )}

      {data && (
        <section className="mb-6 bg-white border border-oo-rule rounded-oo p-7">
          <div className="flex flex-wrap items-baseline justify-between gap-2">
            <SectionLabel as="h2">Watched companies</SectionLabel>
            <p className="text-oo-meta text-oo-muted" role="status">
              {data.caps.in_list} of {data.caps.per_list} on this list
            </p>
          </div>
          {data.review?.sentence && (
            <p className="mt-2 text-oo-small text-oo-warn-text" data-testid="review-due-line">
              {data.review.sentence}
            </p>
          )}
          {data.watches.length === 0 ? (
            <p className="mt-3 text-oo-small text-oo-muted">
              This list is empty. Open any company&apos;s report and press Watch for changes.
            </p>
          ) : (
            <ul className="mt-4 divide-y divide-oo-rule" aria-label="Watched companies">
              {data.watches.map((w) => (
                <li key={w.lei} className="py-4 first:pt-0 last:pb-0">
                  <WatchRow
                    w={w}
                    busy={busy === w.lei}
                    label={label}
                    onOpen={onOpen}
                    onRecheck={() => recheck(w.lei)}
                    onRemove={() => remove(w.lei)}
                    onSaveContext={(patch) => saveContext(w.lei, patch)}
                  />
                </li>
              ))}
            </ul>
          )}
          <p className="mt-5 text-oo-meta text-oo-muted" data-testid="tier-sentence">
            {honesty}
          </p>
        </section>
      )}

      {data && (
        <section className="bg-white border border-oo-rule rounded-oo p-7">
          <SectionLabel as="h2">What changed</SectionLabel>
          {allEntries.length > 0 && (
            <div className="mt-3 flex flex-wrap items-center gap-2" role="group" aria-label="Show">
              <span className="text-oo-meta text-oo-muted">Show:</span>
              <Button
                variant={filter === "all" ? "primary" : "secondary"}
                size="sm"
                aria-pressed={filter === "all"}
                onClick={() => chooseFilter("all")}
              >
                All
              </Button>
              <Button
                variant={filter === "material" ? "primary" : "secondary"}
                size="sm"
                aria-pressed={filter === "material"}
                onClick={() => chooseFilter("material")}
              >
                Material only
              </Button>
              <span className="text-oo-meta text-oo-muted">
                Every entry carries a tier — {MATERIALITY_TIERS.map((t) => MATERIALITY_LABELS[t]).join(" · ")}.
                Material = the last two.
              </span>
            </div>
          )}
          {hiddenNote && (
            <p className="mt-2 text-oo-meta text-oo-muted" data-testid="log-hidden">
              {hiddenNote}
            </p>
          )}
          {allEntries.length === 0 ? (
            <p className="mt-3 text-oo-small text-oo-muted" data-testid="log-empty">
              Nothing yet. An entry appears here, and in the feed, when GLEIF or OpenSanctions
              publish a change to a watched company and the re-run finds a difference, or when the
              Estonian e-Business Register records one.
            </p>
          ) : entries.length === 0 ? (
            <p className="mt-3 text-oo-small text-oo-muted" data-testid="log-all-note">
              Every entry so far is note only — recorded for the history, nothing to act on by itself.
            </p>
          ) : (
            <ol className="mt-4 space-y-5" aria-label="Change log, newest first">
              {entries.map((e) => (
                <li key={e.id} className="rounded-oo border border-oo-rule p-4">
                  <EntryView e={e} label={label} onOpen={onOpen} />
                </li>
              ))}
            </ol>
          )}
        </section>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------

function WatchRow({
  w,
  busy,
  label,
  onOpen,
  onRecheck,
  onRemove,
  onSaveContext,
}: {
  w: Watch;
  busy: boolean;
  label: (id: string) => string;
  onOpen?: (lei: string) => void;
  onRecheck: () => void;
  onRemove: () => void;
  onSaveContext: (patch: WatchContextPatch) => Promise<string | null>;
}) {
  const b = w.baseline;
  const status = b.register_status;
  const gleifStatus = w.gleif_facts?.registration_status ?? null;
  const review = reviewChip(w.review);
  return (
    <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
      <div className="min-w-0">
        <a
          href={`/?lei=${w.lei}`}
          onClick={(e) => {
            if (!onOpen) return;
            e.preventDefault();
            onOpen(w.lei);
          }}
          className="font-head font-bold text-oo-ink underline-offset-2 hover:underline hover:text-oo-blue"
        >
          {w.legal_name || w.lei}
        </a>
        <p className="mt-0.5 font-mono text-oo-meta text-oo-muted break-all">
          {w.jurisdiction ? `${w.jurisdiction} · ` : ""}LEI {w.lei}
        </p>
        <div className="mt-2 flex flex-wrap items-center gap-1.5">
          {status?.liveness && (
            <Chip tone={status.liveness === "live" ? "ok" : status.liveness === "terminal" ? "warn" : "neutral"} size="sm">
              {status.liveness === "live"
                ? "Active"
                : status.liveness === "terminal"
                  ? "Dissolved"
                  : status.liveness === "declared"
                    ? "Last declared active"
                    : status.liveness}
              {status.source_id ? ` · ${label(status.source_id)}` : ""}
            </Chip>
          )}
          {gleifStatus && (
            <Chip tone="neutral" size="sm">
              LEI {gleifStatus.toLowerCase()}
            </Chip>
          )}
          {b.risk_codes.map((code) => (
            <Chip key={code} tone="risk" size="sm">
              {RISK_PRESENTATION[code]?.label ?? code}
            </Chip>
          ))}
          {b.coverage && (
            <Chip tone={b.degraded_sources.length ? "warn" : "neutral"} size="sm">
              {b.coverage.answered} of {b.coverage.applicable} sources
            </Chip>
          )}
          {review && (
            <Chip tone={review.tone} size="sm" data-testid="review-due-chip">
              {review.label}
            </Chip>
          )}
        </div>
        <p className="mt-2 text-oo-meta text-oo-muted">
          Watching since {day(w.added_at)} · last checked {day(w.last_checked_at)}
          {w.gleif_watermark ? ` · GLEIF record as of ${w.gleif_watermark.slice(0, 10)}` : ""}
        </p>
        {w.review && (
          <p
            className={`mt-1 text-oo-meta ${w.review.due ? "text-oo-warn-text" : "text-oo-muted"}`}
            data-testid="review-sentence"
          >
            {w.review.sentence}
          </p>
        )}
        {w.gleif_history && (
          <details className="mt-1 text-oo-meta text-oo-muted">
            <summary className="cursor-pointer hover:text-oo-ink">
              GLEIF's log in the 30 days before you started watching
              {w.gleif_history.available ? ` (${w.gleif_history.items.length + (w.gleif_history.more || 0)})` : ""}
            </summary>
            <GleifLogBlock lines={gleifLogLines(w.gleif_history, "GLEIF's log")} />
          </details>
        )}
        <WatchContextBlock w={w} busy={busy} onSave={onSaveContext} />
      </div>
      <div className="flex shrink-0 gap-2 md:flex-col md:items-end">
        <Button variant="warn" size="sm" onClick={onRecheck} disabled={busy}>
          {busy ? "Checking…" : "Re-check now"}
        </Button>
        <Button variant="ghost" size="sm" onClick={onRemove} disabled={busy}>
          Stop watching
        </Button>
      </div>
    </div>
  );
}

function EntryView({
  e,
  label,
  onOpen,
}: {
  e: WatchEntry;
  label: (id: string) => string;
  onOpen?: (lei: string) => void;
}) {
  const checked = checkedSentence(e.checked);
  const degraded = Array.from(new Set(e.degraded.map((d) => d.source_id))).filter(Boolean);
  const chip = tierChip(e.tier);
  const materiality = materialityChip(e.materiality);
  const registerLines = e.tier === "register" ? registerChangeLines(e.trigger?.changes) : [];
  return (
    <div>
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        {e.tier === "review" ? (
          // pKYC stage C: an entry about the list, not a company — nothing to open.
          <span className="font-head font-bold text-oo-ink">{entryHeadline(e)}</span>
        ) : (
          <a
            href={`/?lei=${e.lei}`}
            onClick={(ev) => {
              if (!onOpen) return;
              ev.preventDefault();
              onOpen(e.lei);
            }}
            className="font-head font-bold text-oo-ink underline-offset-2 hover:underline hover:text-oo-blue"
          >
            {entryHeadline(e)}
          </a>
        )}
        <span className="text-oo-meta text-oo-muted">
          {materiality && (
            <>
              <Chip tone={materiality.tone} size="sm" data-testid="materiality-chip">
                {materiality.label}
              </Chip>{" "}
            </>
          )}
          <Chip tone={chip.tone} size="sm">
            {chip.label}
          </Chip>{" "}
          {e.created_at.replace("T", " ").slice(0, 16)} UTC
        </span>
      </div>
      {e.materiality && (
        <p className="mt-1 text-oo-meta text-oo-muted">
          {MATERIALITY_SENTENCES[e.materiality]}
          {e.materiality_rules == null ? ` ${GRADED_TODAY_NOTE}` : ""}
        </p>
      )}
      <p className="mt-2 text-oo-small text-oo-ink">{triggerSentence(e)}</p>
      {registerLines.length > 0 && (
        <ul className="mt-2 space-y-1 text-oo-small text-oo-ink" aria-label="What the register recorded">
          {registerLines.map((line) => (
            <li key={line} className="flex gap-2">
              <span aria-hidden="true" className="text-oo-muted">
                –
              </span>
              <span>{line}</span>
            </li>
          ))}
        </ul>
      )}
      {e.changes.length > 0 ? (
        <ul className="mt-2 space-y-1 text-oo-small text-oo-ink" aria-label="What the re-run found">
          {e.changes.map((c, i) => (
            <li key={`${c.kind}-${i}`} className="flex gap-2">
              <span aria-hidden="true" className="text-oo-muted">
                –
              </span>
              <span className={c.kind.endsWith("_unchecked") ? "text-oo-warn-text" : ""}>
                {describeChange(c, label)}
                {(() => {
                  const exp = expectationSentence(c);
                  return exp ? (
                    <span
                      className={exp.tone === "warn" ? "text-oo-warn-text" : "text-oo-ok-text"}
                      data-testid="expectation-sentence"
                    >
                      {" "}
                      {exp.text}
                    </span>
                  ) : null;
                })()}
              </span>
              {c.materiality && c.materiality !== "note" && (
                <Chip tone={c.materiality === "escalate" ? "risk" : "warn"} size="sm">
                  {MATERIALITY_LABELS[c.materiality]}
                </Chip>
              )}
            </li>
          ))}
        </ul>
      ) : (
        e.tier !== "review" && (
          <p className="mt-2 text-oo-small text-oo-muted">
            {e.tier === "register"
              ? "A full re-run found nothing else that differs from the last check."
              : "A full re-run found no difference from the last check."}
          </p>
        )
      )}
      {e.gleif_log && <GleifLogBlock lines={gleifLogLines(e.gleif_log)} />}
      <p className="mt-2 text-oo-meta text-oo-muted">
        {checked}
        {degraded.length > 0 && (
          <span className="block text-oo-warn-text">
            Could not check: {degraded.map(label).join(", ")} — the absence of a finding there is not a
            clean result.
          </span>
        )}
      </p>
    </div>
  );
}

/** pKYC stage B — what the watcher expected of this company, and the form
 *  to say it. Closed, the block is one line (or the invitation to add
 *  context); open, the four fields with Save and Clear. A baseline that
 *  already disagrees with an expectation is said here at once — it is
 *  written to no entry, because the watcher's belief changing is not a
 *  change to the company. The backend validates every field and the
 *  refusal names the field; the form shows it where it was typed. */
function WatchContextBlock({
  w,
  busy,
  onSave,
}: {
  w: Watch;
  busy: boolean;
  onSave: (patch: WatchContextPatch) => Promise<string | null>;
}) {
  const ctx = w.context;
  const [editing, setEditing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [jurisdiction, setJurisdiction] = useState(ctx?.expected_jurisdiction ?? "");
  const [parent, setParent] = useState(ctx?.expected_parent_lei ?? "");
  const [rationale, setRationale] = useState(ctx?.rationale ?? "");
  const [report, setReport] = useState(ctx?.onboarding_report_id ?? "");
  const idBase = `ctx-${w.lei}`;

  const open = () => {
    setJurisdiction(ctx?.expected_jurisdiction ?? "");
    setParent(ctx?.expected_parent_lei ?? "");
    setRationale(ctx?.rationale ?? "");
    setReport(ctx?.onboarding_report_id ?? "");
    setError(null);
    setEditing(true);
  };

  const submit = async (ev: React.FormEvent) => {
    ev.preventDefault();
    const reportId = report.trim() ? reportIdFromInput(report) : null;
    if (report.trim() && !reportId) {
      setError("That is not a saved report link or id.");
      return;
    }
    const patch: WatchContextPatch = {
      expected_jurisdiction: jurisdiction.trim() ? jurisdiction.trim().toUpperCase() : null,
      expected_parent_lei: parent.trim() ? parent.trim().toUpperCase() : null,
      rationale: rationale.trim() ? rationale.trim() : null,
      onboarding_report_id: reportId,
    };
    const problem = await onSave(patch);
    if (problem) {
      setError(problem);
      return;
    }
    setEditing(false);
  };

  const clear = async () => {
    const problem = await onSave({
      expected_jurisdiction: null,
      expected_parent_lei: null,
      rationale: null,
      onboarding_report_id: null,
    });
    if (problem) setError(problem);
    else setEditing(false);
  };

  if (ctx === undefined) return null; // an older server: nothing to show, nothing to set

  const disagreements = ctx.baseline_disagrees ?? [];
  const summary = hasContext(ctx) ? (
    <ul className="space-y-0.5" aria-label="What your watch expects">
      {ctx.rationale && <li>{ctx.rationale}</li>}
      {ctx.expected_jurisdiction && (
        <li>
          {CONTEXT_FIELD_LABELS.expected_jurisdiction}: {ctx.expected_jurisdiction}
        </li>
      )}
      {ctx.expected_parent_lei && (
        <li className="break-all">
          Expected ultimate parent: {ctx.expected_parent_name ? `${ctx.expected_parent_name} · ` : ""}
          <span className="font-mono">{ctx.expected_parent_lei}</span>
        </li>
      )}
      {ctx.onboarding_report_id && (
        <li>
          {ctx.onboarding_report?.available && ctx.onboarding_report.report_path ? (
            <>
              Onboarding record:{" "}
              <a href={ctx.onboarding_report.report_path} className="underline underline-offset-2 hover:text-oo-blue">
                saved report of {day(ctx.onboarding_report.saved_at)}
              </a>
            </>
          ) : (
            <>Onboarding record: saved report {ctx.onboarding_report_id} — no longer held on this instance.</>
          )}
        </li>
      )}
    </ul>
  ) : (
    <p>No context recorded. Say what you expect of this company and the log will say when a change contradicts it.</p>
  );

  return (
    <div className="mt-2 text-oo-meta text-oo-muted" data-testid="watch-context">
      {!editing && (
        <>
          {summary}
          {disagreements.length > 0 && (
            <ul className="mt-1 space-y-0.5 text-oo-warn-text" aria-label="Baseline disagrees with what your watch expects">
              {disagreements.map((d) => (
                <li key={d.field} data-testid="baseline-disagrees">
                  {disagreementSentence(d, ctx.expected_parent_name)}
                </li>
              ))}
            </ul>
          )}
          <Button variant="ghost" size="sm" onClick={open} disabled={busy} className="mt-1">
            {hasContext(ctx) ? "Edit context" : "Add context"}
          </Button>
        </>
      )}
      {editing && (
        <form onSubmit={submit} className="mt-1 grid gap-2 max-w-xl" aria-label={`Context for ${w.legal_name || w.lei}`}>
          <p>{CONTEXT_FORM_NOTE}</p>
          <label className="grid gap-0.5" htmlFor={`${idBase}-j`}>
            <span>{CONTEXT_FIELD_LABELS.expected_jurisdiction}</span>
            <input
              id={`${idBase}-j`}
              value={jurisdiction}
              onChange={(e) => setJurisdiction(e.target.value)}
              placeholder="GB, DE, US-DE"
              maxLength={6}
              className="border border-oo-rule rounded px-2 py-1.5 text-oo-small text-oo-ink bg-white focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue"
            />
          </label>
          <label className="grid gap-0.5" htmlFor={`${idBase}-p`}>
            <span>{CONTEXT_FIELD_LABELS.expected_parent_lei}</span>
            <input
              id={`${idBase}-p`}
              value={parent}
              onChange={(e) => setParent(e.target.value)}
              placeholder="20-character LEI"
              maxLength={20}
              className="border border-oo-rule rounded px-2 py-1.5 font-mono text-oo-small text-oo-ink bg-white focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue"
            />
          </label>
          <label className="grid gap-0.5" htmlFor={`${idBase}-r`}>
            <span>
              {CONTEXT_FIELD_LABELS.rationale} ({rationale.length}/{RATIONALE_MAX})
            </span>
            <input
              id={`${idBase}-r`}
              value={rationale}
              onChange={(e) => setRationale(e.target.value)}
              maxLength={RATIONALE_MAX}
              className="border border-oo-rule rounded px-2 py-1.5 text-oo-small text-oo-ink bg-white focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue"
            />
          </label>
          <label className="grid gap-0.5" htmlFor={`${idBase}-s`}>
            <span>{CONTEXT_FIELD_LABELS.onboarding_report_id}</span>
            <input
              id={`${idBase}-s`}
              value={report}
              onChange={(e) => setReport(e.target.value)}
              placeholder="Paste a saved report link or id"
              className="border border-oo-rule rounded px-2 py-1.5 text-oo-small text-oo-ink bg-white focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue"
            />
          </label>
          {error && (
            <p role="alert" className="text-oo-warn-text">
              {error}
            </p>
          )}
          <div className="flex flex-wrap gap-2">
            <Button type="submit" variant="primary" size="sm" disabled={busy}>
              {busy ? "Saving…" : "Save context"}
            </Button>
            <Button variant="ghost" size="sm" onClick={() => setEditing(false)} disabled={busy}>
              Cancel
            </Button>
            {hasContext(ctx) && (
              <Button variant="ghost" size="sm" onClick={clear} disabled={busy}>
                Clear all
              </Button>
            )}
          </div>
        </form>
      )}
    </div>
  );
}

/** Phase 303 — GLEIF's own field-modification log: a lead sentence and
 *  its lines, the same words the Atom feed uses. */
function GleifLogBlock({ lines }: { lines: { lead: string; items: string[] } | null }) {
  if (!lines) return null;
  return (
    <div className="mt-2 text-oo-meta text-oo-muted">
      <p>{lines.lead}</p>
      {lines.items.length > 0 && (
        <ul className="mt-1 space-y-0.5" aria-label="GLEIF's modification log">
          {lines.items.map((line, i) => (
            <li key={i} className="flex gap-2">
              <span aria-hidden="true">–</span>
              <span className="break-words">{line}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
