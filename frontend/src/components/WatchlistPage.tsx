/**
 * `/watchlist` — the companies this browser watches, and what changed
 * (Phase 215).
 *
 * The page is the front of a delta-driven loop, and it says so: a company
 * on the list is re-checked only when GLEIF's Golden Copy delta or
 * OpenSanctions' entity delta names it. Nothing here polls a register. The
 * honesty line under the list (`tierSentence`) states what each tier is
 * actually doing on this instance — the mirror's watermark and how many
 * records the last delta changed — so "3.4 million LEI records; 16,000
 * changed today; one of them was yours" is a description, not a slogan.
 *
 * The token comes from `?token=` (a shared or bookmarked link) or from this
 * browser's storage; a URL token is kept for next time. It is a capability:
 * the page says so beside the feed address.
 *
 * Three surfaces: the watched companies (baseline, last check, re-check
 * now, remove); the log, newest first, each entry opening with the tier
 * that fired and closing with how many sources were reached; and the Atom
 * address with a copy button.
 */

import { useCallback, useEffect, useMemo, useState } from "react";

import { getWatchlist, recheckWatch, removeWatch } from "../lib/api";
import { trackEvent } from "../lib/analytics";
import { sourceLabel } from "../lib/vocab";
import {
  checkedSentence,
  describeChange,
  entryHeadline,
  feedHelp,
  readToken,
  sortEntries,
  storeToken,
  tierSentence,
  tokenFromLocation,
  triggerSentence,
  type Watch,
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

  const copyFeed = () => {
    if (!data) return;
    navigator.clipboard?.writeText(data.feed_url);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1500);
  };

  const entries = useMemo(() => (data ? sortEntries(data.entries) : []), [data]);
  const honesty = useMemo(() => tierSentence(data?.tiers ?? null), [data]);

  return (
    <div className="max-w-oo-page mx-auto">
      <section className="mb-6 bg-white border border-oo-rule rounded-oo p-7">
        <h1 className={sectionLabelClasses("muted")}>Watchlist</h1>
        <p className="mt-2 max-w-2xl text-oo-small text-oo-muted">
          The companies this browser watches. Each is re-checked only when GLEIF or OpenSanctions
          publish a change to its record — the re-run then fetches every source a normal check
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
          {entries.length === 0 ? (
            <p className="mt-3 text-oo-small text-oo-muted" data-testid="log-empty">
              Nothing yet. An entry appears here, and in the feed, when GLEIF or OpenSanctions
              publish a change to a watched company and the re-run finds a difference.
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
}: {
  w: Watch;
  busy: boolean;
  label: (id: string) => string;
  onOpen?: (lei: string) => void;
  onRecheck: () => void;
  onRemove: () => void;
}) {
  const b = w.baseline;
  const status = b.register_status;
  const gleifStatus = w.gleif_facts?.registration_status ?? null;
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
              {status.liveness === "live" ? "Active" : status.liveness === "terminal" ? "Dissolved" : status.liveness}
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
        </div>
        <p className="mt-2 text-oo-meta text-oo-muted">
          Watching since {day(w.added_at)} · last checked {day(w.last_checked_at)}
          {w.gleif_watermark ? ` · GLEIF record as of ${w.gleif_watermark.slice(0, 10)}` : ""}
        </p>
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
  const tierTone = e.tier === "opensanctions" ? "risk" : e.tier === "gleif" ? "accent" : "neutral";
  return (
    <div>
      <div className="flex flex-wrap items-baseline justify-between gap-2">
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
        <span className="text-oo-meta text-oo-muted">
          <Chip tone={tierTone} size="sm">
            {e.tier === "gleif" ? "GLEIF delta" : e.tier === "opensanctions" ? "OpenSanctions delta" : "By hand"}
          </Chip>{" "}
          {e.created_at.replace("T", " ").slice(0, 16)} UTC
        </span>
      </div>
      <p className="mt-2 text-oo-small text-oo-ink">{triggerSentence(e)}</p>
      {e.changes.length > 0 ? (
        <ul className="mt-2 space-y-1 text-oo-small text-oo-ink" aria-label="What the re-run found">
          {e.changes.map((c, i) => (
            <li key={`${c.kind}-${i}`} className="flex gap-2">
              <span aria-hidden="true" className="text-oo-muted">
                –
              </span>
              <span className={c.kind.endsWith("_unchecked") ? "text-oo-warn-text" : ""}>
                {describeChange(c, label)}
              </span>
            </li>
          ))}
        </ul>
      ) : (
        <p className="mt-2 text-oo-small text-oo-muted">A full re-run found no difference from the last check.</p>
      )}
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
