/**
 * KnowabilityChainList — what can be known along the ownership path
 * (Phase 226, Phase C of the knowability ticket).
 *
 * One line per jurisdiction the mapped chain runs through — the subject's
 * first, then each rank of owners — so a chain through KY and BM says what
 * those two publish about beneficial owners rather than leaving the reader to
 * infer it from the "Outside EU/EEA" chip. The run's own chain arrives frozen
 * on the `knowability_chain` event; as FullCheck expands, the panel hands this
 * list the jurisdictions now on the path and it asks `GET /knowability` for
 * the ones it does not yet hold. A frozen sentence is never replaced by a
 * fetched one (`mergeChain`), so nothing changes under the reader.
 *
 * Saved report (`readOnly`): the frozen chain only — the network cannot be
 * expanded there, and a saved page reads no live table (Phase 218).
 *
 * Voice rules as in `VerdictStrip`'s band: the sentences are the server's,
 * verbatim; badges are `context`/`neutral`; nothing here is a signal.
 */

import { useEffect, useMemo, useState } from "react";

import { fetchKnowability, type KnowabilityChain, type KnowabilityStatement } from "../../lib/api";
import { knowabilityView } from "../../lib/knowability";
import { chainSummary, mergeChain } from "../../lib/knowabilityChain";
import { Chip, SectionLabel } from "../ui";
import { Explain } from "../ui/Explain";

export function KnowabilityChainList({
  frozen,
  graphCodes,
  expanding = false,
  readOnly = false,
}: {
  /** The run's `knowability_chain` event, or null before it lands / on an
   *  older saved report. */
  frozen: KnowabilityChain | null;
  /** The jurisdictions on the upward path of the network as drawn now. */
  graphCodes: string[];
  /** The explorer is mid-expansion: say so instead of "no path". */
  expanding?: boolean;
  /** A saved report: show the frozen chain, fetch nothing. */
  readOnly?: boolean;
}) {
  const [fetched, setFetched] = useState<Map<string, KnowabilityStatement>>(() => new Map());
  const [fetchError, setFetchError] = useState<string | null>(null);

  const merged = useMemo(
    () => mergeChain(frozen, readOnly ? [] : graphCodes, fetched),
    [frozen, graphCodes, fetched, readOnly],
  );
  const pendingKey = merged.pending.join(",");

  useEffect(() => {
    if (readOnly || !pendingKey) return;
    let live = true;
    fetchKnowability(pendingKey.split(","))
      .then((res) => {
        if (!live) return;
        setFetched((prev) => {
          const next = new Map(prev);
          for (const st of res.statements) if (!next.has(st.code)) next.set(st.code, st);
          return next;
        });
        setFetchError(null);
      })
      .catch((e: unknown) => {
        if (live) setFetchError((e as Error).message);
      });
    return () => {
      live = false;
    };
  }, [pendingKey, readOnly]);

  const subject = frozen?.subject ?? merged.codes[0] ?? null;
  if (merged.codes.length === 0 && !frozen) return null;

  return (
    <div data-testid="knowability-chain" className="flex flex-col gap-2.5">
      <p className="text-oo-small text-oo-muted leading-[1.6] max-w-[82ch]">
        {chainSummary(merged.codes, subject, expanding)}
        {merged.pending.length > 0 && !fetchError && !readOnly && " Reading the register notes…"}
        {fetchError && ` The register notes for ${merged.pending.join(", ")} could not be loaded (${fetchError}).`}
      </p>
      <ul className="flex flex-col gap-2.5">
        {merged.statements.map((st) => {
          const v = knowabilityView(st);
          return (
            <li key={st.code} className="flex flex-col gap-1">
              <div className="flex flex-wrap items-center gap-2 min-w-0 max-w-full">
                <SectionLabel as="h3">{v.subheading}</SectionLabel>
                {st.code === subject && (
                  <span className="text-oo-meta text-oo-muted">subject</span>
                )}
                <Chip tone={v.badge.tone} size="sm">
                  {v.badge.label}
                </Chip>
              </div>
              <div className="text-oo-small text-oo-ink max-w-[76ch]">
                {v.sentence}
                {(v.rows.length > 0 || v.sources.length > 0) && (
                  <>
                    {" "}
                    <Explain
                      className="align-text-bottom"
                      label={`Show the register facts behind this statement for ${v.subheading}`}
                    >
                      <dl className="grid grid-cols-1 gap-y-1 sm:grid-cols-[max-content_1fr] sm:gap-x-4">
                        {v.rows.map((row) => (
                          <div key={row.label} className="contents">
                            <dt className="font-semibold text-oo-muted">{row.label}</dt>
                            <dd>{row.value}</dd>
                          </div>
                        ))}
                      </dl>
                      {v.sources.length > 0 && (
                        <p className="mt-1.5">
                          <span className="font-semibold text-oo-muted">Sources</span>{" "}
                          {v.sources.map((src, i) => (
                            <span key={src.url}>
                              {i > 0 && " · "}
                              <a
                                href={src.url}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="underline underline-offset-2 hover:no-underline break-all"
                              >
                                {src.title}
                              </a>
                            </span>
                          ))}
                        </p>
                      )}
                      {v.asOfLine && <p className="mt-1.5 text-oo-muted">{v.asOfLine}.</p>}
                    </Explain>
                  </>
                )}
              </div>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
