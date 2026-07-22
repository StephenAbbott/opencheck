/**
 * BackgroundCheckPanel — the BackgroundCheck (people screening) view.
 *
 * SPIKE (feat/background-check). QuickCheck screens the subject entity;
 * FullCheck maps the corporate network; BackgroundCheck brings the
 * *people* connected to the entity to the fore — officers, PSCs and
 * beneficial owners from the assembled BODS bundle — and lets the user
 * run an on-demand, per-person screen across every person-capable
 * source (Companies House officers, OpenSanctions, EveryPolitician,
 * Wikidata, OpenAleph).
 *
 * Evidence discipline: person screening is name-based, so every result
 * is framed as a *potential match* with its similarity score and birth
 * year corroboration shown; risk chips only ever come from strong
 * matches (server-gated); no-hit sources are listed explicitly and a
 * clean screen is never presented as proof of absence.
 */

import { useEffect, useMemo, useState } from "react";
import {
  lookup,
  personCheck,
  type PersonCheckResponse,
  type PersonMatch,
} from "../../lib/api";
import {
  extractConnectedPeople,
  type ConnectedPerson,
  type Stmt,
} from "../../lib/backgroundCheck";
import { RiskChip } from "../risk/RiskChip";

/** Cap for the "Check all" convenience action — keeps the fan-out to
 * upstream APIs (OpenSanctions free tier in particular) bounded. */
const CHECK_ALL_CAP = 8;

type CheckState =
  | { status: "idle" }
  | { status: "running" }
  | { status: "done"; result: PersonCheckResponse }
  | { status: "error"; message: string };

export default function BackgroundCheckPanel({
  lei,
  legalName,
}: {
  lei: string;
  legalName: string | null;
}) {
  const [statements, setStatements] = useState<Stmt[] | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [checks, setChecks] = useState<Record<string, CheckState>>({});
  const [checkingAll, setCheckingAll] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setStatements(null);
    setLoadError(null);
    setChecks({});
    lookup(lei)
      .then((r) => {
        if (!cancelled) setStatements(r.bods as Stmt[]);
      })
      .catch((e) => {
        if (!cancelled) setLoadError((e as Error).message);
      });
    return () => {
      cancelled = true;
    };
  }, [lei]);

  const people = useMemo(
    () => (statements ? extractConnectedPeople(statements) : []),
    [statements]
  );

  const runCheck = async (person: ConnectedPerson) => {
    setChecks((c) => ({ ...c, [person.key]: { status: "running" } }));
    try {
      const result = await personCheck(person.name, person.birthYear);
      setChecks((c) => ({ ...c, [person.key]: { status: "done", result } }));
    } catch (e) {
      setChecks((c) => ({
        ...c,
        [person.key]: { status: "error", message: (e as Error).message },
      }));
    }
  };

  const runAll = async () => {
    setCheckingAll(true);
    try {
      const targets = people
        .filter((p) => checks[p.key]?.status !== "done")
        .slice(0, CHECK_ALL_CAP);
      // Sequential on purpose — a courteous request rate to upstreams.
      for (const p of targets) {
        // eslint-disable-next-line no-await-in-loop
        await runCheck(p);
      }
    } finally {
      setCheckingAll(false);
    }
  };

  return (
    <section className="mb-8" aria-label="BackgroundCheck — people screening">
      <div className="mb-3 rounded-oo border border-violet-300 bg-violet-50 px-4 py-3">
        <h3 className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-violet-700 mb-1">
          BackgroundCheck · People screening
        </h3>
        <p className="text-[13px] text-oo-ink leading-[1.6]">
          The people connected to{" "}
          <span className="font-medium">{legalName ?? lei}</span> in the data
          already gathered — officers, persons with significant control and
          beneficial owners. Run a check to screen a person against every
          person-capable source for PEP, sanctions and offshore-leaks signals.
        </p>
        <p className="text-[12px] text-violet-900/80 leading-[1.6] mt-1.5">
          Person screening is <span className="font-medium">name-based</span>:
          results are potential matches with their evidence shown, never
          confirmed identities — and a clean screen is not proof of absence.
        </p>
      </div>

      {loadError && (
        <p className="text-[13px] text-red-700 mb-4" role="alert">
          Could not load the entity bundle: {loadError}
        </p>
      )}
      {!statements && !loadError && (
        <p className="text-[13px] text-oo-muted italic">
          Loading connected people…
        </p>
      )}

      {statements && people.length === 0 && (
        <div className="rounded-oo border border-oo-rule bg-white px-4 py-6 text-center">
          <p className="text-[14px] text-oo-ink font-medium mb-1">
            No named people found for this entity
          </p>
          <p className="text-[12px] text-oo-muted leading-[1.6] max-w-xl mx-auto">
            None of the sources that responded published named officers,
            persons with significant control or beneficial owners for this
            entity (protected or anonymous persons are excluded). That is a
            data-availability statement, not a due-diligence conclusion.
          </p>
        </div>
      )}

      {people.length > 0 && (
        <>
          <div className="mb-4 flex items-center justify-between gap-3 flex-wrap">
            <p className="text-[12px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted">
              {people.length} connected {people.length === 1 ? "person" : "people"}
            </p>
            <button
              type="button"
              onClick={runAll}
              disabled={checkingAll}
              className="rounded-oo border border-violet-300 bg-white px-3 py-1.5 text-[12px] font-medium text-violet-800 hover:bg-violet-50 disabled:opacity-50"
            >
              {checkingAll
                ? "Checking…"
                : `Check ${people.length > CHECK_ALL_CAP ? `first ${CHECK_ALL_CAP}` : "all"}`}
            </button>
          </div>
          <ul className="space-y-4 list-none p-0 m-0">
            {people.map((person) => (
              <li key={person.key}>
                <PersonCard
                  person={person}
                  state={checks[person.key] ?? { status: "idle" }}
                  onCheck={() => runCheck(person)}
                />
              </li>
            ))}
          </ul>
        </>
      )}
    </section>
  );
}

function PersonCard({
  person,
  state,
  onCheck,
}: {
  person: ConnectedPerson;
  state: CheckState;
  onCheck: () => void;
}) {
  const resultId = `bgc-result-${person.key.replace(/[^a-z0-9]/gi, "-")}`;
  return (
    <div className="rounded-oo border border-violet-200 bg-white overflow-hidden">
      <div className="px-4 py-3 flex items-start justify-between gap-3 flex-wrap">
        <div>
          <p className="font-head font-bold text-[15px] text-oo-ink">
            {person.name}
            {person.birthYear && (
              <span className="ml-2 font-sans font-normal text-[12px] text-oo-muted">
                b. {person.birthDate}
              </span>
            )}
          </p>
          {person.nationalities.length > 0 && (
            <p className="text-[12px] text-oo-muted mt-0.5">
              {person.nationalities.join(", ")}
            </p>
          )}
          <ul className="mt-1.5 space-y-0.5 list-none p-0 m-0">
            {person.roles.map((role, i) => (
              <li key={i} className="text-[12px] text-oo-ink leading-[1.5]">
                <span className="font-medium">{role.label}</span>
                {role.subjectName && (
                  <span className="text-oo-muted"> — {role.subjectName}</span>
                )}
                {(role.startDate || role.endDate) && (
                  <span className="text-oo-muted font-mono text-[11px]">
                    {" "}
                    ({role.startDate ?? "…"}
                    {role.endDate ? ` → ${role.endDate}` : ""})
                  </span>
                )}
              </li>
            ))}
          </ul>
          {person.sources.length > 0 && (
            <p className="text-[10px] text-oo-muted mt-1.5">
              Recorded by: {person.sources.join(" · ")}
            </p>
          )}
        </div>
        <button
          type="button"
          onClick={onCheck}
          disabled={state.status === "running"}
          aria-expanded={state.status === "done"}
          aria-controls={resultId}
          className="shrink-0 rounded-oo bg-violet-700 px-3.5 py-2 text-[12px] font-semibold text-white hover:bg-violet-800 disabled:opacity-60"
        >
          {state.status === "running"
            ? "Checking…"
            : state.status === "done"
              ? "Re-run check"
              : "Run background check"}
        </button>
      </div>
      <div id={resultId}>
        {state.status === "error" && (
          <p className="px-4 pb-3 text-[12px] text-red-700" role="alert">
            Check failed: {state.message}
          </p>
        )}
        {state.status === "done" && <CheckResult result={state.result} />}
      </div>
    </div>
  );
}

function CheckResult({ result }: { result: PersonCheckResponse }) {
  const [showWeak, setShowWeak] = useState(false);
  const strong = result.matches.filter((m) => m.strong);
  const weak = result.matches.filter((m) => !m.strong);
  const failed = result.sources.filter((s) => s.error);
  const checkedOk = result.sources.filter((s) => !s.error);

  return (
    <div className="border-t border-violet-100 bg-violet-50/30 px-4 py-3 space-y-3">
      {result.risk_signals.length > 0 ? (
        <div>
          <p className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-1.5">
            Risk signals (from strong matches only)
          </p>
          <div className="flex flex-wrap gap-1.5">
            {result.risk_signals.map((s, i) => (
              <RiskChip key={i} signal={s} compact />
            ))}
          </div>
        </div>
      ) : (
        <p className="text-[12px] text-oo-ink leading-[1.6]">
          <span className="font-medium">
            No risk signals from strong matches
          </span>{" "}
          across {checkedOk.length} checked source
          {checkedOk.length === 1 ? "" : "s"}
          {failed.length > 0 &&
            ` (${failed.length} source${failed.length === 1 ? "" : "s"} failed — see below)`}
          . Absence from these sources is not proof of absence.
        </p>
      )}

      {strong.length > 0 && (
        <div>
          <p className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-1.5">
            Strong matches
          </p>
          <ul className="space-y-2 list-none p-0 m-0">
            {strong.map((m, i) => (
              <MatchRow key={i} match={m} />
            ))}
          </ul>
        </div>
      )}

      {weak.length > 0 && (
        <div>
          <button
            type="button"
            onClick={() => setShowWeak((v) => !v)}
            aria-expanded={showWeak}
            className="text-[11px] text-oo-muted underline hover:no-underline"
          >
            {showWeak ? "Hide" : "Show"} {weak.length} weaker name match
            {weak.length === 1 ? "" : "es"} (below similarity threshold — likely
            different people)
          </button>
          {showWeak && (
            <ul className="mt-2 space-y-2 list-none p-0 m-0">
              {weak.map((m, i) => (
                <MatchRow key={i} match={m} />
              ))}
            </ul>
          )}
        </div>
      )}

      {failed.length > 0 && (
        <div role="alert">
          <p className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-amber-700 mb-1">
            Sources that could not be checked
          </p>
          <ul className="space-y-0.5 list-none p-0 m-0">
            {failed.map((s) => (
              <li key={s.source_id} className="text-[11px] text-amber-800">
                {s.name}: {s.error} — this person was{" "}
                <span className="font-medium">not screened</span> against this
                source.
              </li>
            ))}
          </ul>
        </div>
      )}

      <details>
        <summary className="text-[10px] text-oo-muted cursor-pointer">
          Checked {checkedOk.length} source{checkedOk.length === 1 ? "" : "s"} —
          attribution &amp; licences
        </summary>
        <ul className="mt-1.5 space-y-1 list-none p-0 m-0">
          {result.sources.map((s) => (
            <li key={s.source_id} className="text-[10px] text-oo-muted leading-[1.5]">
              <a
                href={s.homepage}
                target="_blank"
                rel="noreferrer"
                className="font-medium text-oo-ink underline hover:no-underline"
              >
                {s.name}
              </a>{" "}
              · {s.hit_count} hit{s.hit_count === 1 ? "" : "s"}
              {!s.live && " · offline/stub mode"} · {s.attribution}
            </li>
          ))}
        </ul>
      </details>
    </div>
  );
}

function MatchRow({ match }: { match: PersonMatch }) {
  const pct = Math.round(match.name_score * 100);
  return (
    <li className="rounded-oo border border-oo-rule bg-white px-3 py-2">
      <div className="flex items-center justify-between gap-2 flex-wrap">
        <span className="text-[12px] font-medium text-oo-ink">
          {match.hit.name}
          {match.hit.is_stub && (
            <span className="ml-1.5 text-[10px] font-mono text-oo-muted">
              (stub)
            </span>
          )}
        </span>
        <span
          className={`text-[10px] font-mono px-1.5 py-0.5 rounded ${
            match.strong
              ? "bg-violet-100 text-violet-800"
              : "bg-slate-100 text-slate-600"
          }`}
          title="Name similarity between the queried person and this record"
        >
          {match.strong ? "potential match" : "weak"} · {pct}%
          {!match.birth_year_compatible && " · birth year differs"}
        </span>
      </div>
      <p className="text-[11px] text-oo-muted mt-0.5">
        {match.hit.summary || "No summary from source."}{" "}
        <span className="font-mono">via {match.hit.source_id}</span>
      </p>
    </li>
  );
}
