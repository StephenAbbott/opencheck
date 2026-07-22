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
  personAppointments,
  personCheck,
  type PersonAppointmentsResponse,
  type PersonCheckResponse,
  type PersonMatch,
} from "../../lib/api";
import {
  extractConnectedPeople,
  possiblySamePeople,
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
  | {
      status: "done";
      result: PersonCheckResponse;
      /** Wall-clock time the check completed — shown so a re-run's
       * freshness is explicit. */
      fetchedAt: string;
      /** Result region visibility — users can hide a completed check's
       * matches without discarding them. */
      open: boolean;
    }
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
  const [checkAllProgress, setCheckAllProgress] = useState<
    { done: number; total: number } | null
  >(null);

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
  const samePairs = useMemo(() => possiblySamePeople(people), [people]);

  const runCheck = async (person: ConnectedPerson) => {
    setChecks((c) => ({ ...c, [person.key]: { status: "running" } }));
    try {
      const result = await personCheck(person.name, person.birthYear);
      setChecks((c) => ({
        ...c,
        [person.key]: {
          status: "done",
          result,
          fetchedAt: new Date().toISOString(),
          open: true,
        },
      }));
    } catch (e) {
      setChecks((c) => ({
        ...c,
        [person.key]: { status: "error", message: (e as Error).message },
      }));
    }
  };

  const toggleOpen = (key: string) => {
    setChecks((c) => {
      const state = c[key];
      if (state?.status !== "done") return c;
      return { ...c, [key]: { ...state, open: !state.open } };
    });
  };

  const runAll = async () => {
    setCheckingAll(true);
    try {
      const targets = people
        .filter((p) => checks[p.key]?.status !== "done")
        .slice(0, CHECK_ALL_CAP);
      setCheckAllProgress({ done: 0, total: targets.length });
      // Sequential on purpose — a courteous request rate to upstreams.
      for (let i = 0; i < targets.length; i++) {
        // eslint-disable-next-line no-await-in-loop
        await runCheck(targets[i]);
        setCheckAllProgress({ done: i + 1, total: targets.length });
      }
    } finally {
      setCheckingAll(false);
      setCheckAllProgress(null);
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
            <span className="flex items-center gap-2">
              {checkAllProgress && (
                <span
                  className="text-[11px] text-violet-800"
                  role="status"
                  aria-live="polite"
                >
                  {checkAllProgress.done} of {checkAllProgress.total} checked…
                </span>
              )}
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
            </span>
          </div>
          {samePairs.length > 0 && (
            <div className="mb-4 rounded-oo border border-amber-200 bg-amber-50 px-4 py-3">
              <p className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-amber-800 mb-1">
                Possibly the same person — review
              </p>
              <ul className="list-none p-0 m-0 space-y-0.5">
                {samePairs.map((pair) => (
                  <li
                    key={`${pair.a}|${pair.b}`}
                    className="text-[12px] text-amber-900 leading-[1.6]"
                  >
                    Two entries named{" "}
                    <span className="font-medium">{pair.name}</span> ({pair.reason}
                    ) — they are listed separately below; review before treating
                    them as one individual.
                  </li>
                ))}
              </ul>
            </div>
          )}
          <ul className="space-y-4 list-none p-0 m-0">
            {people.map((person) => (
              <li key={person.key}>
                <PersonCard
                  person={person}
                  state={checks[person.key] ?? { status: "idle" }}
                  onCheck={() => runCheck(person)}
                  onToggle={() => toggleOpen(person.key)}
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
  onToggle,
}: {
  person: ConnectedPerson;
  state: CheckState;
  onCheck: () => void;
  onToggle: () => void;
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
        <span className="shrink-0 flex items-center gap-2">
          {state.status === "done" && (
            <button
              type="button"
              onClick={onToggle}
              aria-expanded={state.open}
              aria-controls={resultId}
              className="rounded-oo border border-violet-300 bg-white px-3 py-2 text-[12px] font-medium text-violet-800 hover:bg-violet-50"
            >
              {state.open ? "Hide" : "Show"}
            </button>
          )}
          <button
            type="button"
            onClick={onCheck}
            disabled={state.status === "running"}
            className="rounded-oo bg-violet-700 px-3.5 py-2 text-[12px] font-semibold text-white hover:bg-violet-800 disabled:opacity-60"
          >
            {state.status === "running"
              ? "Checking…"
              : state.status === "done"
                ? "Re-run check"
                : "Run background check"}
          </button>
        </span>
      </div>
      <div id={resultId}>
        {state.status === "error" && (
          <p className="px-4 pb-3 text-[12px] text-red-700" role="alert">
            Check failed: {state.message}
          </p>
        )}
        {state.status === "done" && state.open && (
          <CheckResult result={state.result} fetchedAt={state.fetchedAt} />
        )}
      </div>
    </div>
  );
}

function CheckResult({
  result,
  fetchedAt,
}: {
  result: PersonCheckResponse;
  fetchedAt: string;
}) {
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

      {result.cross_source_links.length > 0 && (
        <div>
          <p className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-1.5">
            Same person across sources
          </p>
          <ul className="space-y-1 list-none p-0 m-0">
            {result.cross_source_links.map((link, i) => (
              <li
                key={i}
                className="text-[11px] text-oo-ink leading-[1.6] rounded-oo border border-oo-rule bg-white px-3 py-1.5"
              >
                {link.hits.map((h) => h.source_id).join(" and ")} describe the
                same record — matched on shared identifier{" "}
                <span className="font-mono">{link.key}</span> ={" "}
                <span className="font-mono">{link.key_value}</span>
              </li>
            ))}
          </ul>
        </div>
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

      <p className="text-[10px] text-oo-muted">
        Checked at{" "}
        <time dateTime={fetchedAt}>
          {new Date(fetchedAt).toLocaleTimeString()}
        </time>{" "}
        — results reflect the sources at that moment; re-run to refresh.
      </p>

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

type ApptState =
  | { status: "idle" }
  | { status: "loading" }
  | { status: "done"; data: PersonAppointmentsResponse }
  | { status: "error"; message: string };

function MatchRow({ match }: { match: PersonMatch }) {
  const pct = Math.round(match.name_score * 100);
  const [appts, setAppts] = useState<ApptState>({ status: "idle" });
  const [apptsOpen, setApptsOpen] = useState(false);
  // Companies House officer hits carry the register's stable officer id
  // as hit_id — the identifier-backed appointments view hangs off it.
  const isChOfficer = match.hit.source_id === "companies_house" && !match.hit.is_stub;
  const apptsId = `appts-${match.hit.source_id}-${match.hit.hit_id.replace(/[^a-zA-Z0-9]/g, "-")}`;

  const loadAppointments = async () => {
    if (apptsOpen) {
      setApptsOpen(false);
      return;
    }
    setApptsOpen(true);
    if (appts.status === "done") return;
    setAppts({ status: "loading" });
    try {
      const data = await personAppointments(match.hit.hit_id);
      setAppts({ status: "done", data });
    } catch (e) {
      setAppts({ status: "error", message: (e as Error).message });
    }
  };

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
          {match.strong ? "potential match" : "below threshold"} · {pct}%
          {!match.birth_year_compatible && " · birth year differs"}
        </span>
      </div>
      <p className="text-[11px] text-oo-muted mt-0.5">
        {match.hit.summary || "No summary from source."}{" "}
        <span className="font-mono">via {match.hit.source_id}</span>
      </p>
      {isChOfficer && (
        <div className="mt-1.5">
          <button
            type="button"
            onClick={loadAppointments}
            aria-expanded={apptsOpen}
            aria-controls={apptsId}
            className="text-[11px] text-violet-800 underline hover:no-underline"
          >
            {apptsOpen ? "Hide appointments" : "View appointments across companies"}
          </button>
          <div id={apptsId}>
            {apptsOpen && appts.status === "loading" && (
              <p className="text-[11px] text-oo-muted italic mt-1">
                Loading appointments…
              </p>
            )}
            {apptsOpen && appts.status === "error" && (
              <p className="text-[11px] text-red-700 mt-1" role="alert">
                Could not load appointments: {appts.message}
              </p>
            )}
            {apptsOpen && appts.status === "done" && (
              <AppointmentsList data={appts.data} />
            )}
          </div>
        </div>
      )}
    </li>
  );
}

function AppointmentsList({ data }: { data: PersonAppointmentsResponse }) {
  if (data.is_stub) {
    return (
      <p className="text-[11px] text-oo-muted mt-1">
        Live Companies House access is not configured — appointments
        unavailable in offline mode.
      </p>
    );
  }
  return (
    <div className="mt-1.5 rounded-oo border border-violet-100 bg-violet-50/40 px-3 py-2">
      <p className="text-[11px] text-oo-ink mb-1.5">
        <span className="font-medium">{data.name ?? "This officer"}</span>
        {data.birth_date && (
          <span className="text-oo-muted"> (b. {data.birth_date})</span>
        )}{" "}
        — {data.total_results ?? data.appointments.length} appointment
        {(data.total_results ?? data.appointments.length) === 1 ? "" : "s"} under
        this officer record, {data.active_count} active. The register asserts
        these belong to one officer identifier — this is stronger evidence than
        a name match.
      </p>
      <ul className="space-y-1 list-none p-0 m-0">
        {data.appointments.map((a, i) => (
          <li key={i} className="text-[11px] leading-[1.5] text-oo-ink">
            <span className="font-medium">{a.company_name}</span>
            {a.company_number && (
              <span className="font-mono text-oo-muted"> {a.company_number}</span>
            )}
            {a.role && <span> — {a.role}</span>}
            {a.appointed_on && (
              <span className="text-oo-muted font-mono text-[10px]">
                {" "}
                ({a.appointed_on}
                {a.resigned_on ? ` → ${a.resigned_on}` : " → present"})
              </span>
            )}
            {a.resigned_on ? (
              <span className="ml-1 text-[10px] text-oo-muted">resigned</span>
            ) : (
              a.company_status === "active" && (
                <span className="ml-1 text-[10px] text-emerald-700">active</span>
              )
            )}
          </li>
        ))}
      </ul>
      <p className="text-[10px] text-oo-muted mt-1.5">{data.caveat}</p>
      <p className="text-[10px] text-oo-muted mt-0.5">{data.attribution}</p>
    </div>
  );
}
