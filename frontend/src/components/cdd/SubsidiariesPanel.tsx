/**
 * SubsidiariesPanel — the Subsidiaries tab (Phase 185).
 *
 * What this company owns, from every source OpenCheck holds, kept apart per
 * source. Before this the same lists were scattered across three tabs: the
 * GLEIF network behind a strip at the bottom of FullCheck, the MEIP signpost
 * at the bottom of QuickCheck, EITI's declared list inside an ESG card that
 * fetched the GLEIF network a second time to compare against. None had a
 * URL. Now `?mode=subsidiaries` has all of them and each is fetched once.
 *
 * The sources disagree, and the panel says so in its first sentence rather
 * than reconciling them into one list: GLEIF Level 2 is accounting
 * consolidation, MEIP is the OECD's register of the 500 largest groups, EITI
 * is what a company declared about its extractive operations, GEM is asset
 * ownership in the energy sector. No two measure the same thing and no public
 * source has the whole picture — which is the tab's advocacy point.
 *
 * What it does settle is which rows can be opened: a row links to its own
 * Subsidiaries tab when its source publishes an LEI, and when another list
 * holds an LEI for the same name the link is offered with a name-match chip,
 * never asserted. Rows with neither say so.
 *
 * Renders bands, not cards (the Phase 128 rule): the tabpanel owns the
 * `PanelCard` and this supplies its `PanelSection`s.
 */

import { useEffect, useMemo, useState } from "react";
import {
  getDeclaredSubsidiaries,
  type DeclaredSource,
  type DeclaredSubsidiariesResponse,
  type MeipMatch,
  type RiskSignal,
  type SubsidiariesResponse,
} from "../../lib/api";
import { describeFetchFailure, type PanelError, type PanelId } from "../../lib/panelErrors";
import {
  coverageSentence,
  LIST_LABEL,
  openableSentence,
  orderRows,
  resolveLists,
  subsidiaryHref,
  type Coverage,
  type ResolvedList,
  type ResolvedRow,
} from "../../lib/subsidiariesMode";
import { NOT_IN_GRAPH } from "../../lib/vocab";
import { Button } from "../ui/Button";
import { Chip } from "../ui/Chip";
import MatchConfidenceChip from "../ui/MatchConfidenceChip";
import PanelSection from "../ui/PanelSection";
import { MeipSignpost } from "./MeipSignpost";
import { SubsidiaryNetwork } from "./SubsidiaryNetwork";

/** Rows shown before a list collapses behind a control. */
const VISIBLE_ROWS = 12;

/** The lists a company can appear in, for the "N of M" aside. */
const LIST_COUNT = 4;

// ---------------------------------------------------------------------
// One resolved row
// ---------------------------------------------------------------------

function relationLabel(list: ResolvedList["id"], relation: string | null): string | null {
  if (!relation) return null;
  if (list === "gleif") {
    return relation === "both" ? "Direct + ultimate" : relation === "ultimate" ? "Ultimate" : "Direct";
  }
  if (relation === "in group") return "In the group";
  if (relation === "declared") return null; // every EITI row is declared; the band says so once
  return "Direct";
}

function Row({ row, list }: { row: ResolvedRow; list: ResolvedList }) {
  const lei = row.lei ?? row.matched?.lei ?? null;
  const rel = relationLabel(list.id, row.relation);
  return (
    <li className="flex flex-wrap items-center gap-x-2 gap-y-1 px-3 py-2">
      <span className="text-oo-small text-oo-ink">
        {lei ? (
          <a href={subsidiaryHref(lei)} className="hover:underline">
            {row.name}
          </a>
        ) : (
          row.name
        )}
      </span>
      {row.country && <Chip tone="neutral">{row.country}</Chip>}
      {rel && <Chip tone="context">{rel}</Chip>}
      {row.percent != null && (
        <span className="font-mono text-oo-meta text-oo-muted">{row.percent}%</span>
      )}
      {row.years.length > 0 && (
        <span className="text-oo-meta text-oo-muted">{row.years.join(", ")}</span>
      )}
      {row.via && list.id === "meip" && row.relation === "in group" && (
        <span className="text-oo-meta text-oo-muted">via {row.via}</span>
      )}
      {row.alsoIn.length > 0 && (
        <Chip tone="ok">Also in {row.alsoIn.map((id) => LIST_LABEL[id]).join(" · ")}</Chip>
      )}
      {/* Name-derived: chipped as such, never asserted. */}
      {!row.lei && row.matched && (
        <MatchConfidenceChip confidence="low" basis={`from ${LIST_LABEL[row.matched.from]}`} />
      )}
      {lei ? (
        <span className="font-mono text-oo-meta text-oo-muted">{lei}</span>
      ) : (
        <span className="text-oo-meta text-oo-muted">no LEI published</span>
      )}
    </li>
  );
}

function RowsList({ list }: { list: ResolvedList }) {
  const [showAll, setShowAll] = useState(false);
  const ordered = useMemo(() => orderRows(list.rows), [list.rows]);
  const rows = showAll ? ordered : ordered.slice(0, VISIBLE_ROWS);
  if (list.rows.length === 0) {
    return <p className="text-oo-small text-oo-muted">No entries to list.</p>;
  }
  return (
    <>
      <ul
        className="divide-y divide-oo-rule rounded-oo border border-oo-rule bg-white"
        data-testid={`declared-list-${list.id}`}
      >
        {rows.map((r, i) => (
          <Row key={`${r.name}-${r.lei ?? i}`} row={r} list={list} />
        ))}
      </ul>
      {ordered.length > VISIBLE_ROWS && (
        <Button
          variant="ghost"
          size="sm"
          className="mt-2"
          aria-expanded={showAll}
          onClick={() => setShowAll((v) => !v)}
        >
          {showAll ? `Show the first ${VISIBLE_ROWS}` : `Show all ${ordered.length.toLocaleString()}`}
        </Button>
      )}
    </>
  );
}

/** "294 listed · 294 with an LEI · 12 also in another list" */
function listAside(list: ResolvedList, source?: DeclaredSource): string {
  const parts: string[] = [];
  if (source?.total != null && source.total > list.rows.length) {
    parts.push(`${list.rows.length.toLocaleString()} of ${source.total.toLocaleString()} listed here`);
  } else {
    parts.push(`${list.rows.length.toLocaleString()} listed`);
  }
  parts.push(`${list.openable.toLocaleString()} can be opened`);
  return parts.join(" · ");
}

// ---------------------------------------------------------------------
// The coverage strip — the tab's first band
// ---------------------------------------------------------------------

function CoverageStrip({
  cov,
  declared,
  name,
  gleifState,
}: {
  cov: Coverage;
  declared: DeclaredSubsidiariesResponse | null;
  name: string;
  gleifState: "pending" | "ok" | "refused";
}) {
  const missing: { label: string; text: string; tone: "muted" | "warn" }[] = [];
  if (gleifState === "refused") {
    missing.push({ label: LIST_LABEL.gleif, text: "did not answer, so it is not compared here", tone: "warn" });
  }
  for (const s of declared?.sources ?? []) {
    if (!s.available) missing.push({ label: LIST_LABEL[s.id], text: s.reason ?? "could not be read", tone: "warn" });
    else if (!s.covered) missing.push({ label: LIST_LABEL[s.id], text: s.reason ?? "does not cover this company", tone: "muted" });
  }
  return (
    <>
      <p className="text-oo-body text-oo-ink leading-[1.6] max-w-[82ch]">{coverageSentence(cov, name)}</p>
      {cov.listed > 0 && (
        <p className="mt-1.5 text-oo-small text-oo-muted leading-[1.6] max-w-[82ch]">{openableSentence(cov)}</p>
      )}
      {cov.lists.length > 0 && (
        <ul className="mt-3 flex flex-wrap gap-2" aria-label="Lists available">
          {cov.lists.map((l) => (
            <li
              key={l.id}
              className="rounded-oo border border-oo-rule bg-oo-bg px-2.5 py-1.5 text-oo-meta text-oo-ink"
            >
              <span className="font-semibold">{l.label}</span>
              <span className="text-oo-muted"> · {l.rows.length.toLocaleString()} · {l.openable.toLocaleString()} can be opened</span>
            </li>
          ))}
        </ul>
      )}
      {missing.length > 0 && (
        <ul className="mt-2 space-y-0.5" aria-label="Lists not available">
          {missing.map((m) => (
            <li
              key={m.label}
              className={`text-oo-meta ${m.tone === "warn" ? "text-oo-warn-text" : "text-oo-muted"}`}
            >
              {m.label} — {m.text}.
            </li>
          ))}
        </ul>
      )}
    </>
  );
}

// ---------------------------------------------------------------------
// The panel
// ---------------------------------------------------------------------

export default function SubsidiariesPanel({
  lei,
  legalName,
  signals = [],
  meip = null,
  onPanelError,
  onPanelRecovered,
}: {
  lei: string;
  legalName: string | null;
  signals?: RiskSignal[];
  /** The lookup's MEIP match, when the stream has delivered one: its
   *  identifiers and group context head the MEIP band. */
  meip?: MeipMatch | null;
  /** Forwarded to SubsidiaryNetwork — a /subsidiaries failure reaches the
   *  report-level notice; this panel is mounted inside a tab, so nothing
   *  above it would otherwise learn the fetch failed. */
  onPanelError?: (e: PanelError) => void;
  onPanelRecovered?: (panel: PanelId) => void;
}) {
  const name = legalName ?? lei;
  const [declared, setDeclared] = useState<DeclaredSubsidiariesResponse | null>(null);
  const [declaredError, setDeclaredError] = useState<string | null>(null);
  const [gleif, setGleif] = useState<SubsidiariesResponse | null>(null);

  useEffect(() => {
    let live = true;
    setDeclared(null);
    setDeclaredError(null);
    setGleif(null);
    getDeclaredSubsidiaries(lei)
      .then((r) => {
        if (live) setDeclared(r);
      })
      .catch((e) => {
        if (live) setDeclaredError(describeFetchFailure(e));
      });
    return () => {
      live = false;
    };
  }, [lei]);

  // GLEIF's list enters the comparison only when it answered: a refused
  // network must not read as "GLEIF holds none of these".
  const gleifState: "pending" | "ok" | "refused" = !gleif
    ? "pending"
    : gleif.children_available === false
      ? "refused"
      : "ok";
  const cov = useMemo(
    () => resolveLists(declared?.sources ?? [], gleifState === "ok" ? gleif!.children : null),
    [declared, gleif, gleifState],
  );
  const bySource = new Map((declared?.sources ?? []).map((s) => [s.id, s]));
  const listById = new Map(cov.lists.map((l) => [l.id, l]));
  const loading = !declared && !declaredError;

  const meipList = listById.get("meip");
  const eitiList = listById.get("eiti_assessment");
  const gemList = listById.get("climatetrace");
  const meipSource = bySource.get("meip");
  const eitiSource = bySource.get("eiti_assessment");
  const gemSource = bySource.get("climatetrace");

  return (
    <>
      <PanelSection
        title="What the sources say"
        aside={
          loading
            ? "Reading the lists…"
            : `${cov.lists.length + (gleifState === "pending" ? 1 : 0)} of ${LIST_COUNT} lists available`
        }
      >
        {declaredError && (
          <p
            role="alert"
            className="mb-2 text-oo-small text-oo-warn-text bg-oo-warn-bg border border-oo-warn-border rounded-oo px-3 py-2"
          >
            The declared lists could not be fetched — {declaredError}. The GLEIF network below is
            unaffected. This is not a finding that the company has no subsidiaries.
          </p>
        )}
        {loading ? (
          <p role="status" className="text-oo-small text-oo-muted italic">
            Reading what each source lists for {name}…
          </p>
        ) : (
          <CoverageStrip cov={cov} declared={declared} name={name} gleifState={gleifState} />
        )}
      </PanelSection>

      <PanelSection
        title="GLEIF Level 2 network"
        aside="direct and ultimate children · consolidation, not shareholding"
      >
        <SubsidiaryNetwork
          lei={lei}
          entityName={legalName ?? undefined}
          signals={signals}
          onError={onPanelError}
          onRecovered={onPanelRecovered}
          onData={setGleif}
          bare
          autoRun
        />
      </PanelSection>

      {/* MEIP: the register's own list, headed by the signpost's context and
          identifiers when the lookup matched. The signpost used to be the
          last thing on the QuickCheck page. */}
      {meipList && meipSource && (
        meip ? (
          <MeipSignpost match={meip} measures={meipSource.measures}>
            <RowsList list={meipList} />
          </MeipSignpost>
        ) : (
          <PanelSection title={LIST_LABEL.meip} aside={listAside(meipList, meipSource)}>
            <p className="mb-2 text-oo-small text-oo-muted leading-[1.6] max-w-[82ch]">
              {capitalise(meipSource.measures)}. <span className="italic">{NOT_IN_GRAPH}</span>.
            </p>
            <RowsList list={meipList} />
          </PanelSection>
        )
      )}

      {eitiList && eitiSource && (
        <PanelSection
          title={`${LIST_LABEL.eiti_assessment} · declared controlled subsidiaries`}
          aside={listAside(eitiList, eitiSource)}
        >
          <p className="mb-2 text-oo-small text-oo-muted leading-[1.6] max-w-[82ch]">
            {capitalise(eitiSource.measures)}
            {typeof eitiSource.context?.assessment_year === "string"
              ? ` · ${eitiSource.context.assessment_year} assessment`
              : ""}
            . <span className="italic">{NOT_IN_GRAPH}</span>. A name that also appears in another
            list is offered as a name match, not as proof the two records are the same company.
          </p>
          <RowsList list={eitiList} />
        </PanelSection>
      )}

      {gemList && gemSource && (
        <PanelSection
          title={`${LIST_LABEL.climatetrace} · directly owned entities`}
          aside={listAside(gemList, gemSource)}
        >
          <p className="mb-2 text-oo-small text-oo-muted leading-[1.6] max-w-[82ch]">
            {capitalise(gemSource.measures)}. <span className="italic">{NOT_IN_GRAPH}</span>.
          </p>
          <RowsList list={gemList} />
        </PanelSection>
      )}
    </>
  );
}

function capitalise(s: string): string {
  return s ? s[0].toUpperCase() + s.slice(1) : s;
}
