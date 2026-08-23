import { useRef, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { EXPORT_FORMATS, exportUrl, getLicenseMatrix, type ExportFormat } from "../../lib/api";
import { SectionHeading } from "../ui";
import { DATA_SECTION_ID } from "./ExportMenu";

/**
 * Download data — the export surface (v2).
 *
 * v1 was a `<select>` of nine options with a Download button beside it, and
 * the licensing assessment stacked underneath in three separate blocks. Two
 * things were wrong with that beyond its looks.
 *
 * **A `<select>` hides the answer to the question the section exists to ask.**
 * "Everything on this page, in the format you work in" is a question about
 * what OpenCheck can produce, and a closed dropdown answers it with one word.
 * Eleven chips answer it at a glance — and the format a reader wants is
 * usually recognised, not searched for.
 *
 * **Licensing is a condition of reuse, not a footnote to a download.** It was
 * three separate blocks trailing off the bottom of the section; it is now one
 * "You can reuse this" panel directly under the formats, where the choice is
 * being made. It sat *beside* the formats for one revision — the mockup's
 * 300px side column — which real data broke immediately: "Nigeria CAC —
 * Persons with Significant Control register" wrapped to seven lines, its
 * licence pill overflowed, and the section grew a thousand pixels of empty
 * space beside it. The traffic-light colours stay, but every one is also
 * spelled out in words — how restrictive a licence is must never be carried
 * by a dot alone (WCAG 1.4.1).
 *
 * The format list is exactly `_EXPORT_FORMATS` in `routers/export.py`. Nothing
 * is offered here that the backend cannot produce: an export picker that lists
 * an aspirational format is a 400 with extra steps.
 */

const COLOR: Record<"green" | "amber" | "red", string> = {
  green: "bg-emerald-50 text-emerald-700 border-emerald-200",
  amber: "bg-amber-50 text-amber-800 border-amber-200",
  red: "bg-red-50 text-red-700 border-red-300",
};
const DOT: Record<"green" | "amber" | "red", string> = {
  green: "text-emerald-500",
  amber: "text-amber-500",
  red: "text-red-500",
};

/** The picker's option set is the client's export-format type, so a format
 *  added on one side cannot be missed on the other. */
type Format = ExportFormat;

/**
 * Ordered by how many readers want them, not alphabetically. BODS JSON first
 * because it is what the page itself is made of.
 *
 The order comes from `EXPORT_FORMATS` (shared with the API reference on the
 * About page) and the labels from a `Record<ExportFormat, string>`, so adding
 * a format to the shared type without adding a chip fails the build. Typing
 * the array as `{value: Format}[]` — the first attempt — only checked that
 * each entry was *a* valid format, which is not the property that matters: a
 * missing chip compiles clean and the picker silently loses an export.
 */
const FORMAT_LABEL: Record<Format, string> = {
  json: "BODS JSON",
  zip: "ZIP bundle",
  csv: "CSV tables",
  xlsx: "Excel",
  jsonl: "JSONL",
  xml: "BODS XML",
  ftm: "FollowTheMoney",
  cypher: "Neo4j · Cypher",
  rdf: "RDF · TriG",
  senzing: "Senzing JSON",
  gql: "BigQuery · GQL",
  amlai: "Google AML AI",
};

const FORMATS: { value: Format; label: string }[] = EXPORT_FORMATS.map((value) => ({
  value,
  label: FORMAT_LABEL[value],
}));


const LINK = "underline text-oo-blue hover:text-oo-burst";

function Ext({ href, children }: { href: string; children: ReactNode }) {
  return (
    <a href={href} target="_blank" rel="noreferrer" className={LINK}>
      {children}
      <span className="sr-only"> (opens in new tab)</span>
    </a>
  );
}

/** One explanation per format, always rendered — a picker whose description
 *  appears for five of eleven options teaches the reader that the other six
 *  are the unremarkable ones, which is not true of Cypher or RDF. */
const BLURB: Record<Format, ReactNode> = {
  json: (
    <>
      BODS JSON is the format for the{" "}
      <Ext href="https://standard.openownership.org/en/0.4.0/">
        Beneficial Ownership Data Standard
      </Ext>{" "}
      — the same shape every source on this page was mapped into, so the file
      loads straight into other BODS tooling.
    </>
  ),
  zip: (
    <>
      The bundle: BODS as JSON, JSONL and XML, plus Senzing and FollowTheMoney
      projections, a manifest recording what ran, and the per-source licence
      notes. The one to take if you are archiving the check rather than
      querying it.
    </>
  ),
  csv: (
    <>
      Three flat tables — companies, people and the ownership or control edges
      between them — for a spreadsheet or a database load. The same tables the
      BigQuery package carries, without its schema and queries.
    </>
  ),
  xlsx: (
    <>
      The same three tables as one Excel workbook, a sheet each, with the
      licence notes on a fourth. Every cell is written as text, so a company
      number keeps its leading zeros instead of becoming a number.
    </>
  ),
  jsonl: (
    <>
      One BODS statement per line. The shape stream processors and{" "}
      <span className="font-mono">jq</span> want, and the one to use when the
      graph is too large to hold in memory as a single array.
    </>
  ),
  xml: (
    <>
      Canonical BODS XML, for pipelines that validate against a schema rather
      than parse JSON.
    </>
  ),
  cypher: (
    <>
      A{" "}
      <Ext href="https://neo4j.com/docs/cypher-manual/current/">Cypher</Ext>{" "}
      script that builds this graph in Neo4j — companies and people as nodes,
      each disclosed interest as a relationship. Paste it into the browser or
      pipe it through <span className="font-mono">cypher-shell</span>.
    </>
  ),
  senzing: (
    <>
      Senzing JSON projects this ownership graph into the{" "}
      <Ext href="https://www.senzing.com/docs/entity_specification/">
        Senzing entity specification
      </Ext>{" "}
      (newline-delimited records, ready to load for entity resolution) — one
      record per company and person, with each disclosed ownership/control
      relationship as a Senzing disclosed relationship.
    </>
  ),
  ftm: (
    <>
      FollowTheMoney projects this ownership graph into{" "}
      <Ext href="https://followthemoney.tech/">FtM entities</Ext>{" "}
      (newline-delimited) — companies and people as nodes, each disclosed
      interest as an Ownership or Directorship link — ready for OpenSanctions
      matching, OpenAleph/Aleph (via{" "}
      <span className="font-mono">alephclient write-entities</span>) and the{" "}
      <span className="font-mono">ftm</span> CLI.
    </>
  ),
  gql: (
    <>
      BigQuery GQL projects this ownership graph into a{" "}
      <Ext href="https://cloud.google.com/bigquery/docs/property-graphs">
        BigQuery property graph
      </Ext>{" "}
      queryable with{" "}
      <Ext href="https://www.gqlstandards.org/">GQL (ISO/IEC 39075)</Ext>. The
      zip holds node/edge CSV tables, the{" "}
      <span className="font-mono">CREATE PROPERTY GRAPH</span> schema and 14
      ready-made GQL queries (UBO detection, corporate groups, circular
      ownership) — generated with{" "}
      <Ext href="https://github.com/StephenAbbott/bods-gql">bods-gql</Ext>, with
      load instructions in its README.
    </>
  ),
  amlai: (
    <>
      Google AML AI projects this ownership graph into the{" "}
      <Ext href="https://docs.cloud.google.com/financial-services/anti-money-laundering/docs/reference/schemas/aml-input-data-model">
        AML AI input data model
      </Ext>{" "}
      — NDJSON tables ready for <span className="font-mono">bq load</span>. AML
      AI has no party-to-party relationship table, so ownership is encoded as
      numeric supplementary-data signals per party plus synthetic “ownership
      accounts” linking owners to owned entities — generated with{" "}
      <Ext href="https://github.com/StephenAbbott/bods-aml-ai">bods-aml-ai</Ext>,
      with the encoding explained in the bundled README.
    </>
  ),
  rdf: (
    <>
      RDF projects this ownership graph into{" "}
      <Ext href="https://vocab.openownership.org/pages/4_convertingdata.html">
        BODS RDF
      </Ext>{" "}
      (TriG, one named graph per statement, published{" "}
      <Ext href="https://vocab.openownership.org/terms/bods-vocabulary-0.4.0.ttl">
        BODS vocabulary
      </Ext>{" "}
      terms). Every statement carries its source&rsquo;s canonical licence URI.
      Two kinds of <span className="font-mono">bods:Annotation</span> travel
      with it, kept in separate named graphs so you can query either without the
      other: the register&rsquo;s own words — the nature-of-control code behind
      an interest, or a note that a birth date was published imprecise — sit in
      each statement&rsquo;s own graph, while OpenCheck&rsquo;s risk signals and
      entity-resolution links sit in a separate analysis graph. Queryable in
      SPARQL tools or directly in DuckDB via the community{" "}
      <span className="font-mono">rdf</span> extension.
    </>
  ),
};

export function ExportPanel({
  lei,
  legalName,
  contributingSourceIds,
}: {
  lei: string;
  legalName: string | null;
  contributingSourceIds: string[];
}) {
  const [format, setFormat] = useState<Format>("json");
  const chipRefs = useRef<Partial<Record<Format, HTMLButtonElement | null>>>({});
  const [subsidiaries, setSubsidiaries] = useState(false);

  const sorted = [...contributingSourceIds].sort();
  const licensing = useQuery({
    queryKey: ["license-matrix", sorted],
    queryFn: () => getLicenseMatrix(sorted),
    enabled: sorted.length > 0,
    staleTime: 60_000,
  });
  const a = licensing.data?.assessment;

  const href = exportUrl(lei, format, { subsidiaries });

  // Arrow keys move the selection and the focus together, which is what the
  // radiogroup pattern specifies and what makes the chips usable in a screen
  // reader's forms mode. Home/End jump to the ends; the group wraps.
  function onFormatKeyDown(e: React.KeyboardEvent<HTMLDivElement>) {
    const keys = ["ArrowRight", "ArrowDown", "ArrowLeft", "ArrowUp", "Home", "End"];
    if (!keys.includes(e.key)) return;
    e.preventDefault();
    const i = FORMATS.findIndex((f) => f.value === format);
    const last = FORMATS.length - 1;
    const next =
      e.key === "Home"
        ? 0
        : e.key === "End"
          ? last
          : e.key === "ArrowRight" || e.key === "ArrowDown"
            ? (i + 1) % FORMATS.length
            : (i - 1 + FORMATS.length) % FORMATS.length;
    const value = FORMATS[next].value;
    setFormat(value);
    chipRefs.current[value]?.focus();
  }

  return (
    <section
      id={DATA_SECTION_ID}
      className="px-4 py-[18px] sm:px-6 sm:py-[22px] scroll-mt-4"
    >
      <div className="mb-3 flex flex-col items-start gap-1 sm:flex-row sm:items-baseline sm:justify-between sm:gap-4">
        {/* tabIndex + data-export-target: the Export menu's "Download data"
            item scrolls here and moves focus to this heading, so keyboard
            and screen-reader users land where sighted users scrolled. */}
        <SectionHeading tabIndex={-1} data-export-target className="focus:outline-none">
          Download data
        </SectionHeading>
        <p className="text-oo-small text-oo-muted">
          Everything on this page, in the format you work in
        </p>
      </div>

      <div className="flex flex-col gap-5">
        <div className="flex flex-col gap-3">
          {/* A radiogroup rather than eleven buttons: they are one choice.
              That role is a promise about keyboard behaviour, and a first pass
              made it without keeping it — eleven tab stops, arrows dead, and a
              screen reader announcing "1 of 11" positions it could not move
              between. Worse than the `<select>` it replaced. So: one tab stop
              (roving tabIndex) and arrow keys that move *and* select, per the
              WAI-ARIA radiogroup pattern. */}
          <div
            role="radiogroup"
            aria-label="Export format"
            className="flex flex-wrap gap-2"
            onKeyDown={onFormatKeyDown}
          >
            {FORMATS.map((f) => {
              const on = f.value === format;
              return (
                <button
                  key={f.value}
                  type="button"
                  role="radio"
                  aria-checked={on}
                  tabIndex={on ? 0 : -1}
                  ref={(el) => {
                    chipRefs.current[f.value] = el;
                  }}
                  onClick={() => setFormat(f.value)}
                  className={`rounded-oo border px-3 py-1.5 text-oo-small transition-colors ${
                    on
                      ? "border-oo-softBorder bg-oo-soft text-oo-blue font-bold"
                      : "border-oo-rule bg-white text-oo-ink hover:border-oo-softBorder"
                  }`}
                >
                  {f.label}
                </button>
              );
            })}
          </div>

          <p className="text-oo-small text-oo-muted leading-[1.6] max-w-[70ch]">
            {BLURB[format]}
          </p>

          <div className="flex items-center flex-wrap gap-3">
            <a
              href={href}
              download
              className="shrink-0 whitespace-nowrap bg-oo-blue text-white text-oo-small font-bold rounded-oo px-4 py-2.5 hover:bg-oo-burst transition-colors inline-block"
            >
              Download
            </a>
            <label className="flex items-center gap-2 text-oo-small text-oo-muted cursor-pointer select-none">
              <input
                type="checkbox"
                checked={subsidiaries}
                onChange={(e) => setSubsidiaries(e.target.checked)}
                className="accent-oo-blue"
              />
              Include the GLEIF subsidiary network
            </label>
          </div>
          <p className="text-oo-meta text-oo-muted leading-[1.6] max-w-[70ch]">
            Reproducible export for{" "}
            {legalName ? <span>{legalName} (</span> : null}
            <span className="font-mono">{lei}</span>
            {legalName ? <span>)</span> : null}. The subsidiary network is off
            by default — a large corporate group can add hundreds of statements.
          </p>
        </div>

        {a && (
          // Under the formats at every width, not beside them.
          //
          // A 300px column beside the chips looked right in the mockup, which
          // had four short licence rows. Real ones are not short — "Nigeria CAC
          // — Persons with Significant Control register" wrapped to seven
          // lines, its licence pill overflowed the column, and the section grew
          // a thousand pixels of empty space to the left of it. The content
          // decides the layout: this is a list of sources with terms attached,
          // and a list wants width.
          <aside className="rounded-oo border border-oo-rule bg-oo-bg px-4 py-3.5" role="status">
            <div className="flex flex-col gap-1 sm:flex-row sm:items-baseline sm:justify-between sm:gap-4">
              <p className="text-oo-small font-bold text-oo-ink">You can reuse this</p>
              <p className="text-oo-small text-oo-muted leading-[1.5]">
                Commercial use: <strong>{a.commercial_use}</strong> · Attribution:{" "}
                <strong>{a.attribution_required ? "required" : "not required"}</strong>
                {a.share_alike ? (
                  <>
                    {" "}· <strong>share-alike</strong>
                  </>
                ) : null}
              </p>
            </div>
            <p className="text-oo-small text-oo-muted mt-0.5 leading-[1.5]">{a.headline}</p>
            {a.warnings.map((w, i) => (
              <p
                key={i}
                className={`text-oo-meta mt-2 leading-[1.5] rounded-oo border px-2.5 py-1.5 ${COLOR[a.color]}`}
              >
                {w}
              </p>
            ))}

            {/* One row per source: name, its licence, and the plain-English
                terms under both. The terms are not in a `title` because a
                licence condition is the thing a reuser has to comply with, and
                the colour is never the only cue for how restrictive it is. */}
            <dl className="mt-3 grid gap-x-6 gap-y-2 sm:grid-cols-2">
              {a.per_source.map((s) => (
                <div key={s.source_id} className="min-w-0">
                  <div className="flex items-baseline flex-wrap gap-x-2 gap-y-1 text-oo-small">
                    <dt className="text-oo-burst">{s.name}</dt>
                    <dd
                      className={`inline-flex items-center gap-1 border rounded px-1.5 py-0.5 text-oo-meta font-mono break-all ${COLOR[s.terms.color]}`}
                    >
                      <span className={DOT[s.terms.color]} aria-hidden="true">
                        ●
                      </span>
                      {s.terms.license}
                    </dd>
                  </div>
                  {s.terms.summary && (
                    <p className="text-oo-meta text-oo-muted leading-[1.5]">
                      {s.terms.summary}
                    </p>
                  )}
                </div>
              ))}
            </dl>
            <p className="text-oo-meta text-oo-muted mt-3 leading-[1.5]">{a.disclaimer}</p>
          </aside>
        )}
      </div>
    </section>
  );
}
