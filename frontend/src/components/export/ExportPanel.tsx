import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { exportUrl, getLicenseMatrix } from "../../lib/api";
import { DATA_SECTION_ID } from "./ExportMenu";

/**
 * Download button + format selector that points at /export, plus a licensing
 * assistant: a traffic-light compatibility verdict for the sources that
 * contributed to this result (commercial use / attribution / share-alike).
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

export function ExportPanel({
  lei,
  legalName,
  contributingSourceIds,
}: {
  lei: string;
  legalName: string | null;
  contributingSourceIds: string[];
}) {
  const [format, setFormat] = useState<
    "zip" | "json" | "jsonl" | "xml" | "senzing" | "ftm" | "gql" | "amlai" | "rdf"
  >("zip");
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

  return (
    <section
      id={DATA_SECTION_ID}
      className="mb-8 bg-white border border-oo-rule rounded-oo p-5 scroll-mt-4"
    >
      <div className="flex items-baseline justify-between gap-4 flex-wrap">
        <div className="min-w-0">
          {/* tabIndex + data-export-target: the Export menu's "Download data"
              item scrolls here and moves focus to this heading, so keyboard
              and screen-reader users land where sighted users scrolled. */}
          <h2
            tabIndex={-1}
            data-export-target
            className="font-head font-bold text-[15px] text-oo-ink focus:outline-none"
          >
            Download data
          </h2>
          <p className="text-[13px] text-oo-muted mt-1 leading-[1.6]">
            Reproducible export for{" "}
            {legalName ? <span>{legalName} (</span> : null}
            <span className="font-mono">{lei}</span>
            {legalName ? <span>)</span> : null}. Includes BODS statements,
            manifest, and per-source licence notes.
          </p>
        </div>
        {/* w-full on mobile so the control row owns its own line and the
            select can shrink; min-w-0 lets flex actually shrink it below the
            intrinsic width of the longest <option>, which otherwise pushes
            the Download button off-screen on narrow viewports. */}
        <div className="flex items-center gap-2 w-full sm:w-auto min-w-0">
          <select
            aria-label="Export format"
            value={format}
            onChange={(e) =>
              setFormat(
                e.target.value as
                  | "zip"
                  | "json"
                  | "jsonl"
                  | "xml"
                  | "senzing"
                  | "ftm"
                  | "gql"
                  | "amlai"
                  | "rdf"
              )
            }
            className="min-w-0 flex-1 sm:flex-none border border-oo-rule rounded px-2 py-1.5 text-[13px] bg-white"
          >
            <option value="zip">ZIP (bods + manifest + licenses)</option>
            <option value="json">JSON (BODS array)</option>
            <option value="jsonl">JSONL (newline-delimited)</option>
            <option value="xml">XML (canonical BODS)</option>
            <option value="senzing">Senzing JSON (entity resolution)</option>
            <option value="ftm">FollowTheMoney (OpenSanctions / Aleph)</option>
            <option value="gql">BigQuery GQL (CSV tables + graph schema)</option>
            <option value="amlai">Google AML AI (NDJSON input tables)</option>
            <option value="rdf">RDF (BODS TriG, linked open data)</option>
          </select>
          <a
            href={href}
            download
            className="shrink-0 whitespace-nowrap bg-oo-blue text-white text-[13px] font-medium rounded px-4 py-1.5 hover:bg-oo-burst transition-colors inline-block"
          >
            Download
          </a>
        </div>
      </div>

      {format === "senzing" && (
        <p className="mt-3 text-[12px] text-oo-muted leading-[1.6]">
          Senzing JSON projects this ownership graph into the{" "}
          <a
            href="https://www.senzing.com/docs/entity_specification/"
            target="_blank"
            rel="noreferrer"
            className="underline text-oo-blue hover:text-oo-burst"
          >
            Senzing entity specification
            <span className="sr-only"> (opens in new tab)</span>
          </a>{" "}
          (newline-delimited records, ready to load for entity resolution) — one
          record per company and person, with each disclosed ownership/control
          relationship as a Senzing disclosed relationship.
        </p>
      )}

      {format === "gql" && (
        <p className="mt-3 text-[12px] text-oo-muted leading-[1.6]">
          BigQuery GQL projects this ownership graph into a{" "}
          <a
            href="https://cloud.google.com/bigquery/docs/property-graphs"
            target="_blank"
            rel="noreferrer"
            className="underline text-oo-blue hover:text-oo-burst"
          >
            BigQuery property graph
            <span className="sr-only"> (opens in new tab)</span>
          </a>{" "}
          queryable with{" "}
          <a
            href="https://www.gqlstandards.org/"
            target="_blank"
            rel="noreferrer"
            className="underline text-oo-blue hover:text-oo-burst"
          >
            GQL (ISO/IEC 39075)
            <span className="sr-only"> (opens in new tab)</span>
          </a>
          . The zip holds node/edge CSV tables, the{" "}
          <span className="font-mono">CREATE PROPERTY GRAPH</span> schema and 14
          ready-made GQL queries (UBO detection, corporate groups, circular
          ownership) — generated with{" "}
          <a
            href="https://github.com/StephenAbbott/bods-gql"
            target="_blank"
            rel="noreferrer"
            className="underline text-oo-blue hover:text-oo-burst"
          >
            bods-gql
            <span className="sr-only"> (opens in new tab)</span>
          </a>
          , with load instructions in its README.
        </p>
      )}

      {format === "amlai" && (
        <p className="mt-3 text-[12px] text-oo-muted leading-[1.6]">
          Google AML AI projects this ownership graph into the{" "}
          <a
            href="https://docs.cloud.google.com/financial-services/anti-money-laundering/docs/reference/schemas/aml-input-data-model"
            target="_blank"
            rel="noreferrer"
            className="underline text-oo-blue hover:text-oo-burst"
          >
            AML AI input data model
            <span className="sr-only"> (opens in new tab)</span>
          </a>{" "}
          — NDJSON tables ready for{" "}
          <span className="font-mono">bq load</span>. AML AI has no
          party-to-party relationship table, so ownership is encoded as numeric
          supplementary-data signals per party plus synthetic “ownership
          accounts” linking owners to owned entities — generated with{" "}
          <a
            href="https://github.com/StephenAbbott/bods-aml-ai"
            target="_blank"
            rel="noreferrer"
            className="underline text-oo-blue hover:text-oo-burst"
          >
            bods-aml-ai
            <span className="sr-only"> (opens in new tab)</span>
          </a>
          , with the encoding explained in the bundled README.
        </p>
      )}

      {format === "rdf" && (
        <p className="mt-3 text-[12px] text-oo-muted leading-[1.6]">
          RDF projects this ownership graph into{" "}
          <a
            href="https://vocab.openownership.org/pages/4_convertingdata.html"
            target="_blank"
            rel="noreferrer"
            className="underline text-oo-blue hover:text-oo-burst"
          >
            BODS RDF
            <span className="sr-only"> (opens in new tab)</span>
          </a>{" "}
          (TriG, one named graph per statement, published{" "}
          <a
            href="https://vocab.openownership.org/terms/bods-vocabulary-0.4.0.ttl"
            target="_blank"
            rel="noreferrer"
            className="underline text-oo-blue hover:text-oo-burst"
          >
            BODS vocabulary
            <span className="sr-only"> (opens in new tab)</span>
          </a>{" "}
          terms). Every statement carries its source&rsquo;s canonical licence URI, and
          OpenCheck&rsquo;s risk signals and entity-resolution links travel as{" "}
          <span className="font-mono">bods:Annotation</span> overlays in a separate
          named analysis graph — queryable in SPARQL tools or directly in DuckDB via
          the community <span className="font-mono">rdf</span> extension.
        </p>
      )}

      {format === "ftm" && (
        <p className="mt-3 text-[12px] text-oo-muted leading-[1.6]">
          FollowTheMoney projects this ownership graph into{" "}
          <a
            href="https://followthemoney.tech/"
            target="_blank"
            rel="noreferrer"
            className="underline text-oo-blue hover:text-oo-burst"
          >
            FtM entities
            <span className="sr-only"> (opens in new tab)</span>
          </a>{" "}
          (newline-delimited) — companies and people as nodes, each disclosed
          interest as an Ownership or Directorship link — ready for
          OpenSanctions matching, OpenAleph/Aleph (via{" "}
          <span className="font-mono">alephclient write-entities</span>) and the{" "}
          <span className="font-mono">ftm</span> CLI.
        </p>
      )}

      <label className="mt-3 flex items-start gap-2 text-[12px] text-oo-muted cursor-pointer select-none">
        <input
          type="checkbox"
          checked={subsidiaries}
          onChange={(e) => setSubsidiaries(e.target.checked)}
          className="mt-0.5 accent-oo-blue"
        />
        <span>
          Include the GLEIF subsidiary network (direct &amp; ultimate children).
          Off by default — a large corporate group can add hundreds of statements.
        </span>
      </label>

      {a && (
        <div className="mt-4">
          {/* Overall verdict */}
          <div
            className={`rounded-oo border px-3 py-2 ${COLOR[a.color]}`}
            role="status"
          >
            <p className="text-[13px] font-head font-bold leading-[1.5]">
              <span className={DOT[a.color]} aria-hidden="true">
                ●
              </span>{" "}
              {a.headline}
            </p>
            <p className="text-[12px] mt-0.5 leading-[1.5] opacity-90">
              Commercial use: <strong>{a.commercial_use}</strong> · Attribution:{" "}
              <strong>{a.attribution_required ? "required" : "not required"}</strong>
              {a.share_alike ? (
                <>
                  {" "}· <strong>share-alike</strong>
                </>
              ) : null}
            </p>
            {a.warnings.map((w, i) => (
              <p key={i} className="text-[12px] mt-1 leading-[1.5]">
                ⚠️ {w}
              </p>
            ))}
          </div>

          {/* Per-source traffic lights */}
          <div className="mt-3 flex flex-wrap gap-1.5">
            {a.per_source.map((s) => (
              <span
                key={s.source_id}
                title={s.terms.summary}
                className={`inline-flex items-center gap-1 text-[11px] border rounded px-1.5 py-0.5 ${COLOR[s.terms.color]}`}
              >
                <span className={DOT[s.terms.color]} aria-hidden="true">
                  ●
                </span>
                {s.name}
                <span className="font-mono opacity-70">{s.terms.license}</span>
              </span>
            ))}
          </div>

          <p className="text-[11px] text-oo-muted mt-2 leading-[1.5]">
            {a.disclaimer}
          </p>
        </div>
      )}
    </section>
  );
}
