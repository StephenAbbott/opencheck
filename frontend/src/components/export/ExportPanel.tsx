import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { exportUrl, getLicenseMatrix } from "../../lib/api";

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
    "zip" | "json" | "jsonl" | "xml" | "senzing" | "ftm"
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
    <section className="mb-8 bg-white border border-oo-rule rounded-oo p-5">
      <div className="flex items-baseline justify-between gap-4 flex-wrap">
        <div className="min-w-0">
          <h2 className="font-head font-bold text-[15px] text-oo-ink">
            Download BODS bundle
          </h2>
          <p className="text-[13px] text-oo-muted mt-1 leading-[1.6]">
            Reproducible export for{" "}
            {legalName ? <span>{legalName} (</span> : null}
            <span className="font-mono">{lei}</span>
            {legalName ? <span>)</span> : null}. Includes BODS statements,
            manifest, and per-source licence notes.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <select
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
              )
            }
            className="border border-oo-rule rounded px-2 py-1.5 text-[13px] bg-white"
          >
            <option value="zip">ZIP (bods + manifest + licenses)</option>
            <option value="json">JSON (BODS array)</option>
            <option value="jsonl">JSONL (newline-delimited)</option>
            <option value="xml">XML (canonical BODS)</option>
            <option value="senzing">Senzing JSON (entity resolution)</option>
            <option value="ftm">FollowTheMoney (OpenSanctions / Aleph)</option>
          </select>
          <a
            href={href}
            download
            className="bg-oo-blue text-white text-[13px] font-medium rounded px-4 py-1.5 hover:bg-oo-burst transition-colors inline-block"
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
          </a>{" "}
          (newline-delimited records, ready to load for entity resolution) — one
          record per company and person, with each disclosed ownership/control
          relationship as a Senzing disclosed relationship.
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
