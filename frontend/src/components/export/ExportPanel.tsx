import { useState } from "react";
import { exportUrl } from "../../lib/api";

/**
 * Download button + format selector that points at /export.
 *
 * Renders an in-place NC-license warning when any contributing source
 * carries a CC BY-NC clause.
 */
export function ExportPanel({
  lei,
  legalName,
  sourceLicenses,
  contributingSourceIds,
}: {
  lei: string;
  legalName: string | null;
  sourceLicenses: Record<string, string>;
  contributingSourceIds: string[];
}) {
  const [format, setFormat] = useState<"zip" | "json" | "jsonl" | "xml">("zip");

  const ncSources = contributingSourceIds.filter((id) =>
    (sourceLicenses[id] ?? "").toLowerCase().includes("nc")
  );

  const href = exportUrl(lei, format);

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
            {legalName ? <span>)</span> : null}. Includes BODS
            statements, manifest, and per-source license notes.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <select
            value={format}
            onChange={(e) =>
              setFormat(e.target.value as "zip" | "json" | "jsonl" | "xml")
            }
            className="border border-oo-rule rounded px-2 py-1.5 text-[13px] bg-white"
          >
            <option value="zip">ZIP (bods + manifest + licenses)</option>
            <option value="json">JSON (BODS array)</option>
            <option value="jsonl">JSONL (newline-delimited)</option>
            <option value="xml">XML (canonical BODS)</option>
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
      {ncSources.length > 0 && (
        <p className="mt-3 text-[12px] bg-amber-50 border border-amber-200 text-amber-900 rounded-oo px-3 py-2 leading-[1.6]">
          <span className="font-head font-bold">License notice.</span>{" "}
          This bundle will include data from {ncSources.join(", ")} (CC
          BY-NC). The combined dataset inherits the non-commercial
          restriction — re-publication or commercial use is not
          permitted under the source license. See{" "}
          <span className="font-mono">LICENSES.md</span> inside the zip
          for details.
        </p>
      )}
    </section>
  );
}
