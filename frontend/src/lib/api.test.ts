import { describe, it, expect } from "vitest";

import { EXPORT_FORMATS, exportUrl, leiInputMessage, reportRequestBody, savedReportShareUrl } from "./api";

/**
 * Wiring tests for the export URL builder. These pin the request the Export
 * panel's format selector produces — in particular the Senzing JSON option
 * added alongside the BODS formats. (The repo has no DOM render harness, so we
 * assert the URL the button points at rather than rendering the component.)
 */

const _LEI = "21380068P1DRHMJ8KU70"; // Shell plc

describe("exportUrl", () => {
  it("builds a Senzing export request", () => {
    const url = exportUrl(_LEI, "senzing");
    expect(url).toContain("/export?");
    expect(url).toContain(`lei=${_LEI}`);
    expect(url).toContain("format=senzing");
    expect(url).not.toContain("subsidiaries");
  });

  it("combines the Senzing format with the subsidiaries opt-in", () => {
    const url = exportUrl(_LEI, "senzing", { subsidiaries: true });
    expect(url).toContain("format=senzing");
    expect(url).toContain("subsidiaries=true");
  });

  it("still builds the existing BODS formats unchanged", () => {
    for (const fmt of ["zip", "json", "jsonl", "xml"] as const) {
      expect(exportUrl(_LEI, fmt)).toContain(`format=${fmt}`);
    }
  });
});

it("builds an rdf export URL", () => {
  const url = exportUrl(_LEI, "rdf");
  expect(url).toContain("/export?");
  expect(url).toContain("format=rdf");
});

describe("EXPORT_FORMATS", () => {
  it("is exactly the backend's _EXPORT_FORMATS set", () => {
    // backend/opencheck/routers/export.py. Two frontend surfaces read this —
    // the download picker and the API reference on the About page — and the
    // reference has drifted from the backend twice. A chip that names a format
    // the backend rejects is a 400 with extra steps; a reference that omits
    // one under-reports the API.
    expect([...EXPORT_FORMATS].sort()).toEqual(
      [
        "amlai", "csv", "cypher", "ftm", "gql", "json", "jsonl", "rdf",
        "senzing", "xlsx", "xml", "zip",
      ].sort()
    );
  });

  it("lists each format once", () => {
    expect(new Set(EXPORT_FORMATS).size).toBe(EXPORT_FORMATS.length);
  });

  it("leads with the format the page itself is made of", () => {
    expect(EXPORT_FORMATS[0]).toBe("json");
  });
});

describe("downloads from a saved report (Phase 218)", () => {
  const ID = "SU82_KMkQo2QbEv3Kcfm8A";

  it("names the saved report and never adds the subsidiary network", () => {
    const url = new URL(exportUrl(_LEI, "zip", { subsidiaries: true, savedReportId: ID }), "https://x.test");
    expect(url.searchParams.get("saved_report_id")).toBe(ID);
    expect(url.searchParams.get("lei")).toBe(_LEI);
    expect(url.searchParams.has("subsidiaries")).toBe(false);
  });

  it("posts only the LEI and the saved report id for a report, never a narrative", () => {
    const narrative = { summary: "x" } as never;
    expect(reportRequestBody(_LEI, narrative, null, ID)).toEqual({ lei: _LEI, saved_report_id: ID });
    expect(reportRequestBody(_LEI, narrative, null, null)).toEqual({ lei: _LEI, narrative, dispositions: null });
  });

  it("shares a saved report through its own share page", () => {
    expect(savedReportShareUrl(ID)).toMatch(new RegExp(`/share/saved/${ID}$`));
  });
});

describe("leiInputMessage (Phase 241)", () => {
  it("accepts a well-formed LEI, whatever its case and surrounding space", () => {
    expect(leiInputMessage(" 213800lh1bzh3di6g760 ")).toBeNull();
  });
  it("says what is wrong, not only that something is", () => {
    expect(leiInputMessage("")).toBe("Paste an LEI: 20 letters and digits.");
    expect(leiInputMessage("2138-00LH1BZH3DI6G76")).toBe("An LEI holds only letters and digits.");
    expect(leiInputMessage("213800LH1BZH3DI6G7")).toBe("An LEI is 20 characters long; this is 18.");
  });
});
