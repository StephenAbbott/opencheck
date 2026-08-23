import { describe, it, expect } from "vitest";

import { EXPORT_FORMATS, exportUrl } from "./api";

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
        "senzing", "xml", "zip",
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
