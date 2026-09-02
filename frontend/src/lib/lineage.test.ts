/**
 * lineage — the browser copy of the backend's source-lineage rules. The
 * cases mirror backend/tests/test_lineage.py so the two cannot count
 * corroboration differently.
 */
import { describe, expect, it } from "vitest";

import { independent, independentCount, independentSources, toSourceId } from "./lineage";
import { countLeiConfirmingSources } from "./identifierBadge";

describe("lineage", () => {
  it("resolves mapper description labels to source ids", () => {
    expect(toSourceId("UK Companies House")).toBe("companies_house");
    expect(toSourceId("OpenCorporates")).toBe("opencorporates");
    expect(toSourceId("GLEIF")).toBe("gleif");
    // Ids pass through; unknown labels are original sources, never a crash.
    expect(toSourceId("gleif")).toBe("gleif");
    expect(toSourceId("Some Future Source")).toBe("Some Future Source");
  });

  it("a source is never independent of itself, nor of what it republishes", () => {
    expect(independent("gleif", "gleif")).toBe(false);
    expect(independent("opencorporates", "companies_house")).toBe(false);
    expect(independent("UK Companies House", "OpenCorporates")).toBe(false);
    expect(independent("opensanctions", "gleif")).toBe(false);
    expect(independent("everypolitician", "gleif")).toBe(false); // transitive
    expect(independent("opencorporates", "openaleph")).toBe(false); // shared upstream
    expect(independent("gleif", "wikidata")).toBe(true);
    expect(independent("companies_house", "cvr_denmark")).toBe(true);
  });

  it("collapses the Novo Nordisk and Shell source sets to their origins", () => {
    // Live exports, 2 Sept 2026 — by description label, as the network sees them.
    expect(
      independentSources([
        "GLEIF",
        "OpenSanctions",
        "Wikidata",
        "OpenCorporates",
        "CVR — Det Centrale Virksomhedsregister (Danish Business Authority)",
      ])
    ).toEqual(["cvr_denmark", "gleif", "wikidata"]);
    expect(
      independentSources(["GLEIF", "OpenAleph", "OpenCorporates", "Wikidata", "UK Companies House"])
    ).toEqual(["companies_house", "gleif", "wikidata"]);
    // The 22 Shell officers: Companies House twice is one observation.
    expect(independentCount(["UK Companies House", "OpenCorporates"])).toBe(1);
    expect(independentCount(["gleif", "opensanctions"])).toBe(1);
    expect(independentCount(["opencorporates", "openaleph"])).toBe(1);
    expect(independentCount(["opencorporates"])).toBe(1);
    expect(independentCount([])).toBe(0);
    expect(independentCount(["", "gleif", "gleif"])).toBe(1);
  });

  it("the LEI-confirmation badge counts independent origins", () => {
    const lei = "21380068P1DRHMJ8KU70";
    const link = (ids: string[]) => ({
      key: "lei",
      key_value: lei,
      confidence: "strong" as const,
      hits: ids.map((source_id) => ({ source_id, hit_id: source_id, name: source_id })),
    });
    expect(countLeiConfirmingSources([link(["gleif", "opensanctions"])], lei)).toBe(1);
    expect(countLeiConfirmingSources([link(["gleif", "opensanctions", "wikidata"])], lei)).toBe(2);
    expect(countLeiConfirmingSources([link(["gleif", "wikidata", "openaleph"])], lei)).toBe(2);
  });
});
