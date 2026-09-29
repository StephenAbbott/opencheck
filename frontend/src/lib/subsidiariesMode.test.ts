import { describe, expect, it } from "vitest";
import type { DeclaredSource, SubsidiaryChild } from "./api";
import {
  countrySpread,
  coverageSentence,
  directParentLine,
  enrichmentNote,
  meipContextLine,
  openableSentence,
  orderRows,
  relationshipDates,
  resolveLists,
  subsidiaryHref,
  subsidiaryRowId,
} from "./subsidiariesMode";

function src(id: DeclaredSource["id"], rows: Partial<DeclaredSource["rows"][number]>[], over: Partial<DeclaredSource> = {}): DeclaredSource {
  return {
    id,
    label: id,
    measures: "m",
    homepage: "https://example.org",
    available: true,
    covered: true,
    reason: null,
    total: null,
    listed: rows.length,
    with_lei: rows.filter((r) => r.lei).length,
    context: null,
    rows: rows.map((r) => ({
      name: r.name ?? "",
      lei: r.lei ?? null,
      country: r.country ?? null,
      relation: r.relation ?? null,
      percent: r.percent ?? null,
      years: r.years ?? [],
      via: r.via ?? null,
    })),
    ...over,
  };
}

function child(name: string, lei: string, relation: SubsidiaryChild["relation"] = "direct"): SubsidiaryChild {
  return { name, lei, jurisdiction: "NO", status: "ACTIVE", relation, link: null };
}

const NORSKE = "213800F4ETX85XLF5K47";

describe("subsidiaryHref", () => {
  it("addresses the Subsidiaries tab of a company", () => {
    expect(subsidiaryHref("21380068P1DRHMJ8KU70")).toBe("/?lei=21380068P1DRHMJ8KU70&mode=subsidiaries");
  });
});

describe("resolveLists", () => {
  it("attaches a name-derived LEI from another list, and says where it came from", () => {
    const eiti = src("eiti_assessment", [{ name: "A/S Norske Shell", country: "NOR" }]);
    const meip = src("meip", [{ name: "A/S NORSKE SHELL", lei: NORSKE, country: "NOR" }]);
    const cov = resolveLists([meip, eiti], null);
    const eitiRow = cov.lists.find((l) => l.id === "eiti_assessment")!.rows[0];
    expect(eitiRow.lei).toBeNull();
    expect(eitiRow.matched).toEqual({ lei: NORSKE, from: "meip" });
    expect(eitiRow.alsoIn).toEqual(["meip"]);
    const meipRow = cov.lists.find((l) => l.id === "meip")!.rows[0];
    expect(meipRow.matched).toBeNull();
    expect(meipRow.alsoIn).toEqual(["eiti_assessment"]);
    expect(cov.agreedNames).toBe(1);
    expect(cov.distinctNames).toBe(1);
    expect(cov.openable).toBe(2);
  });

  it("never lets a fetch failure read as GLEIF holding none of them", () => {
    const eiti = src("eiti_assessment", [{ name: "Newton Energy Limited" }]);
    const cov = resolveLists([eiti], null);
    expect(cov.lists.map((l) => l.id)).toEqual(["eiti_assessment"]);
    expect(cov.lists[0].rows[0].alsoIn).toEqual([]);
  });

  it("leaves out a source that is not covered or not available", () => {
    const cov = resolveLists(
      [
        src("meip", [], { covered: false, reason: "not in the MEIP register" }),
        src("climatetrace", [{ name: "X" }], { available: false, reason: "not loaded" }),
      ],
      [child("Y", NORSKE)],
    );
    expect(cov.lists.map((l) => l.id)).toEqual(["gleif"]);
  });

  it("prefers the row's own LEI over a name match and counts the two apart", () => {
    const gleif = [child("Shell Energy North America", "5493001KJTIIGC8Y1R12")];
    const gem = src("climatetrace", [
      { name: "Shell Energy North America", lei: "5493001KJTIIGC8Y1R12", percent: 100 },
      { name: "No-LEI Sub" },
      { name: "shell energy north america" }, // same name, no LEI
    ]);
    const cov = resolveLists([gem], gleif);
    const gemList = cov.lists.find((l) => l.id === "climatetrace")!;
    expect(gemList.withLei).toBe(1);
    expect(gemList.matched).toBe(1);
    expect(gemList.openable).toBe(2);
    expect(gemList.rows[2].matched?.from).toBe("gleif");
  });

  it("carries the GLEIF relation and jurisdiction through", () => {
    const cov = resolveLists([], [child("A", NORSKE, "both")]);
    expect(cov.lists[0].rows[0]).toMatchObject({ relation: "both", country: "NO", lei: NORSKE });
  });
});

describe("orderRows", () => {
  it("puts openable rows first, then larger holdings, then names", () => {
    const cov = resolveLists(
      [
        src("climatetrace", [
          { name: "C no lei", percent: 90 },
          { name: "B", lei: NORSKE, percent: 10 },
          { name: "A", lei: "5493001KJTIIGC8Y1R12", percent: 60 },
        ]),
      ],
      null,
    );
    expect(orderRows(cov.lists[0].rows).map((r) => r.name)).toEqual(["A", "B", "C no lei"]);
  });
});

describe("coverageSentence", () => {
  it("says what the disagreement means rather than hiding it", () => {
    const cov = resolveLists(
      [
        src("meip", [{ name: "A", lei: NORSKE }, { name: "B", lei: "5493001KJTIIGC8Y1R12" }]),
        src("eiti_assessment", [{ name: "B" }, { name: "C" }]),
      ],
      [child("D", "529900T8BM49AURSDO55")],
    );
    const s = coverageSentence(cov, "Shell plc");
    expect(s).toMatch(/^Three sources list what Shell plc owns — 4 distinct names across 5 rows/);
    expect(s).toContain("only 1 name appears in more than one of them");
    expect(s).toContain("each measures something different");
    expect(s).not.toMatch(/complete|comprehensive|guarantee/i);
  });

  it("does not call an absence of records a finding", () => {
    const s = coverageSentence(resolveLists([], null), "Acme");
    expect(s).toContain("not a finding that it owns nothing");
  });

  it("says a covered-but-empty company is an absence of records, not zero across zero", () => {
    // A/S Norske Shell on production: GLEIF, MEIP and GEM all know it and
    // none lists a subsidiary. The sentence must not do arithmetic on nothing.
    const cov = resolveLists([src("meip", []), src("climatetrace", [])], []);
    const s = coverageSentence(cov, "A/S Norske Shell");
    expect(s).toBe(
      "Three sources cover A/S Norske Shell, and none of them lists anything it owns. That is an absence of records, not a finding that it owns nothing.",
    );
    expect(s).not.toMatch(/0 distinct|0 rows/);
    const one = coverageSentence(resolveLists([src("meip", [])], null), "Acme");
    expect(one).toMatch(/^One source covers Acme, and it lists nothing it owns/);
  });

  it("does not compare a single list against nothing", () => {
    const s = coverageSentence(resolveLists([src("eiti_assessment", [{ name: "A" }])], null), "Acme");
    expect(s).toMatch(/^One source lists/);
    expect(s).toContain("No second source");
  });
});

describe("openableSentence", () => {
  it("counts the name matches separately and marks them as such", () => {
    const cov = resolveLists(
      [src("meip", [{ name: "A", lei: NORSKE }]), src("eiti_assessment", [{ name: "a" }, { name: "B" }])],
      null,
    );
    expect(openableSentence(cov)).toBe(
      "2 of 3 rows can be opened in OpenCheck — 1 of them by a name match to another list, marked as such.",
    );
  });

  it("is honest when nothing can be opened", () => {
    const cov = resolveLists([src("eiti_assessment", [{ name: "A" }])], null);
    expect(openableSentence(cov)).toContain("none can be opened");
  });
});

describe("meipContextLine — the MEIP band's opening sentence (Phase 208)", () => {
  const edition = "2024-12-31";

  it("names the group a member is listed in, and the edition, without an ownership verb", () => {
    const s = meipContextLine({ mode: "subsidiary", parent_mne: "SHELL PLC", immediate_parent: "SHELL PLC", edition, complete: true, memberships: 1 });
    expect(s).toBe("Listed in the SHELL PLC group, register of 31 December 2024. ");
    expect(s).not.toMatch(/\bown(s|ed|ership)?\b/i);
  });

  it("adds the immediate parent when the spreadsheet names one below the head", () => {
    const s = meipContextLine({ mode: "subsidiary", parent_mne: "SHELL PLC", immediate_parent: "Shell Petroleum N.V.", edition, complete: true });
    expect(s).toBe("Listed in the SHELL PLC group, immediate parent Shell Petroleum N.V., register of 31 December 2024. ");
  });

  it("says a company is in two groups rather than picking one (decision 6)", () => {
    const s = meipContextLine({ mode: "subsidiary", parent_mne: "AXA", memberships: 2, edition, complete: true });
    expect(s).toBe("Listed in 2 groups, including AXA, register of 31 December 2024. ");
  });

  it("describes a group head as one of the 500", () => {
    const s = meipContextLine({ mode: "mne_head", parent_mne: "SHELL PLC", edition, complete: true });
    expect(s).toBe("One of the 500 largest multinational enterprise groups, register of 31 December 2024. ");
  });

  it("says out loud when the list is the JSON fallback's LEI-carrying subset", () => {
    const s = meipContextLine({ mode: "mne_head", complete: false });
    expect(s).toBe("One of the 500 largest multinational enterprise groups. Only the members that carry an LEI are listed here. ");
  });

  it("degrades to nothing on an unknown context", () => {
    expect(meipContextLine(null)).toBe("");
    expect(meipContextLine({ mode: "subsidiary" })).toBe("");
  });
});

// ---------------------------------------------------------------------
// Phase 261 — the GLEIF rows' own fields
// ---------------------------------------------------------------------

describe("countrySpread (Phase 261)", () => {
  it("prefers the backend's country roll-up", () => {
    const spread = countrySpread({
      jurisdictions: [{ code: "US-DE", count: 17 }, { code: "US", count: 1 }],
      countries: [{ code: "US", count: 18 }],
    });
    expect(spread).toEqual([{ code: "US", count: 18 }]);
  });

  it("rolls subdivisions up itself for a backend without `countries` — Shell's CA and US", () => {
    const spread = countrySpread({
      jurisdictions: [
        { code: "GB", count: 40 },
        { code: "US-DE", count: 17 },
        { code: "CA-AB", count: 6 },
        { code: "CA", count: 5 },
        { code: "US", count: 1 },
      ],
    });
    expect(spread).toEqual([
      { code: "GB", count: 40 },
      { code: "US", count: 18 },
      { code: "CA", count: 11 },
    ]);
  });
});

describe("relationshipDates (Phase 261)", () => {
  it("states the start of the consolidation", () => {
    expect(relationshipDates({ relationship_start: "2013-11-01" })).toBe("Consolidated since 1 Nov 2013");
  });
  it("states an end, with or without a start", () => {
    expect(relationshipDates({ relationship_start: "2013-11-01", relationship_end: "2021-03-03" })).toBe(
      "Consolidated 1 Nov 2013 – ended 3 Mar 2021",
    );
    expect(relationshipDates({ relationship_end: "2021-03-03" })).toBe("Consolidation ended 3 Mar 2021");
  });
  it("says nothing when GLEIF gave no period", () => {
    expect(relationshipDates({})).toBeNull();
    expect(relationshipDates({ relationship_start: null, relationship_end: null })).toBeNull();
  });
  it("never uses ownership words or a percentage", () => {
    const s = relationshipDates({ relationship_start: "2013-11-01", relationship_end: "2021-03-03" }) ?? "";
    expect(s).not.toMatch(/\bown|hold|share|%/i);
  });
});

describe("directParentLine (Phase 261)", () => {
  const names = new Map<string, string | null>([
    ["213800C2Y6KDQCD2WZ09", "SHELL DIRECT HOLDINGS LIMITED"],
    ["549300XNL1VRVIODFM92", "SHELL DEEP B.V."],
  ]);

  it("names an in-network parent by its row's name", () => {
    expect(
      directParentLine(
        { relation: "ultimate", direct_parent_lei: "213800C2Y6KDQCD2WZ09", direct_parent_in_network: true },
        names,
        "SHELL PLC",
      ),
    ).toEqual({ kind: "in_network", lei: "213800C2Y6KDQCD2WZ09", name: "SHELL DIRECT HOLDINGS LIMITED" });
  });

  it("gives the LEI and says the path is not shown for a parent outside the network", () => {
    const line = directParentLine(
      { relation: "ultimate", direct_parent_lei: "213800AAAAAAAAAAAA99", direct_parent_in_network: false },
      names,
      "SHELL PLC",
    );
    expect(line?.kind).toBe("outside");
    expect(line?.lei).toBe("213800AAAAAAAAAAAA99");
    expect(line && "sentence" in line ? line.sentence : "").toContain("213800AAAAAAAAAAAA99");
    expect(line && "sentence" in line ? line.sentence : "").toContain("SHELL PLC's subsidiaries");
    expect(line && "sentence" in line ? line.sentence : "").toContain("not shown");
    expect(line && "sentence" in line ? line.sentence : "").not.toMatch(/\bowns?\b|holds?|%/i);
  });

  it("treats an in-network flag without a row as outside, rather than linking nowhere", () => {
    const line = directParentLine(
      { relation: "ultimate", direct_parent_lei: "5493000000000000ZZ99", direct_parent_in_network: true },
      names,
    );
    expect(line?.kind).toBe("outside");
  });

  it("says nothing for a direct child, or where GLEIF names no parent", () => {
    expect(directParentLine({ relation: "direct", direct_parent_lei: "213800C2Y6KDQCD2WZ09" }, names)).toBeNull();
    expect(directParentLine({ relation: "both", direct_parent_lei: "213800C2Y6KDQCD2WZ09" }, names)).toBeNull();
    expect(directParentLine({ relation: "ultimate", direct_parent_lei: null }, names)).toBeNull();
  });
});

describe("enrichmentNote (Phase 261)", () => {
  const kid = { lei: "X", name: "X", jurisdiction: null, status: null, relation: "direct" as const, link: null };
  it("says the dates and paths could not be read when GLEIF refused them", () => {
    const note = enrichmentNote({ enriched: false, children: [kid] });
    expect(note).toContain("could not be read");
    expect(note).toContain("not a finding");
  });
  it("names only the paths when the relationship records answered (Shell, 29 Sept 2026)", () => {
    const note = enrichmentNote({ enriched: false, relationships_read: true, parents_read: false, children: [kid] }) ?? "";
    expect(note).toContain("some paths through intermediate companies could not be read");
    expect(note).not.toMatch(/date/i);
  });
  it("names only the dates when the direct parents answered", () => {
    const note = enrichmentNote({ enriched: false, relationships_read: false, parents_read: true, children: [kid] }) ?? "";
    expect(note).toContain("dates");
    expect(note).not.toMatch(/via|path/i);
  });
  it("names both when both were refused, or the backend predates the split", () => {
    const both = enrichmentNote({ enriched: false, relationships_read: false, parents_read: false, children: [kid] });
    expect(both).toContain("the dates and the paths");
    expect(enrichmentNote({ enriched: false, children: [kid] })).toBe(both);
  });
  it("is silent when enriched, when the backend predates the flag, and for an empty network", () => {
    expect(enrichmentNote({ enriched: true, children: [kid] })).toBeNull();
    expect(enrichmentNote({ children: [kid] })).toBeNull();
    expect(enrichmentNote({ enriched: false, children: [] })).toBeNull();
  });
});

describe("subsidiaryRowId (Phase 261)", () => {
  it("is a stable anchor per LEI", () => {
    expect(subsidiaryRowId("213800C2Y6KDQCD2WZ09")).toBe("subsidiary-row-213800C2Y6KDQCD2WZ09");
  });
});
