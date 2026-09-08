import { describe, expect, it } from "vitest";
import type { DeclaredSource, SubsidiaryChild } from "./api";
import {
  coverageSentence,
  openableSentence,
  orderRows,
  resolveLists,
  subsidiaryHref,
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
