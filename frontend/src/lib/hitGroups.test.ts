import { describe, expect, it } from "vitest";
import { groupHitsForDisplay, siblingNote } from "./hitGroups";
import type { SourceHit } from "./api";

const hit = (over: Partial<SourceHit> = {}): SourceHit =>
  ({
    source_id: "openaleph",
    hit_id: "a",
    kind: "entity",
    name: "Bp P.L.C.",
    summary: "collection: Companies House (UK) PSC · Company",
    finding: "Indexed in Companies House (UK) Persons with Significant Control.",
    identifiers: {},
    raw: {},
    is_stub: false,
    ...over,
  }) as SourceHit;

describe("groupHitsForDisplay", () => {
  it("collapses rows a reader cannot tell apart", () => {
    // Live on BP: four PSC records, identical to the pixel, because what
    // distinguishes them (which company BP controls) is not on the record.
    const groups = groupHitsForDisplay([
      hit({ hit_id: "gb-coh-psc-00593645-x" }),
      hit({ hit_id: "gb-coh-psc-01150608-y" }),
      hit({ hit_id: "gb-coh-psc-00542515-z" }),
      hit({ hit_id: "gb-coh-psc-01030652-w" }),
    ]);
    expect(groups).toHaveLength(1);
    expect(groups[0].count).toBe(4);
    expect(groups[0].lead.hit_id).toBe("gb-coh-psc-00593645-x");
    expect(groups[0].rest).toHaveLength(3);
  });

  it("keeps rows that differ in anything the reader sees", () => {
    const groups = groupHitsForDisplay([
      hit({ hit_id: "a", finding: "Indexed in GLEIF Concatenated Data File." }),
      hit({ hit_id: "b" }),
      hit({ hit_id: "c", name: "BP P.L.C." }), // different capitalisation IS visible
      hit({ hit_id: "d", summary: "collection: EU ESMA · Company" }),
    ]);
    expect(groups).toHaveLength(4);
  });

  it("groups BP's PSC records, which now render identically", () => {
    // Two of BP's four scored 107 and one scored 106 — a BM25 number on no
    // published scale, which kept the fourth row out of its own group until
    // Phase 133 subtracted it from the key. Phase 135 stopped printing it at
    // the source instead, so these rows arrive identical and the key needs no
    // special case. What the reader sees is what groups.
    const groups = groupHitsForDisplay([
      hit({ hit_id: "a", summary: "collection: CH PSC \u00b7 Company \u00b7 identifier corroborated" }),
      hit({ hit_id: "b", summary: "collection: CH PSC \u00b7 Company \u00b7 identifier corroborated" }),
    ]);
    expect(groups).toHaveLength(1);
    expect(groups[0].count).toBe(2);
  });

  it("subtracts nothing, so a score in a summary would split a group again", () => {
    // The inverse of what Phase 133 pinned, and deliberately so. That phase
    // taught the key to ignore one visible difference; Phase 135 removed the
    // difference instead, and the key went back to being "everything the row
    // says". If a retrieval score ever reaches a summary again these rows
    // stop grouping — which is the honest outcome for a key defined by what
    // the reader sees, and is caught upstream by the adapter test that pins
    // the score out of the sentence.
    const groups = groupHitsForDisplay([
      hit({ hit_id: "a", summary: "collection: CH PSC \u00b7 FtM match score 107" }),
      hit({ hit_id: "b", summary: "collection: CH PSC \u00b7 FtM match score 106" }),
    ]);
    expect(groups).toHaveLength(2);
  });

  it("does not group on the identifier, which the reader never sees", () => {
    // Two records with the same id would be a fault upstream; two with
    // different ids and the same rendering are the case that matters.
    const groups = groupHitsForDisplay([hit({ hit_id: "a" }), hit({ hit_id: "b" })]);
    expect(groups).toHaveLength(1);
  });

  it("leads with the first, which is the best-scoring record", () => {
    const groups = groupHitsForDisplay([
      hit({ hit_id: "best" }),
      hit({ hit_id: "worse" }),
    ]);
    expect(groups[0].lead.hit_id).toBe("best");
  });

  it("preserves first-seen order across groups", () => {
    const groups = groupHitsForDisplay([
      hit({ hit_id: "a", finding: "First." }),
      hit({ hit_id: "b", finding: "Second." }),
      hit({ hit_id: "c", finding: "First." }),
    ]);
    expect(groups.map((g) => g.lead.finding)).toEqual(["First.", "Second."]);
    expect(groups.map((g) => g.count)).toEqual([2, 1]);
  });

  it("says nothing about nothing", () => {
    expect(groupHitsForDisplay([])).toEqual([]);
  });
});

describe("siblingNote", () => {
  it("only speaks when there is more than one", () => {
    expect(siblingNote(1)).toBeNull();
    expect(siblingNote(0)).toBeNull();
  });

  it("says how many there are and that one of them is open", () => {
    // The drawer shows one record. Without this it silently stands in for
    // all four.
    const note = siblingNote(4);
    expect(note).toContain("4 records");
    expect(note).toContain("closest match");
  });
});
