import { describe, expect, it } from "vitest";
import { countLeiConfirmingSources, identityBandContents } from "./identifierBadge";
import type { CrossSourceLink } from "./api";

const LEI = "529900IH9V4I3VHQVO92";

function link(
  key: string,
  key_value: string,
  sourceIds: string[],
): CrossSourceLink {
  return {
    key,
    key_value,
    confidence: "strong",
    hits: sourceIds.map((source_id, i) => ({
      source_id,
      hit_id: `${source_id}-${i}`,
      name: "Deutsche Bank AG",
    })),
  };
}

describe("countLeiConfirmingSources", () => {
  it("counts distinct sources on the subject's lei link only", () => {
    const links = [
      link("lei", LEI, ["gleif", "opencorporates", "wikidata"]),
      // Non-LEI bridges must NOT inflate the badge — this was the bug:
      // the badge sits next to the LEI but counted every linked source.
      link("wikidata_qid", "Q66048", ["wikidata", "opensanctions"]),
      link("gb_coh", "00102498", ["companies_house", "opencorporates"]),
    ];
    expect(countLeiConfirmingSources(links, LEI)).toBe(3);
  });

  it("ignores lei links for a different entity's LEI", () => {
    const links = [link("lei", "213800LBDB8WB3QGVN21", ["gleif", "opencorporates"])];
    expect(countLeiConfirmingSources(links, LEI)).toBe(0);
  });

  it("deduplicates a source appearing in multiple hits of the link", () => {
    const links = [link("lei", LEI, ["gleif", "gleif", "opencorporates"])];
    expect(countLeiConfirmingSources(links, LEI)).toBe(2);
  });

  it("matches the LEI case-insensitively and ignores whitespace", () => {
    const links = [link("lei", ` ${LEI.toLowerCase()} `, ["gleif", "opencorporates"])];
    expect(countLeiConfirmingSources(links, LEI.toLowerCase())).toBe(2);
  });

  it("returns 0 with no links or a blank LEI", () => {
    expect(countLeiConfirmingSources([], LEI)).toBe(0);
    expect(countLeiConfirmingSources([link("lei", LEI, ["gleif"])], "")).toBe(0);
  });
});

describe("the badge count and the section count", () => {
  it("are different numbers about different things, on purpose", () => {
    // On BP the page carried "Identifier confirmed by 6 sources" beside the
    // LEI and "3 identifiers matched across 8 sources" a screen below, and
    // nothing said they were answers to different questions. Both are right:
    // this counts sources publishing the LEI, the section counts every
    // source sharing any identifier. The badge names the LEI now.
    const links = [
      link("lei", LEI, ["gleif", "opencorporates"]),
      link("wikidata_qid", "Q66048", ["wikidata", "opensanctions"]),
      link("gb_coh", "00102498", ["companies_house", "openaleph"]),
    ];
    const allSources = new Set(links.flatMap((l) => l.hits.map((h) => h.source_id)));

    expect(countLeiConfirmingSources(links, LEI)).toBe(2);
    expect(allSources.size).toBe(6);
    // The badge's number is a subset of the section's, never the other way
    // round: every source publishing the LEI participates in a link.
    expect(countLeiConfirmingSources(links, LEI)).toBeLessThanOrEqual(
      allSources.size,
    );
  });
});

describe("identityBandContents (Phase 245)", () => {
  it("says what the band holds and never counts sources a second time", () => {
    const text = identityBandContents({ profile: true, identifiers: 2, candidatePairs: 1 });
    expect(text).toBe("Company profile · 2 shared identifiers · 1 candidate pair to review");
    // The subject card's badge is the one corroboration count on the page.
    expect(text).not.toMatch(/source/);
  });

  it("names only what is present", () => {
    expect(identityBandContents({ profile: false, identifiers: 1, candidatePairs: 0 })).toBe(
      "1 shared identifier",
    );
    expect(identityBandContents({ profile: false, identifiers: 0, candidatePairs: 2 })).toBe(
      "2 candidate pairs to review",
    );
  });
});
