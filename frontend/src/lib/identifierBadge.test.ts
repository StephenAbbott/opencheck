import { describe, expect, it } from "vitest";
import { countLeiConfirmingSources } from "./identifierBadge";
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
