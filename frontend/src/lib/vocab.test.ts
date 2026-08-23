import { describe, expect, it } from "vitest";
import {
  ACRONYMS,
  BANNED_SYNONYMS,
  bodsRecordCount,
  expandOnFirstUse,
  graphPartiesLabel,
  OPENALEPH_TOPIC,
  resultCount,
  sourceLabel,
  sourceList,
  topicLabel,
  topicList,
} from "./vocab";

describe("resultCount", () => {
  it("agrees in number", () => {
    // App.tsx:1511 hardcoded "results" with no singular branch — a plain bug
    // at n=1 that a shared helper cannot reproduce.
    expect(resultCount(0)).toBe("0 results");
    expect(resultCount(1)).toBe("1 result");
    expect(resultCount(2)).toBe("2 results");
  });

  it("never says hit", () => {
    // The same array was "3 results" on one line of SourceBucketCard and
    // "No hits." fifty-four lines later.
    for (const n of [0, 1, 5]) expect(resultCount(n)).not.toMatch(/hit/i);
  });
});

describe("bodsRecordCount", () => {
  it("always qualifies the word", () => {
    // "12 statements" meant BODS records on the source card and narrative
    // claims in the panel directly above it.
    expect(bodsRecordCount(12)).toBe("12 BODS records");
    expect(bodsRecordCount(1)).toBe("1 BODS record");
    for (const n of [1, 12]) expect(bodsRecordCount(n)).toContain("BODS");
  });
});

describe("expandOnFirstUse", () => {
  it("expands once per surface, then leaves the term alone", () => {
    const seen = new Set<string>();
    expect(expandOnFirstUse("BODS", seen)).toBe("BODS (Beneficial Ownership Data Standard)");
    expect(expandOnFirstUse("BODS", seen)).toBe("BODS");
  });

  it("tracks each term separately", () => {
    const seen = new Set<string>();
    expect(expandOnFirstUse("LEI", seen)).toContain("Legal Entity Identifier");
    expect(expandOnFirstUse("PEP", seen)).toContain("politically exposed person");
    expect(expandOnFirstUse("LEI", seen)).toBe("LEI");
  });

  it("starts fresh for a new surface", () => {
    // A reader who lands on a report link has not read the homepage, so
    // "first use" is per page, not per session.
    expect(expandOnFirstUse("BODS", new Set())).toContain("(");
  });

  it("passes an unknown term through untouched", () => {
    expect(expandOnFirstUse("QUICKCHECK", new Set())).toBe("QUICKCHECK");
  });

  it("expands every acronym the report actually shows", () => {
    for (const [term, expansion] of Object.entries(ACRONYMS)) {
      expect(expansion.length, term).toBeGreaterThan(term.length);
      expect(expansion, term).not.toBe(expansion.toUpperCase());
    }
  });
});

describe("sourceLabel", () => {
  it("uses the display name the response carries", () => {
    expect(sourceLabel("companies_house", { companies_house: "UK Companies House" })).toBe(
      "UK Companies House",
    );
  });

  it("never shows a reader a snake_case slug", () => {
    // "via companies_house" rendered in mono on every person match row, and
    // `.join(" and ")` over raw ids built English sentences out of them.
    expect(sourceLabel("companies_house")).toBe("Companies House");
    expect(sourceLabel("cvr_denmark")).toBe("CVR Denmark");
    expect(sourceLabel("ted_eu")).toBe("TED EU");
    expect(sourceLabel("opensanctions")).toBe("Opensanctions");
    for (const id of ["companies_house", "bods_gleif", "jar_lithuania"]) {
      expect(sourceLabel(id)).not.toContain("_");
    }
  });

  it("falls back rather than rendering nothing for an unmapped source", () => {
    expect(sourceLabel("brand_new_source", { other: "Other" })).toBe("Brand New Source");
  });
});

describe("sourceList", () => {
  it("writes an English list", () => {
    const names = { companies_house: "UK Companies House", opensanctions: "OpenSanctions" };
    expect(sourceList(["companies_house"], names)).toBe("UK Companies House");
    expect(sourceList(["companies_house", "opensanctions"], names)).toBe(
      "UK Companies House and OpenSanctions",
    );
  });

  it("uses a serial comma list beyond two", () => {
    expect(sourceList(["a_source", "b_source", "c_source"])).toBe(
      "A Source, B Source and C Source",
    );
  });

  it("deduplicates", () => {
    expect(sourceList(["opensanctions", "opensanctions"])).toBe("Opensanctions");
  });

  it("returns empty for no sources rather than a dangling connective", () => {
    expect(sourceList([])).toBe("");
  });
});

describe("topicLabel", () => {
  it("translates the upstream slugs a reader was shown raw", () => {
    expect(topicLabel("corp.disqual")).toBe("disqualified director");
    expect(topicLabel("poi")).toBe("person of interest");
    expect(topicLabel("crime.fin")).toBe("financial crime");
  });

  it("prettifies an unmapped topic rather than dropping it", () => {
    // The fact that a collection was tagged is information even when the tag
    // is new to us, so an unknown id is cleaned up, not hidden.
    expect(topicLabel("some.new_topic")).toBe("some new topic");
  });

  it("maps only what upstream already means — no inference", () => {
    // Every entry must be a translation of the slug, not a judgement added on
    // top. A label that introduced a word like "confirmed" or "guilty" would
    // be asserting something FollowTheMoney's topic does not.
    for (const label of Object.values(OPENALEPH_TOPIC)) {
      expect(label).not.toMatch(/\b(confirmed|guilty|proven|verified)\b/i);
    }
  });
});

describe("topicList", () => {
  it("joins readable labels, deduplicated", () => {
    expect(topicList(["poi", "corp.disqual"])).toBe("person of interest, disqualified director");
    expect(topicList(["poi", "poi"])).toBe("person of interest");
  });
});

describe("BANNED_SYNONYMS", () => {
  it("names a replacement for every banned term", () => {
    for (const [banned, replacement] of Object.entries(BANNED_SYNONYMS)) {
      expect(replacement, banned).toBeTruthy();
      expect(replacement.toLowerCase(), banned).not.toBe(banned.toLowerCase());
    }
  });

  it("does not ban a word it also recommends", () => {
    // A replacement that is itself banned would make the rule uncloseable.
    const banned = new Set(Object.keys(BANNED_SYNONYMS).map((k) => k.toLowerCase()));
    for (const replacement of Object.values(BANNED_SYNONYMS)) {
      expect(banned.has(replacement.toLowerCase()), replacement).toBe(false);
    }
  });
});

describe("the banned labels the lint enforces", () => {
  it("bans phrases, not words", () => {
    // The first version of this list banned single words and the lint that
    // enforced it reported 90 violations, every one false: "an empty result
    // here is not a clean screen", "a fast screen of the subject", the SSE
    // event named "hit", `Liveness = "stub"`, and `min-h-screen` in a Tailwind
    // class. A word ban would have forced rewrites of correct English.
    for (const banned of Object.keys(BANNED_SYNONYMS)) {
      expect(banned.length, banned).toBeGreaterThan(4);
    }
  });

  it("bans each label that actually regressed", () => {
    // One button said "Screen person" while another said "Run background
    // check" for the same action; an empty state said "No hits." forty lines
    // from one that said "3 results".
    for (const label of ["No hits.", "Screen person", "Run background check", "Look up"]) {
      expect(Object.keys(BANNED_SYNONYMS), label).toContain(label);
    }
  });

  it("points every banned label at something renderable", () => {
    for (const [banned, replacement] of Object.entries(BANNED_SYNONYMS)) {
      expect(replacement, banned).toBeTruthy();
      expect(replacement, banned).not.toBe(banned);
    }
  });
});

describe("graphPartiesLabel", () => {
  it("counts entities, not 'parties'", () => {
    // The chip is computed from the entity split alone. Calling that total
    // "parties" put "7 parties" over a diagram holding eleven nodes.
    expect(graphPartiesLabel(7)).toBe("7 entities");
    expect(graphPartiesLabel(7, 0)).toBe("7 entities");
  });

  it("names the people when the source disclosed any", () => {
    expect(graphPartiesLabel(7, 4)).toBe("7 entities · 4 people");
  });

  it("agrees in number on both halves", () => {
    expect(graphPartiesLabel(1)).toBe("1 entity");
    expect(graphPartiesLabel(1, 1)).toBe("1 entity · 1 person");
    expect(graphPartiesLabel(2, 1)).toBe("2 entities · 1 person");
  });

  it("never says '0 people', which reads as a finding about disclosure", () => {
    expect(graphPartiesLabel(3, 0)).not.toMatch(/people|person/);
    expect(graphPartiesLabel(3, -1)).toBe("3 entities");
  });

  it("never says 'parties'", () => {
    for (const [e, p] of [[1, 0], [1, 1], [5, 2], [12, 9]] as const) {
      expect(graphPartiesLabel(e, p)).not.toMatch(/part(y|ies)/);
    }
  });
});
