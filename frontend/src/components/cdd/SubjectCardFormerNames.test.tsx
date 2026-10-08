/**
 * Phase 311 — former names live in "Is this the right company?", not on the
 * subject card.
 *
 * Phase 309 put a "Formerly …" line under the LEI as well as the identity
 * band's "Former names" row. On a phone the line ran to six lines for Barrick
 * Mining Corporation and pushed the mode tabs below the fold, so it was taken
 * off (Stephen, 8 Oct 2026). The row stays; the card never gets the names.
 * A source check, because the regression is a prop being wired back in —
 * something no render of the card can see.
 */
import { describe, expect, it } from "vitest";

import appSource from "../../App.tsx?raw";
import cardSource from "./SubjectCard.tsx?raw";

import type { SubjectProfile } from "../../lib/api";
import { profileRows } from "../../lib/subjectProfile";

describe("former names on the subject card", () => {
  it("are not passed to or rendered by the subject card", () => {
    expect(cardSource).not.toMatch(/\bformerly\b|former_names|formerNames/);
    const start = appSource.indexOf("<SubjectCard");
    expect(start).toBeGreaterThan(-1);
    const tag = appSource.slice(start, appSource.indexOf("/>", start));
    expect(tag).not.toMatch(/formerly|former/i);
  });

  it("stay in the identity band's rows", () => {
    const profile = {
      former_names: [{ name: "Barrick Gold Corporation", until: null, from: null, sources: ["gleif"] }],
      name_changed_on: "2025-05-06",
    } as unknown as SubjectProfile;
    const row = profileRows(profile).find((r) => r.label === "Former names");
    expect(row?.value).toBe("Barrick Gold Corporation · legal name changed 6 May 2025");
  });
});
