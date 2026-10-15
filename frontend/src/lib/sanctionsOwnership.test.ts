/**
 * sanctionsOwnership (Phase 342) — the table rows, notes and FullCheck band
 * line, read from two signals the backend emits
 * (`sanctionsOwnershipFixture.json`, pinned by
 * `backend/tests/test_sanctions_ownership_phase342.py`).
 */
import { describe, expect, it } from "vitest";

import fixture from "./sanctionsOwnershipFixture.json";
import type { RiskSignal } from "./api";
import {
  RESULT_WORDS,
  sanctionsOwnershipBandNote,
  sanctionsOwnershipNotes,
  sanctionsOwnershipRows,
  sanctionsOwnershipSignal,
} from "./sanctionsOwnership";

const meets = fixture.meets as unknown as RiskSignal;
const possible = fixture.possible as unknown as RiskSignal;

describe("sanctionsOwnership", () => {
  it("lists every regime side by side, with the caught company and who holds it", () => {
    const rows = sanctionsOwnershipRows(meets);
    expect(rows.map((r) => r.regime)).toEqual(["OFAC (US)", "EU", "UK"]);
    expect(rows[0].resultWords).toBe(RESULT_WORDS.meets);
    expect(rows[0].holding).toBe("50%");
    expect(rows[0].holders).toBe("Holding A 50% (caught company: held by Designated P 60%)");
    expect(rows[1].result).toBe("no_designated_holder");
  });

  it("marks a result that a name-only match would change", () => {
    const [ofac] = sanctionsOwnershipRows(possible);
    expect(possible.kind).toBe("context");
    expect(ofac.result).toBe("not_met");
    expect(ofac.ifConfirmed).toBe(true);
  });

  it("says how it is computed and what it does not compute", () => {
    const notes = sanctionsOwnershipNotes(meets).join(" ");
    expect(notes).toMatch(/control tests are not computed/);
    expect(notes).toMatch(/not a determination/);
  });

  it("gives the FullCheck band one line, and nothing for other signals", () => {
    expect(sanctionsOwnershipBandNote([meets])).toMatch(/^Sanctions ownership test, computed separately — OFAC \(US\): meets; EU: no designated holder/);
    expect(sanctionsOwnershipBandNote([possible])).toMatch(/would meet if a name-only match were confirmed/);
    const other = { ...meets, code: "SANCTIONED" } as RiskSignal;
    expect(sanctionsOwnershipBandNote([other])).toBe("");
    expect(sanctionsOwnershipSignal([other])).toBeNull();
    expect(sanctionsOwnershipRows(other)).toEqual([]);
  });
});
