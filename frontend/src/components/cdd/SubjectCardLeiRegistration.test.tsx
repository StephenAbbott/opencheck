/**
 * Phase 242 — the LEI registration chip on the subject card.
 *
 * `lib/subjectProfile.test.ts` pins the words. What only rendering shows: a
 * lapsed LEI puts exactly one chip in the accessibility tree (the phone and
 * desktop placements are display-toggled, as the register chip's are), the
 * full sentence — including that this is not the company's status — reaches
 * assistive technology, and an issued LEI renders no chip at all.
 */
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";

import { leiRegistrationChip, statusChip } from "../../lib/subjectProfile";
import { SubjectCard } from "./SubjectCard";

const AFPC = "549300W96W2VKSMVDF81";
const SENTENCE =
  "GLEIF records this LEI as lapsed: its renewal was due on 19 Oct 2017 and has not been made, so no issuer has re-checked its reference data since then. GLEIF last updated the record on 8 Apr 2026. This is the status of the LEI record, not of the company.";

function card(status: string) {
  const reg = { status, since: status === "LAPSED" ? "2017-10-19" : null, sentence: SENTENCE };
  return render(
    <SubjectCard
      lei={AFPC}
      legalName="American Foreign Policy Council"
      jurisdiction="US-DC"
      onPdf={() => {}}
      onMarkdown={() => {}}
      status={statusChip({
        legal_form: null,
        register_status: {
          liveness: "live",
          since: null,
          raw: "ACTIVE",
          source_id: "gleif",
          sources: ["gleif"],
          independent_sources: 1,
          other_values: [],
        },
        founding_date: null,
        registered_address: null,
        jurisdiction: "US-DC",
        statement_ids: [],
      })}
      leiRegistration={leiRegistrationChip(reg)}
    />,
  );
}

describe("SubjectCard LEI registration chip", () => {
  it("shows a lapsed LEI with its date, beside — not instead of — the register status", () => {
    card("LAPSED");
    // Both placements render; CSS shows one per breakpoint.
    expect(screen.getAllByText("LEI lapsed since 19 Oct 2017")).toHaveLength(2);
    expect(screen.getAllByText(SENTENCE)).toHaveLength(2);
    // The company is still active on the register — the LEI chip changes nothing there.
    expect(screen.getAllByText("Active · GLEIF").length).toBeGreaterThan(0);
  });

  it("renders no chip for an issued LEI", () => {
    card("ISSUED");
    expect(screen.queryByText(/^LEI issued/)).toBeNull();
    expect(screen.queryByText(SENTENCE)).toBeNull();
  });
});
