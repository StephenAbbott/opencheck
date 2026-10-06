/**
 * Phase 296 — the register-record line on the subject card.
 *
 * It renders from the anchor event alone, so it is there whatever the
 * register's own card did; no payload renders no line.
 */
import { describe, expect, it } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";

import type { RegisterRecord } from "../../lib/registerLinks";
import { SubjectCard } from "./SubjectCard";

const RECORD: RegisterRecord = {
  source_id: "zefix",
  register: "Swiss UID register (FSO)",
  identifier: "CHE469102316",
  url: "https://www.uid.admin.ch/Detail.aspx?uid_id=CHE469102316",
  ra_code: "RA000549",
};

function card(registerRecord: RegisterRecord | null) {
  return render(
    <SubjectCard
      lei="254900JP7NJEXZ9JFG77"
      legalName="Venture Spirit Sàrl"
      jurisdiction="CH"
      onPdf={() => {}}
      onMarkdown={() => {}}
      registerRecord={registerRecord}
    />,
  );
}

describe("SubjectCard register record", () => {
  it("links the register's own page with a name that says where it goes", () => {
    card(RECORD);
    const link = screen.getByRole("link", {
      name: "Swiss UID register (FSO) record CHE469102316 (opens in a new tab)",
    });
    expect(link.getAttribute("href")).toBe(RECORD.url);
    expect(link.getAttribute("target")).toBe("_blank");
    expect(link.getAttribute("rel")).toContain("noopener");
    expect(link.className).toContain("underline");
    expect(screen.getByText("Register record:")).toBeTruthy();
  });

  it("explains where the link comes from", () => {
    card(RECORD);
    fireEvent.click(screen.getByRole("button", { name: "About the register record" }));
    expect(screen.getByText(/does not depend on OpenCheck having reached that register/)).toBeTruthy();
  });

  it("renders nothing without a record", () => {
    card(null);
    expect(screen.queryByText("Register record:")).toBeNull();
  });
});
