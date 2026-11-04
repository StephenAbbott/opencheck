/**
 * The Export menu's "Discrepancy note" item (the discrepancy-report ticket):
 * enabled with its description when the run has a register disagreement,
 * aria-disabled with the reason when it has none, and still reachable by
 * keyboard either way — a greyed-out control that does not say why is a
 * dead end.
 */
import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";

import { ExportMenu } from "./ExportMenu";
import {
  DISCREPANCY_NOTE_DESCRIPTION,
  DISCREPANCY_NOTE_LABEL,
  DISCREPANCY_NOTE_UNAVAILABLE,
} from "../../lib/discrepancyNote";

function open(note: { label: string; description: string; disabled?: boolean; onSelect: () => void }) {
  render(
    <ExportMenu pdfBusy={false} mdBusy={false} onPdf={() => {}} onMarkdown={() => {}} note={note} />,
  );
  fireEvent.click(screen.getByRole("button", { name: /export/i }));
  return screen.getByRole("menuitem", { name: new RegExp(DISCREPANCY_NOTE_LABEL) });
}

describe("ExportMenu discrepancy note", () => {
  it("downloads when the run has a disagreement", () => {
    const onSelect = vi.fn();
    const item = open({ label: DISCREPANCY_NOTE_LABEL, description: DISCREPANCY_NOTE_DESCRIPTION, onSelect });
    expect(item).not.toHaveAttribute("aria-disabled");
    expect(item).toHaveTextContent(DISCREPANCY_NOTE_DESCRIPTION);
    fireEvent.click(item);
    expect(onSelect).toHaveBeenCalledTimes(1);
  });

  it("is aria-disabled with its reason when nothing qualifies, and does nothing when pressed", () => {
    const onSelect = vi.fn();
    const item = open({
      label: DISCREPANCY_NOTE_LABEL,
      description: DISCREPANCY_NOTE_UNAVAILABLE,
      disabled: true,
      onSelect,
    });
    expect(item).toHaveAttribute("aria-disabled", "true");
    expect(item).toHaveTextContent(DISCREPANCY_NOTE_UNAVAILABLE);
    fireEvent.click(item);
    expect(onSelect).not.toHaveBeenCalled();
  });

  it("sits in the Report group after the Markdown item", () => {
    open({ label: DISCREPANCY_NOTE_LABEL, description: DISCREPANCY_NOTE_DESCRIPTION, onSelect: () => {} });
    const names = screen.getAllByRole("menuitem").map((el) => el.textContent ?? "");
    const md = names.findIndex((n) => n.includes("Report as Markdown"));
    const note = names.findIndex((n) => n.includes(DISCREPANCY_NOTE_LABEL));
    expect(md).toBeGreaterThanOrEqual(0);
    expect(note).toBe(md + 1);
  });
});
