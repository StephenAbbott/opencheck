/**
 * The advanced-search-filters ticket — "More filters" is hidden by default,
 * set filters stay visible as chips, and the legal form waits for a
 * jurisdiction.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";

import { MoreFilters } from "./MoreFilters";
import type { EntityFilters, LegalFormList } from "../lib/searchFilters";

afterEach(cleanup);

const GERMANY: LegalFormList = {
  jurisdiction: "DE",
  jurisdiction_name: "Germany",
  has_subdivision_forms: false,
  counts_available: true,
  elf_version: "1.6",
  forms: [
    { code: "2HBR", name: "Gesellschaft mit beschränkter Haftung", abbreviation: "GmbH", abbreviations: ["GmbH"], names: ["Gesellschaft mit beschränkter Haftung"], subdivision: null, issued_leis: 109_065 },
    { code: "6QQB", name: "Aktiengesellschaft", abbreviation: "AG", abbreviations: ["AG"], names: ["Aktiengesellschaft"], subdivision: null, issued_leis: 3_742 },
  ],
};

function renderFilters(filters: EntityFilters, open = false, legalForms: LegalFormList | null = null) {
  const onChange = vi.fn();
  const onOpenChange = vi.fn();
  render(
    <MoreFilters
      filters={filters}
      onChange={onChange}
      open={open}
      onOpenChange={onOpenChange}
      legalForms={legalForms}
      legalFormsLoading={false}
      legalFormsFailed={false}
    />,
  );
  return { onChange, onOpenChange };
}

describe("MoreFilters", () => {
  it("is one closed control by default, with no fields on screen", () => {
    const { onOpenChange } = renderFilters({});
    const toggle = screen.getByRole("button", { name: "More filters" });
    expect(toggle.getAttribute("aria-expanded")).toBe("false");
    expect(screen.queryByRole("group", { name: "More filters" })).toBeNull();
    expect(screen.queryByRole("combobox")).toBeNull();
    fireEvent.click(toggle);
    expect(onOpenChange).toHaveBeenCalledWith(true);
  });

  it("keeps set filters visible as removable chips when closed", () => {
    const { onChange } = renderFilters({ jurisdiction: "DE", legalForm: ["2HBR"] }, false, GERMANY);
    expect(screen.getByRole("button", { name: "More filters · 2" })).toBeTruthy();
    expect(screen.getByText("GmbH · Gesellschaft mit beschränkter Haftung")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Remove jurisdiction filter: Germany" }));
    // The legal form goes with its jurisdiction.
    expect(onChange).toHaveBeenCalledWith({});
  });

  it("asks for a jurisdiction before offering legal forms", () => {
    renderFilters({}, true);
    const field = screen.getByRole("combobox", { name: "Legal form" }) as HTMLInputElement;
    expect(field.disabled).toBe(true);
    expect(field.placeholder).toBe("Choose a jurisdiction first");
  });

  it("lists the jurisdiction's forms, most used first, and picks one by keyboard", () => {
    const { onChange } = renderFilters({ jurisdiction: "DE" }, true, GERMANY);
    const field = screen.getByRole("combobox", { name: "Legal form" });
    fireEvent.focus(field);
    const options = within(screen.getByRole("listbox")).getAllByRole("option").map((o) => o.textContent);
    expect(options[0]).toContain("Any legal form");
    expect(options[1]).toContain("GmbH");
    expect(options[1]).toContain("2HBR");
    expect(options.at(-1)).toContain("No ELF code");
    fireEvent.change(field, { target: { value: "aktien" } });
    fireEvent.keyDown(field, { key: "Enter" });
    expect(onChange).toHaveBeenCalledWith({ jurisdiction: "DE", legalForm: ["6QQB"] });
  });

  it("changing the jurisdiction clears the legal form", () => {
    const { onChange } = renderFilters({ jurisdiction: "DE", legalForm: ["2HBR"] }, true, GERMANY);
    fireEvent.change(screen.getByLabelText("Jurisdiction"), { target: { value: "AT" } });
    expect(onChange).toHaveBeenCalledWith({ jurisdiction: "AT" });
  });

  it("says the LEI status is the record's, not the company's", () => {
    renderFilters({}, true);
    expect(screen.getByText("The LEI record’s status — not the company’s.")).toBeTruthy();
  });
});
