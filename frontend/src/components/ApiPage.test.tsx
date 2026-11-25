/**
 * The /api page's MCP section (the API-page MCP guide).
 *
 * `lib/mcpTools.ts` is pinned to the server by
 * `backend/tests/test_api_page_mcp.py`. What that cannot see is whether the
 * page shows it: every tool once, in the handshake's six steps, the
 * budget-spending ones marked, and the reading rules present.
 */
import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { ApiPage } from "./ApiPage";
import {
  MCP_BUDGET_TOOLS,
  MCP_PROTOCOL_VERSIONS,
  MCP_SKILLS,
  MCP_STEPS,
  MCP_TOOLS,
} from "../lib/mcpTools";
import { ERROR_CODES, RATE_TIERS, REFERENCE_LINKS, RESPONSE_KEYS, SAMPLE_LOOKUP } from "../lib/apiReference";

function mcpSection() {
  render(<ApiPage />);
  return screen.getByRole("region", { name: "MCP server for AI agents" });
}

describe("ApiPage MCP section", () => {
  it("lists every tool exactly once", () => {
    const section = mcpSection();
    const names = MCP_TOOLS.map((t) => t.name);
    expect(new Set(names).size).toBe(names.length);
    for (const name of names) {
      expect(within(section).getAllByText((_, el) => el?.tagName === "CODE" && el.textContent === name)).toHaveLength(1);
    }
  });

  it("shows the handshake's six steps in order", () => {
    const section = mcpSection();
    const list = within(section).getByRole("list", { name: "Call the tools in this order" });
    const headings = within(list).getAllByRole("heading", { level: 5 }).map((h) => h.textContent);
    expect(headings).toEqual(MCP_STEPS.map((s, i) => `Step ${i + 1}: ${s.title}`));
  });

  it("marks exactly the tools that spend the lookup budget", () => {
    const section = mcpSection();
    const steps = within(section).getByRole("list", { name: "Call the tools in this order" });
    expect(within(steps).getAllByText("Lookup budget")).toHaveLength(MCP_BUDGET_TOOLS.length);
    expect(MCP_BUDGET_TOOLS).toContain("opencheck_lookup");
    expect(MCP_BUDGET_TOOLS).not.toContain("opencheck_knowability");
  });

  it("says how to read the answer and what a 429 means", () => {
    const section = mcpSection();
    expect(within(section).getByText("degraded_sources")).toBeTruthy();
    expect(within(section).getByText(/never clean/)).toBeTruthy();
    expect(within(section).getAllByText("retry_after_s").length).toBeGreaterThan(0);
  });

  it("names the protocol revisions, the skills and tasks", () => {
    const section = mcpSection();
    const newer = within(section).getByRole("region", { name: "What newer clients get" });
    for (const v of MCP_PROTOCOL_VERSIONS) expect(within(newer).getByText(v)).toBeTruthy();
    for (const skill of MCP_SKILLS) expect(within(newer).getByText(skill.name)).toBeTruthy();
    expect(within(newer).getByRole("heading", { name: "Tasks" })).toBeTruthy();
  });

  it("is linked from the top of the page", () => {
    render(<ApiPage />);
    const nav = screen.getByRole("navigation", { name: "On this page" });
    expect(within(nav).getByRole("link", { name: "MCP server for AI agents" }).getAttribute("href")).toBe("#mcp");
  });
});

describe("ApiPage reference, sample data, limits and errors", () => {
  it("links the generated reference and llms.txt", () => {
    render(<ApiPage />);
    const ref = screen.getByRole("region", { name: "Reference" });
    for (const link of REFERENCE_LINKS) {
      const a = within(ref).getByRole("link", { name: link.label });
      expect(a.getAttribute("href")?.endsWith(link.path)).toBe(true);
    }
  });

  it("offers the sample answer and describes its keys", () => {
    render(<ApiPage />);
    const sample = screen.getByRole("region", { name: "Sample data" });
    expect(within(sample).getByRole("link", { name: /Download the sample answer/ }).getAttribute("href")).toBe(
      SAMPLE_LOOKUP.path,
    );
    for (const [key] of RESPONSE_KEYS) {
      expect(within(sample).getAllByText(key).length).toBeGreaterThan(0);
    }
  });

  it("states every rate tier and every error status", () => {
    render(<ApiPage />);
    const limits = screen.getByRole("region", { name: "Limits and errors" });
    for (const t of RATE_TIERS) {
      expect(within(limits).getByRole("rowheader", { name: t.tier })).toBeTruthy();
    }
    for (const e of ERROR_CODES) {
      expect(within(limits).getByRole("rowheader", { name: String(e.status) })).toBeTruthy();
    }
    expect(within(limits).getByText("degraded_sources")).toBeTruthy();
  });
});

describe("ApiPage quickstart", () => {
  it("switches the snippets between languages", () => {
    render(<ApiPage />);
    const section = screen.getByRole("region", { name: "Quickstart" });
    const curl = within(section).getByRole("button", { name: "curl" });
    expect(curl).toHaveAttribute("aria-pressed", "true");
    expect(within(section).getByLabelText("curl quickstart").textContent).toContain("curl -s");
    fireEvent.click(within(section).getByRole("button", { name: "Python" }));
    expect(within(section).getByRole("button", { name: "Python" })).toHaveAttribute("aria-pressed", "true");
    expect(curl).toHaveAttribute("aria-pressed", "false");
    expect(within(section).getByLabelText("Python quickstart").textContent).toContain("import requests");
    expect(within(section).getByLabelText("Python stream example").textContent).toContain("/stream");
  });
});

