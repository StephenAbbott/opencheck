/**
 * "More filters" under the Company name box (the advanced-search-filters
 * ticket, approach A, approved 3 Nov 2026).
 *
 * Hidden by default: one quiet control under the search field. Opened, five
 * fields in the page flow — no floating layer, the `Explain` rule. Filters
 * that are set stay visible as removable chips when the panel is closed, so
 * nothing narrows a search out of sight.
 */

import { useId } from "react";
import {
  CATEGORY_WORDS,
  COUNTRY_JURISDICTIONS,
  ENTITY_STATUS_WORDS,
  REGISTRATION_STATUS_WORDS,
  activeFilterCount,
  filterChips,
  jurisdictionName,
  subdivisionsOf,
  withoutFilter,
  type EntityFilters,
  type LegalFormList,
} from "../lib/searchFilters";
import { LegalFormPicker } from "./LegalFormPicker";
import { Button, FilterChip, Icon, sectionLabelClasses } from "./ui";

const FIELD =
  "w-full min-h-[44px] rounded border border-oo-rule bg-white px-3 text-oo-body text-oo-ink focus:border-oo-blue focus:outline-none focus:ring-2 focus:ring-oo-blue/30";

export function MoreFilters({
  filters,
  onChange,
  open,
  onOpenChange,
  legalForms,
  legalFormsLoading,
  legalFormsFailed,
}: {
  filters: EntityFilters;
  onChange: (next: EntityFilters) => void;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  legalForms: LegalFormList | null;
  legalFormsLoading: boolean;
  legalFormsFailed: boolean;
}) {
  const id = useId();
  const panelId = `${id}-panel`;
  const count = activeFilterCount(filters);
  const chips = filterChips(filters, legalForms?.forms ?? []);

  const country = filters.jurisdiction?.split("-")[0] ?? "";
  const subdivisions = country ? subdivisionsOf(country) : [];
  const set = (patch: Partial<EntityFilters>) => {
    const next: EntityFilters = { ...filters, ...patch };
    for (const key of Object.keys(patch) as (keyof EntityFilters)[]) {
      const v = next[key];
      if (v === undefined || v === "" || (Array.isArray(v) && v.length === 0)) delete next[key];
    }
    onChange(next);
  };
  const setJurisdiction = (code: string | undefined) => {
    // A legal form belongs to its jurisdiction; a new jurisdiction clears it.
    const next = { ...filters };
    delete next.legalForm;
    if (code) next.jurisdiction = code;
    else delete next.jurisdiction;
    onChange(next);
  };

  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap items-center gap-2">
        <Button
          variant="ghost"
          size="sm"
          aria-expanded={open}
          aria-controls={panelId}
          onClick={() => onOpenChange(!open)}
          className="!px-1.5"
        >
          {open ? "Fewer filters" : "More filters"}
          {count > 0 ? ` · ${count}` : ""}
          <Icon name="chevronDown" size={14} className={open ? "rotate-180" : ""} />
        </Button>
        {!open &&
          chips.map((chip) => (
            <FilterChip
              key={chip.key}
              label={chip.label}
              removeLabel={chip.removeLabel}
              onRemove={() => onChange(withoutFilter(filters, chip.key))}
            />
          ))}
        {!open && count > 1 && (
          <Button variant="ghost" size="sm" onClick={() => onChange({})} className="!px-1.5 !text-oo-muted">
            Clear all
          </Button>
        )}
      </div>

      {open && (
        <div id={panelId} role="group" aria-label="More filters" className="rounded-oo border border-oo-rule bg-oo-bg p-4">
          <div className="grid grid-cols-1 gap-x-4 gap-y-5 sm:grid-cols-3">
            <div>
              <label htmlFor={`${id}-jurisdiction`} className={`${sectionLabelClasses()} mb-1.5 block`}>
                Jurisdiction
              </label>
              <select
                id={`${id}-jurisdiction`}
                className={FIELD}
                value={country}
                onChange={(e) => setJurisdiction(e.target.value || undefined)}
                aria-describedby={`${id}-jurisdiction-help`}
              >
                <option value="">Any jurisdiction</option>
                {COUNTRY_JURISDICTIONS.map((j) => (
                  <option key={j.code} value={j.code}>
                    {j.name}
                  </option>
                ))}
              </select>
              {subdivisions.length > 0 && (
                <>
                  <label htmlFor={`${id}-subdivision`} className="sr-only">
                    State or province in {jurisdictionName(country)}
                  </label>
                  <select
                    id={`${id}-subdivision`}
                    className={`${FIELD} mt-2`}
                    value={filters.jurisdiction?.includes("-") ? filters.jurisdiction : ""}
                    onChange={(e) => setJurisdiction(e.target.value || country)}
                  >
                    <option value="">Anywhere in {jurisdictionName(country)}</option>
                    {subdivisions.map((s) => (
                      <option key={s.code} value={s.code}>
                        {s.name}
                      </option>
                    ))}
                  </select>
                </>
              )}
              <p id={`${id}-jurisdiction-help`} className="mt-1.5 text-oo-meta text-oo-muted">
                Where the company is incorporated, as GLEIF records it.
              </p>
            </div>

            <div className="sm:col-span-2">
              <LegalFormPicker
                value={filters.legalForm}
                onChange={(codes) => set({ legalForm: codes })}
                list={legalForms}
                loading={legalFormsLoading}
                failed={legalFormsFailed}
                jurisdictionName={filters.jurisdiction ? jurisdictionName(filters.jurisdiction) : null}
                needsJurisdiction={!filters.jurisdiction}
              />
            </div>

            <fieldset>
              <legend className={`${sectionLabelClasses()} mb-1.5`}>Company status</legend>
              <div className="flex overflow-hidden rounded border border-oo-rule bg-white">
                {[["", "Any"], ...Object.entries(ENTITY_STATUS_WORDS)].map(([value, word], i) => {
                  const checked = (filters.entityStatus ?? "") === value;
                  return (
                    <label
                      key={value || "any"}
                      className={`flex min-h-[44px] flex-1 cursor-pointer items-center justify-center text-oo-small has-[:focus-visible]:ring-2 has-[:focus-visible]:ring-inset has-[:focus-visible]:ring-oo-blue ${
                        i > 0 ? "border-l border-oo-rule" : ""
                      } ${checked ? "bg-oo-soft font-semibold text-oo-blue" : "text-oo-burst"}`}
                    >
                      <input
                        type="radio"
                        name={`${id}-status`}
                        value={value}
                        checked={checked}
                        onChange={() => set({ entityStatus: value || undefined })}
                        className="sr-only"
                      />
                      {word}
                    </label>
                  );
                })}
              </div>
              <p className="mt-1.5 text-oo-meta text-oo-muted">As GLEIF records it.</p>
            </fieldset>

            <div>
              <label htmlFor={`${id}-registration`} className={`${sectionLabelClasses()} mb-1.5 block`}>
                LEI registration
              </label>
              <select
                id={`${id}-registration`}
                className={FIELD}
                value={filters.registrationStatus?.[0] ?? ""}
                onChange={(e) => set({ registrationStatus: e.target.value ? [e.target.value] : undefined })}
                aria-describedby={`${id}-registration-help`}
              >
                <option value="">Any</option>
                {Object.entries(REGISTRATION_STATUS_WORDS).map(([value, word]) => (
                  <option key={value} value={value}>
                    {word}
                  </option>
                ))}
              </select>
              <p id={`${id}-registration-help`} className="mt-1.5 text-oo-meta text-oo-muted">
                The LEI record’s status — not the company’s.
              </p>
            </div>

            <div>
              <label htmlFor={`${id}-category`} className={`${sectionLabelClasses()} mb-1.5 block`}>
                Entity category
              </label>
              <select
                id={`${id}-category`}
                className={FIELD}
                value={filters.category?.[0] ?? ""}
                onChange={(e) => set({ category: e.target.value ? [e.target.value] : undefined })}
              >
                <option value="">Any</option>
                {Object.entries(CATEGORY_WORDS).map(([value, word]) => (
                  <option key={value} value={value}>
                    {word}
                  </option>
                ))}
              </select>
            </div>
          </div>
          <div className="mt-4 flex flex-wrap items-center justify-between gap-2">
            <p className="text-oo-meta text-oo-muted">
              These filters narrow GLEIF’s LEI records, which is what Company name searches.
            </p>
            {count > 0 && (
              <Button variant="ghost" size="sm" onClick={() => onChange({})} className="!px-1.5 !text-oo-muted">
                Clear filters
              </Button>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
