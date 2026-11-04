/**
 * The legal-form field of "More filters" (the advanced-search-filters ticket).
 *
 * An APG combobox: a text input that filters a listbox, arrow keys move the
 * active option (`aria-activedescendant`), Enter picks, Escape closes. The
 * list is the jurisdiction's own (ISO 20275 defines forms per country, and
 * per state in the US), most used first where the server has counts, with
 * the abbreviation a reader recognises in bold. Before a jurisdiction is
 * chosen the field says so instead of offering 3,446 forms.
 */

import { useEffect, useId, useMemo, useRef, useState } from "react";
import {
  NO_ELF_CODE_LABEL,
  PLACEHOLDER_FORMS,
  isPlaceholderForm,
  legalFormLabel,
  matchLegalForms,
  type LegalForm,
  type LegalFormList,
} from "../lib/searchFilters";
import { sectionLabelClasses } from "./ui";

const MOST_USED = 8;

interface Option {
  id: string;
  /** null = "Any legal form"; "show-all" options carry no codes either. */
  codes: string[] | null;
  showAll?: boolean;
  abbreviation: string | null;
  name: string;
  meta: string;
}

function optionsFor(list: LegalFormList, typed: string, showAll: boolean): { options: Option[]; more: number } {
  const matched = matchLegalForms(list.forms, typed);
  const limit = typed || showAll || !list.counts_available ? matched.length : MOST_USED;
  const shown = matched.slice(0, limit);
  const options: Option[] = [];
  if (!typed) options.push({ id: "any", codes: null, abbreviation: null, name: "Any legal form", meta: "" });
  for (const form of shown) {
    options.push({
      id: form.code,
      codes: [form.code],
      abbreviation: form.abbreviation,
      name: form.name,
      meta: formMeta(form),
    });
  }
  const more = matched.length - shown.length;
  if (more > 0) {
    options.push({ id: "show-all", codes: null, showAll: true, abbreviation: null, name: `Show all ${list.forms.length} legal forms`, meta: "" });
  }
  const noCode = NO_ELF_CODE_LABEL.toLowerCase();
  if (!typed || noCode.includes(typed.toLowerCase()) || "9999".includes(typed) || "8888".includes(typed)) {
    options.push({ id: "no-elf", codes: [...PLACEHOLDER_FORMS], abbreviation: null, name: NO_ELF_CODE_LABEL, meta: "8888 · 9999" });
  }
  return { options, more };
}

function formMeta(form: LegalForm): string {
  const count =
    form.issued_leis === null
      ? ""
      : form.issued_leis >= 1000
        ? `${(form.issued_leis / 1000).toFixed(form.issued_leis >= 10_000 ? 0 : 1)}k LEIs · `
        : `${form.issued_leis} LEIs · `;
  return `${count}${form.code}`;
}

export function LegalFormPicker({
  value,
  onChange,
  list,
  loading,
  failed,
  jurisdictionName,
  needsJurisdiction,
}: {
  value: string[] | undefined;
  onChange: (codes: string[] | undefined) => void;
  list: LegalFormList | null;
  loading: boolean;
  failed: boolean;
  jurisdictionName: string | null;
  /** No jurisdiction yet: the field explains instead of listing. */
  needsJurisdiction: boolean;
}) {
  const id = useId();
  const inputId = `${id}-input`;
  const listId = `${id}-list`;
  const helpId = `${id}-help`;
  const [open, setOpen] = useState(false);
  const [typed, setTyped] = useState("");
  const [showAll, setShowAll] = useState(false);
  const [active, setActive] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);

  const selectedLabel = useMemo(() => {
    if (!value?.length) return "";
    if (isPlaceholderForm(value)) return NO_ELF_CODE_LABEL;
    const form = list?.forms.find((f) => f.code === value[0]);
    return form ? legalFormLabel(form) : value.join(", ");
  }, [value, list]);

  const { options } = useMemo(
    () => (list ? optionsFor(list, typed, showAll) : { options: [] as Option[], more: 0 }),
    [list, typed, showAll],
  );

  useEffect(() => setActive(0), [typed, showAll, list]);
  useEffect(() => {
    if (needsJurisdiction) {
      setOpen(false);
      setTyped("");
    }
  }, [needsJurisdiction]);

  function pick(option: Option) {
    if (option.showAll) {
      setShowAll(true);
      return;
    }
    onChange(option.codes ?? undefined);
    setOpen(false);
    setTyped("");
    setShowAll(false);
  }

  function onKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      if (!open) setOpen(true);
      else setActive((i) => Math.min(i + 1, options.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive((i) => Math.max(i - 1, 0));
    } else if (e.key === "Enter" && open && options[active]) {
      e.preventDefault();
      pick(options[active]);
    } else if (e.key === "Escape") {
      setOpen(false);
      setTyped("");
    }
  }

  const disabled = needsJurisdiction || (!list && !loading && !failed);
  const subdivisionNote =
    list?.has_subdivision_forms
      ? `Each ${list.jurisdiction === "US" ? "US state" : list.jurisdiction === "CA" ? "Canadian province" : "sub-division"} defines its own legal forms. Choose one under Jurisdiction to see them.`
      : null;

  let help: string;
  if (needsJurisdiction)
    help = "Legal forms are defined country by country — and state by state in the United States, province by province in Canada.";
  else if (loading) help = "Loading legal forms…";
  else if (failed) help = "The list of legal forms did not load. Try again in a moment.";
  else if (subdivisionNote) help = subdivisionNote;
  else if (list)
    help = `${list.forms.length} legal forms in ${jurisdictionName ?? list.jurisdiction_name}${list.counts_available ? ", most used first" : ""}. From the ISO 20275 Entity Legal Forms list.`;
  else help = "";

  return (
    <div className="relative">
      <label htmlFor={inputId} className={`${sectionLabelClasses()} mb-1.5 block`}>
        Legal form
      </label>
      <input
        ref={inputRef}
        id={inputId}
        type="text"
        role="combobox"
        aria-expanded={open && !disabled}
        aria-controls={listId}
        aria-autocomplete="list"
        aria-describedby={help ? helpId : undefined}
        aria-activedescendant={open && options[active] ? `${listId}-${options[active].id}` : undefined}
        disabled={disabled}
        autoComplete="off"
        spellCheck={false}
        value={open ? typed : selectedLabel}
        placeholder={needsJurisdiction ? "Choose a jurisdiction first" : open ? "Name, abbreviation or ELF code" : "Any legal form"}
        onChange={(e) => {
          setTyped(e.target.value);
          setOpen(true);
        }}
        onFocus={() => {
          if (!disabled) setOpen(true);
        }}
        onClick={() => {
          if (!disabled) setOpen(true);
        }}
        onBlur={() => {
          // Let an option's mousedown land first.
          window.setTimeout(() => {
            setOpen(false);
            setTyped("");
          }, 120);
        }}
        onKeyDown={onKeyDown}
        className="w-full min-h-[44px] rounded border border-oo-rule bg-white px-3 text-oo-body text-oo-ink placeholder:text-oo-muted focus:border-oo-blue focus:outline-none focus:ring-2 focus:ring-oo-blue/30 disabled:bg-oo-bg disabled:text-oo-muted"
      />
      {open && !disabled && list && (
        <div className="absolute left-0 right-0 z-20 mt-1 overflow-hidden rounded-oo border border-oo-softBorder bg-white shadow-oo-card">
          <ul id={listId} role="listbox" aria-label={`Legal forms in ${jurisdictionName ?? list.jurisdiction_name}`} className="max-h-80 overflow-y-auto py-1">
            {!typed && list.counts_available && !showAll && (
              <li role="presentation" className={`${sectionLabelClasses()} px-3 pb-1 pt-2`}>
                Most used in {jurisdictionName ?? list.jurisdiction_name}
              </li>
            )}
            {options.map((option, index) => (
              <li
                key={option.id}
                id={`${listId}-${option.id}`}
                role="option"
                aria-selected={
                  option.showAll
                    ? false
                    : option.codes === null
                      ? !value?.length
                      : !!value && option.codes.join() === value.join()
                }
                onMouseDown={(e) => {
                  e.preventDefault();
                  pick(option);
                }}
                onMouseEnter={() => setActive(index)}
                className={`flex min-h-[44px] cursor-pointer items-baseline gap-2.5 px-3 py-2 text-oo-small ${
                  index === active ? "bg-oo-soft" : ""
                }`}
              >
                {option.abbreviation ? (
                  <span className="w-16 shrink-0 font-semibold text-oo-ink">{option.abbreviation}</span>
                ) : option.codes && option.id !== "no-elf" ? (
                  <span aria-hidden="true" className="w-16 shrink-0 text-oo-muted">—</span>
                ) : null}
                <span className={`min-w-0 flex-1 ${option.showAll ? "font-medium text-oo-blue" : "text-oo-ink"}`}>
                  {option.name}
                </span>
                {option.meta && <span className="shrink-0 font-mono text-oo-meta text-oo-muted">{option.meta}</span>}
              </li>
            ))}
            {options.length === 0 && (
              <li role="presentation" className="px-3 py-2 text-oo-small text-oo-muted">
                No legal form in {jurisdictionName ?? list.jurisdiction_name} matches “{typed}”.
              </li>
            )}
          </ul>
        </div>
      )}
      {help && (
        <p id={helpId} className="mt-1.5 text-oo-meta text-oo-muted">
          {help}
        </p>
      )}
    </div>
  );
}
