import { useCallback, useEffect, useRef, useState } from "react";

/**
 * The single Export entry point in the AI summary card header.
 *
 * One "Export" button opens a grouped menu: **Report** items download (PDF /
 * Markdown — both post the card's narrative + dispositions, which is why this
 * control lives on the AI summary card), and the **Data** item is an honest
 * jump link down to the "Download data" section rather than a fake download.
 * On small viewports the same items render as a bottom sheet.
 */

/** DOM id of the "Download data" section this menu's Data item jumps to. */
export const DATA_SECTION_ID = "download-data";

/**
 * Roving-focus keyboard navigation for a vertical menu. Pure so it can be
 * unit-tested: returns the next focus index for a key, or null when the key
 * is not a navigation key.
 */
export function nextMenuIndex(
  current: number,
  key: string,
  count: number
): number | null {
  if (count <= 0) return null;
  switch (key) {
    case "ArrowDown":
      return (current + 1) % count;
    case "ArrowUp":
      return (current - 1 + count) % count;
    case "Home":
      return 0;
    case "End":
      return count - 1;
    default:
      return null;
  }
}

/** Scroll to the Download data section and move focus to its heading. */
export function jumpToDataSection(): void {
  const target = document.getElementById(DATA_SECTION_ID);
  if (!target) return;
  const reduced = window.matchMedia?.(
    "(prefers-reduced-motion: reduce)"
  )?.matches;
  target.scrollIntoView({ behavior: reduced ? "auto" : "smooth" });
  const heading = target.querySelector<HTMLElement>("[data-export-target]");
  heading?.focus({ preventScroll: true });
}

const ITEM_CLASSES =
  "w-full flex items-start gap-2.5 text-left rounded-oo px-3 py-2.5 text-[13px] text-oo-ink hover:bg-[#eef1fb] focus:bg-[#eef1fb] focus:outline-none disabled:opacity-60";

function GroupHeading({ children }: { children: string }) {
  return (
    <div
      role="presentation"
      className="px-3 pt-2 pb-1 text-[10px] font-semibold uppercase tracking-oo-eyebrow text-oo-muted"
    >
      {children}
    </div>
  );
}

export function ExportMenu({
  pdfBusy,
  mdBusy,
  onPdf,
  onMarkdown,
}: {
  pdfBusy: boolean;
  mdBusy: boolean;
  onPdf: () => void;
  onMarkdown: () => void;
}) {
  const [open, setOpen] = useState(false);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const itemRefs = useRef<(HTMLButtonElement | null)[]>([]);

  const close = useCallback((refocus = true) => {
    setOpen(false);
    if (refocus) triggerRef.current?.focus();
  }, []);

  // Close on click/tap outside (the trigger and menu are both "inside").
  useEffect(() => {
    if (!open) return;
    const onDown = (e: PointerEvent) => {
      const t = e.target as Node;
      if (menuRef.current?.contains(t) || triggerRef.current?.contains(t))
        return;
      close(false);
    };
    document.addEventListener("pointerdown", onDown);
    return () => document.removeEventListener("pointerdown", onDown);
  }, [open, close]);

  // Focus the first item when the menu opens.
  useEffect(() => {
    if (open) itemRefs.current[0]?.focus();
  }, [open]);

  function onMenuKeyDown(e: React.KeyboardEvent) {
    if (e.key === "Escape") {
      e.preventDefault();
      close();
      return;
    }
    if (e.key === "Tab") {
      close(false); // let focus move on naturally
      return;
    }
    const items = itemRefs.current.filter(Boolean) as HTMLButtonElement[];
    const current = items.findIndex((el) => el === document.activeElement);
    const next = nextMenuIndex(current, e.key, items.length);
    if (next !== null) {
      e.preventDefault();
      items[next]?.focus();
    }
  }

  function item(index: number) {
    return (el: HTMLButtonElement | null) => {
      itemRefs.current[index] = el;
    };
  }

  return (
    <div className="relative shrink-0">
      <button
        ref={triggerRef}
        type="button"
        aria-haspopup="menu"
        aria-expanded={open}
        onClick={() => setOpen((o) => !o)}
        onKeyDown={(e) => {
          if (e.key === "ArrowDown" && !open) {
            e.preventDefault();
            setOpen(true);
          }
        }}
        className="whitespace-nowrap inline-flex items-center gap-1.5 rounded-oo border border-oo-blue text-oo-blue text-[12px] font-medium px-3 py-1.5 hover:bg-[#eef1fb]"
      >
        Export
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox="0 0 16 16"
          fill="currentColor"
          className="w-3.5 h-3.5"
          aria-hidden="true"
        >
          <path
            fillRule="evenodd"
            d="M4.22 6.22a.75.75 0 0 1 1.06 0L8 8.94l2.72-2.72a.75.75 0 1 1 1.06 1.06l-3.25 3.25a.75.75 0 0 1-1.06 0L4.22 7.28a.75.75 0 0 1 0-1.06Z"
            clipRule="evenodd"
          />
        </svg>
      </button>

      {open && (
        <>
          {/* Mobile-only scrim behind the bottom sheet. */}
          <div
            className="sm:hidden fixed inset-0 bg-oo-navy/35 z-40"
            aria-hidden="true"
            onClick={() => close(false)}
          />
          <div
            ref={menuRef}
            role="menu"
            aria-label="Export"
            onKeyDown={onMenuKeyDown}
            className={
              // <sm: bottom sheet. sm+: dropdown anchored to the trigger.
              "z-50 bg-white border border-oo-rule shadow-oo-card " +
              "fixed inset-x-0 bottom-0 rounded-t-xl p-2 pb-4 " +
              "sm:absolute sm:inset-x-auto sm:bottom-auto sm:right-0 sm:top-full sm:mt-1.5 sm:w-64 sm:rounded-oo sm:p-1.5"
            }
          >
            <div
              className="sm:hidden mx-auto mt-1 mb-2 h-1 w-9 rounded-full bg-oo-rule"
              aria-hidden="true"
            />
            <GroupHeading>Report</GroupHeading>
            <button
              ref={item(0)}
              type="button"
              role="menuitem"
              disabled={pdfBusy}
              onClick={() => {
                close(false);
                onPdf();
              }}
              className={ITEM_CLASSES}
              title="Download an accessible PDF report of these findings"
            >
              <span aria-hidden="true" className="text-oo-blue mt-px">
                ⬇
              </span>
              <span>
                {pdfBusy ? "Preparing PDF…" : "Report as PDF"}
                <span className="block text-[11px] text-oo-muted">
                  Accessible, tagged PDF/UA-1
                </span>
              </span>
            </button>
            <button
              ref={item(1)}
              type="button"
              role="menuitem"
              disabled={mdBusy}
              onClick={() => {
                close(false);
                onMarkdown();
              }}
              className={ITEM_CLASSES}
              title="Download the report as portable Markdown"
            >
              <span aria-hidden="true" className="text-oo-blue mt-px">
                ⬇
              </span>
              <span>
                {mdBusy ? "Preparing Markdown…" : "Report as Markdown"}
                <span className="block text-[11px] text-oo-muted">
                  Portable text — wikis, notes, LLM pipelines
                </span>
              </span>
            </button>
            <div
              className="my-1.5 border-t border-oo-rule"
              role="presentation"
            />
            <GroupHeading>Data</GroupHeading>
            <button
              ref={item(2)}
              type="button"
              role="menuitem"
              onClick={() => {
                close(false);
                jumpToDataSection();
              }}
              className={ITEM_CLASSES}
            >
              <span aria-hidden="true" className="text-oo-blue mt-px">
                ↓
              </span>
              <span className="text-oo-blue">
                Download data
                <span className="block text-[11px] text-oo-muted">
                  Jump to the data section
                </span>
              </span>
            </button>
          </div>
        </>
      )}
    </div>
  );
}
