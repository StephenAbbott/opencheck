import { useId, useState } from "react";
import type { Form13fPayload, Form13fSecurity } from "../../lib/api";
import {
  ADR_LABEL,
  COLLAPSED_ROWS,
  FORM13F_ASIDE,
  FORM13F_HEADING,
  OPEN_ROWS,
  form13fView,
  hiddenRows,
  percentText,
  securityFooter,
  securityLabel,
  sharesText,
  thousandsNote,
  toggleLabel,
  valueText,
} from "../../lib/form13f";
import { ActionChip, Chip } from "../ui";
import PanelSection from "../ui/PanelSection";

/**
 * "Institutional shareholders" — the managers that reported a position in the
 * company's securities on SEC Form 13F (Phase 336).
 *
 * Reads the frozen `form13f` event only, so it renders the same on a saved
 * report as on the live page (it fetches nothing — unlike Securities, which
 * is hidden on a saved report). Never ownership: no total, no graph, no
 * signal; the disclaimer is always visible. Collapsed it shows the largest
 * manager per security; opened, the twenty the event carries.
 */
export function InstitutionalShareholdersSection({ payload }: { payload: Form13fPayload | null }) {
  const [open, setOpen] = useState(false);
  const uid = useId();
  const view = form13fView(payload);
  if (!view) return null;
  const bodyId = `${uid}-rows`;
  const hidden = hiddenRows(view.securities);
  const rows = open ? OPEN_ROWS : COLLAPSED_ROWS;

  return (
    <PanelSection title={FORM13F_HEADING} aside={FORM13F_ASIDE}>
      <div id={bodyId}>
        <p className="text-oo-small text-oo-ink">{view.disclaimer}</p>
        {view.period && <p className="mt-1 text-oo-meta text-oo-muted">{view.period}</p>}

        {view.status !== "found" ? (
          <p className="mt-3 text-oo-small text-oo-muted">{view.line}</p>
        ) : (
          view.securities.map((s) => (
            <SecurityBlock key={s.cusip} security={s} rows={rows} open={open} />
          ))
        )}

        {hidden > 0 && (
          <div className="mt-3">
            <ActionChip onClick={() => setOpen((v) => !v)} expanded={open} controls={bodyId}>
              {toggleLabel(open, view.securities)}
            </ActionChip>
          </div>
        )}

        {open && (
          <div className="mt-3 space-y-1 text-oo-meta text-oo-muted">
            {view.notListed && <p>{view.notListed}</p>}
            {view.notes.map((n) => (
              <p key={n}>{n}</p>
            ))}
          </div>
        )}

        <p className="mt-3 text-oo-meta text-oo-muted">
          {view.caveat}{" "}
          <a
            href={view.sourceUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="text-oo-blue underline underline-offset-2 hover:text-oo-ink"
          >
            {view.attribution}
          </a>
        </p>
      </div>
    </PanelSection>
  );
}

function SecurityBlock({
  security: s,
  rows,
  open,
}: {
  security: Form13fSecurity;
  rows: number;
  open: boolean;
}) {
  const shown = s.positions.slice(0, rows);
  const hasPercent = s.positions.some((p) => p.percent !== null);
  const note = thousandsNote(s, rows);
  return (
    <div className="mt-3">
      <div className="flex flex-wrap items-center gap-2">
        <h3 className="text-oo-small font-semibold text-oo-ink">{securityLabel(s)}</h3>
        <span className="font-mono text-oo-meta text-oo-muted">CUSIP {s.cusip}</span>
        {s.is_adr && <Chip tone="neutral">{ADR_LABEL}</Chip>}
      </div>
      <table className="mt-1.5 w-full table-fixed text-oo-small">
        <caption className="sr-only">
          {`Managers reporting ${securityLabel(s)}, largest by shares`}
        </caption>
        <thead>
          <tr className="text-left text-oo-meta text-oo-muted">
            <th scope="col" className="py-1 pr-2 font-medium">Manager</th>
            <th scope="col" className="w-[7.5rem] py-1 pr-2 text-right font-medium">Shares</th>
            <th scope="col" className="hidden w-[6rem] py-1 pr-2 text-right font-medium sm:table-cell">
              Value
            </th>
            {hasPercent && (
              <th scope="col" className="w-[4.5rem] py-1 text-right font-medium">
                <span aria-hidden="true">%</span>
                <span className="sr-only">Percentage of shares outstanding</span>
              </th>
            )}
          </tr>
        </thead>
        <tbody>
          {shown.map((p) => (
            <tr key={`${p.manager_cik ?? p.manager}`} className="border-t border-oo-rule/60 align-top">
              <td className="py-1.5 pr-2 text-oo-ink break-words">
                {p.manager}
                <span className="block text-oo-meta text-oo-muted sm:hidden">
                  Value {valueText(p)}
                </span>
              </td>
              <td className="py-1.5 pr-2 text-right font-mono text-oo-ink">{sharesText(p.shares)}</td>
              <td className="hidden py-1.5 pr-2 text-right text-oo-muted sm:table-cell">{valueText(p)}</td>
              {hasPercent && (
                <td className="py-1.5 text-right font-mono text-oo-ink">{percentText(p) ?? "—"}</td>
              )}
            </tr>
          ))}
        </tbody>
      </table>
      {note && <p className="mt-1 text-oo-meta text-oo-muted">{note}</p>}
      {open ? (
        <div className="mt-1 space-y-0.5 text-oo-meta text-oo-muted">
          {securityFooter(s).map((line) => (
            <p key={line}>{line}</p>
          ))}
        </div>
      ) : (
        (s.shares_outstanding?.basis || s.adr_note) && (
          <p className="mt-1 text-oo-meta text-oo-muted">
            {s.shares_outstanding?.basis ?? s.adr_note}
          </p>
        )
      )}
    </div>
  );
}
