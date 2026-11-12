/**
 * Find a Tender (UK public procurement) — the words the source card uses.
 *
 * Pure, so the logic-only suite can pin them. A value is shown beside the
 * number of suppliers it was awarded to and is never summed: a framework's
 * value is every supplier's ceiling, and one notice can carry several lots.
 */

export interface FindTenderRecord {
  ocid: string;
  award_id: string;
  notice?: string;
  url?: string;
  date?: string | null;
  date_basis?: string;
  title?: string;
  buyer?: string;
  value?: number | null;
  currency?: string;
  award_status?: string;
  contract_status?: string;
  regime?: string;
  above_threshold?: boolean | null;
  suppliers?: number;
  lots?: number;
  kept_as?: string;
}

/** What the record's date is: the notices date an award four ways. */
export function dateLabel(r: FindTenderRecord): string {
  if (!r.date) return "";
  switch (r.date_basis) {
    case "awarded":
      return `awarded ${r.date}`;
    case "signed":
      return `signed ${r.date}`;
    case "published":
      return `notice published ${r.date}`;
    default:
      return `notice updated ${r.date}`;
  }
}

/** The value as filed, in its currency, with who it was awarded to. */
export function valueLabel(r: FindTenderRecord): string {
  if (r.value == null || !(r.value > 0)) return "";
  const amount =
    r.currency === "GBP"
      ? `£${Math.round(r.value).toLocaleString("en-GB")}`
      : `${Math.round(r.value).toLocaleString("en-GB")} ${r.currency || ""}`.trim();
  const n = r.suppliers ?? 1;
  return n > 1 ? `${amount}, shared by ${n} suppliers` : amount;
}

/** A status worth saying: anything but an award with a live contract. */
export function statusLabel(r: FindTenderRecord): string {
  if (r.contract_status === "terminated") return "contract terminated";
  if (r.contract_status === "cancelled") return "contract cancelled";
  if (r.award_status === "pending") return "awarded, contract not yet signed";
  return "";
}

export function lotsLabel(r: FindTenderRecord): string {
  const n = r.lots ?? 1;
  return n > 1 ? `${n} lots` : "";
}

/** The card's closing note: where the list comes from and what it leaves out. */
export function coverageNote(viaPpon: number): string {
  const parts = [
    "Awards on Find a Tender since 2021, from the Open Contracting Partnership's weekly copy of its notices",
    "matched on the Companies House number" + (viaPpon ? ", or a PPON the notices pair with it" : ""),
  ];
  let note = parts.join(", ") + ".";
  note +=
    " Values are the contract's and are not added up. Notices before the Procurement Act (24 February 2025) rarely named a supplier's number, so earlier awards are under-counted, and contracts won through a subsidiary sit under the subsidiary's number.";
  return note;
}
