"""Dates in the reader's form — "16 Sept 2026", never "2026-09-16".

The one backend formatter for a date a reader sees: the saved-report share
card, the Markdown and PDF reports' "as of" lines, and the footers the
ownership modules write under their tables. Mirrors the frontend's own
format (``lib/savedReport.ts``). A value that is not a date comes back as
written, so a wrong input is visible rather than silently dropped.
"""

from __future__ import annotations

from datetime import UTC, datetime

MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "June", "July", "Aug", "Sept", "Oct", "Nov", "Dec"]


def ui_date(iso: object) -> str:
    """"16 Sept 2026", in UTC, from an ISO date or timestamp; a year-month
    reads "Sept 2026" and a bare year stays as it is."""
    text = str(iso or "").strip()
    if not text:
        return ""
    if len(text) == 7 and text[4] == "-" and text[:4].isdigit() and text[5:].isdigit():
        month = int(text[5:])
        return f"{MONTHS[month - 1]} {text[:4]}" if 1 <= month <= 12 else text
    try:
        if len(text) == 10:
            d = datetime.fromisoformat(text)
        else:
            d = datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return text[:10] if len(text) > 10 and text[4:5] == "-" else text
    return f"{d.day} {MONTHS[d.month - 1]} {d.year}"
