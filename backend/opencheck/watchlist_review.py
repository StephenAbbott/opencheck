"""watchlist_review — when OpenCheck last looked (pKYC stage C).

Moody's recasts periodic review as *confirmation that monitoring caught what
it should*. OpenCheck has no periodic review, and its one time-based rule ran
the other way: a list nobody opened for 90 days was deleted — exactly the
lists whose owners most needed a nudge. This module gives every watch a
**review line**, computed on read from what the store already holds, and
words the **expiry notice** the worker writes before a stale list goes.

Stephen's decisions on the stage C ticket (17 Oct 2026):

- **One instance-wide threshold**, ``OPENCHECK_WATCHLIST_REVIEW_DAYS``
  (90): a watch is *due a review* when its last full re-run is that old.
  Never tied to the list's findings — a review period that depends on the
  materiality of what was found is risk-rating drift by another name.
- **The clock is the last full re-run** (``last_checked_at``), because that
  is when every source was asked. A delta naming the company moves it; a
  GLEIF fact upgrade (Phase 300) does not.
- **Due-ness is a state, not a change**: no entry per due watch. The count
  rides in the feed's ``<subtitle>`` and on the page; the one action is
  ``/recheck``, which already exists. No automatic re-run — that would be a
  schedule, the one thing the watchlist is built not to be.
- **The line never says the company is unchanged.** It says when OpenCheck
  last looked and how much of that look was fresh: sources that answered
  the run from a cache or a snapshot older than the run are named with the
  copy's date; sources that did not answer are named as not reached.
- **The stale-list rule** (``prune_stale_lists``): at
  ``OPENCHECK_WATCHLIST_STALE_DAYS`` (90) unopened the worker writes one
  notice entry per list (tier ``review``, about the list, not a company) and
  stamps ``lists.expiry_notice_at``; ``OPENCHECK_WATCHLIST_DELETE_GRACE_DAYS``
  (30) later it deletes. Any token-bearing request in between clears the
  notice. The same rule for every list, context or not.

Words shared with the page are :data:`REVIEW_WORDS`, mirrored in
``frontend/src/lib/watchlist.ts`` and pinned by ``tests/test_watchlist_review.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

#: The clauses the review line is built from. ``{n}``, ``{total}`` and
#: ``{names}`` are filled per watch; the frontend copies these.
REVIEW_WORDS: dict[str, str] = {
    "last_run": "Last fully re-run {n} days ago",
    "last_run_today": "Last fully re-run today",
    "never_rerun": "Baseline taken {n} days ago and never re-run — no delta has named this company since",
    "all_live": "every source answered live",
    "older_copy": "{n} of {total} sources answered from an older copy ({names})",
    "not_reached": "{names} not reached",
    "action_refresh": "Re-check by hand to refresh.",
    "action_confirm": "Re-check by hand to confirm.",
    "due_chip": "Review due",
    "list_due": "{n} of {total} companies have not been fully re-run in {days} days.",
}

#: The ``review`` entry tier — a note about the list, not about a company.
TIER_REVIEW = "review"
TIER_REVIEW_SENTENCE = "A note about this list, not about a company"
REASON_LIST_EXPIRING = "list_expiring"

#: A source answered "from an older copy" when its retrieval predates the run
#: by more than this many seconds (a day — a cache written the same day is
#: the run's own).
OLDER_COPY_SLACK_S = 86400.0


def _parse(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _days_between(earlier: datetime, later: datetime) -> int:
    return max(0, int((later - earlier).total_seconds() // 86400))


def _day(value: Any) -> str:
    dt = _parse(value)
    return f"{dt.day} {dt:%b %Y}" if dt else "undated"


def _join(names: list[str]) -> str:
    if len(names) <= 1:
        return "".join(names)
    return ", ".join(names[:-1]) + " and " + names[-1]


def review_of(
    watch: dict[str, Any],
    *,
    now: datetime | None = None,
    due_days: int,
    label: Any = None,
) -> dict[str, Any]:
    """The review block for one watch: when OpenCheck last fully re-ran it,
    how fresh that run's answers were, whether a review is due, and the one
    sentence that says so. ``label(source_id)`` words a source for the
    sentence (``sourceLabel``); the ids stay on the block for the page.
    Nothing here is stored."""
    now = now or datetime.now(UTC)
    words = label or (lambda s: s)
    snapshot = watch.get("snapshot") or {}
    added = _parse(watch.get("added_at"))
    last = _parse(watch.get("last_checked_at"))
    never_rerun = bool(added and last and abs((last - added).total_seconds()) < 60)
    anchor = last or added
    days = _days_between(anchor, now) if anchor else None
    due = due_days > 0 and days is not None and days >= due_days

    older: list[dict[str, Any]] = []
    total = 0
    for row in snapshot.get("checked") or []:
        if not isinstance(row, dict) or not row.get("source_id"):
            continue
        if row.get("liveness") == "stub":
            continue
        total += 1
        got = _parse(row.get("retrieved_at"))
        if last and got and (last - got).total_seconds() > OLDER_COPY_SLACK_S:
            older.append({"source_id": row["source_id"], "retrieved_at": row.get("retrieved_at")})
    not_reached: list[str] = sorted(
        {str(d["source_id"]) for d in snapshot.get("degraded_sources") or [] if isinstance(d, dict) and d.get("source_id")}
    )

    clauses: list[str] = []
    if days is None:
        clauses.append("Never fully re-run")
    elif never_rerun:
        clauses.append(REVIEW_WORDS["never_rerun"].format(n=days))
    elif days == 0:
        clauses.append(REVIEW_WORDS["last_run_today"])
    else:
        clauses.append(REVIEW_WORDS["last_run"].format(n=days))
    if not never_rerun and days is not None:
        if older:
            names = _join([f"{words(o['source_id'])} {_day(o['retrieved_at'])}" for o in older])
            clauses.append(REVIEW_WORDS["older_copy"].format(n=len(older), total=total, names=names))
        elif total and not not_reached:
            clauses.append(REVIEW_WORDS["all_live"])
        if not_reached:
            clauses.append(REVIEW_WORDS["not_reached"].format(names=_join([words(s) for s in not_reached])))
    sentence = "; ".join(clauses) + "."
    if never_rerun:
        sentence += " " + REVIEW_WORDS["action_confirm"]
    elif due or older or not_reached:
        sentence += " " + REVIEW_WORDS["action_refresh"]

    return {
        "last_run_at": watch.get("last_checked_at"),
        "last_run_days": days,
        "never_rerun": never_rerun,
        "due": due,
        "due_days": due_days,
        "sources_total": total,
        "sources_from_older_copy": older,
        "sources_not_reached": not_reached,
        "sentence": sentence,
    }


def list_due_sentence(due: int, total: int, due_days: int) -> str | None:
    """The line over the watched companies when any is due; ``None`` otherwise."""
    if due <= 0:
        return None
    return REVIEW_WORDS["list_due"].format(n=due, total=total, days=due_days)


def expiry_trigger(*, notice_at: str, delete_after: str, stale_days: int, grace_days: int) -> dict[str, Any]:
    """The trigger of a ``review``-tier notice entry."""
    return {
        "tier": TIER_REVIEW,
        "reason": REASON_LIST_EXPIRING,
        "notice_at": notice_at,
        "delete_after": delete_after,
        "stale_days": stale_days,
        "grace_days": grace_days,
    }


def expiry_sentence(trigger: dict[str, Any]) -> str:
    """The notice, in the feed's words."""
    return (
        f"Nobody has opened or polled this watchlist for {trigger.get('stale_days')} days. "
        f"It will be deleted, with its watches and this log, after {str(trigger.get('delete_after') or '')[:10]} "
        "unless it is opened or its feed is read before then. Opening it keeps it."
    )


def stale_rule_sentence(stale_days: int, grace_days: int) -> str:
    """The rule as the 404 and the docs state it."""
    return (
        f"A list that nobody opens or polls for {stale_days} days is warned in its feed, "
        f"and deleted {grace_days} days later."
    )
