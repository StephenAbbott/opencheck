"""Phase 333 — the Form 13F index builder (``scripts/build_form13f_index.py``).

Every index here is produced by the real builder from zips in the SEC's exact
published shape: tab-separated, unquoted, the full column headers of the
June–August 2026 data set, ``DD-MON-YYYY`` dates. The rows are modelled on
what that data set holds for Moody's (CUSIP 615369105), period 30 June 2026:
Berkshire Hathaway's 24,669,778 shares filed as three rows of one report,
CalSTRS's transposed shares and value, T. Rowe Price's values in thousands,
restatements and NEW HOLDINGS amendments, and a late filing for an older
quarter that must not decide the period.

A 13F filer is an investment manager, not a beneficial owner: nothing here
reaches BODS or the risk engine (Stephen, 14 Oct 2026). These tests pin what
the index holds and what it refuses.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import json
import sqlite3
import sys
import zipfile
from pathlib import Path
from typing import Any

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_form13f_index.py"


def _load_builder():
    spec = importlib.util.spec_from_file_location("_build_form13f_index", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses look their module up here
    spec.loader.exec_module(module)
    return module


builder = _load_builder()

# ---------------------------------------------------------------------------
# Zips in the SEC's shape
# ---------------------------------------------------------------------------

SUBMISSION_COLS = ["ACCESSION_NUMBER", "FILING_DATE", "SUBMISSIONTYPE", "CIK", "PERIODOFREPORT"]
COVERPAGE_COLS = [
    "ACCESSION_NUMBER", "REPORTCALENDARORQUARTER", "ISAMENDMENT", "AMENDMENTNO", "AMENDMENTTYPE",
    "CONFDENIEDEXPIRED", "DATEDENIEDEXPIRED", "DATEREPORTED", "REASONFORNONCONFIDENTIALITY",
    "FILINGMANAGER_NAME", "FILINGMANAGER_STREET1", "FILINGMANAGER_STREET2", "FILINGMANAGER_CITY",
    "FILINGMANAGER_STATEORCOUNTRY", "FILINGMANAGER_ZIPCODE", "REPORTTYPE", "FORM13FFILENUMBER",
    "CRDNUMBER", "SECFILENUMBER", "PROVIDEINFOFORINSTRUCTION5", "ADDITIONALINFORMATION",
]
SUMMARY_COLS = [
    "ACCESSION_NUMBER", "OTHERINCLUDEDMANAGERSCOUNT", "TABLEENTRYTOTAL", "TABLEVALUETOTAL",
    "ISCONFIDENTIALOMITTED",
]
INFOTABLE_COLS = [
    "ACCESSION_NUMBER", "INFOTABLE_SK", "NAMEOFISSUER", "TITLEOFCLASS", "CUSIP", "FIGI", "VALUE",
    "SSHPRNAMT", "SSHPRNAMTTYPE", "PUTCALL", "INVESTMENTDISCRETION", "OTHERMANAGER",
    "VOTING_AUTH_SOLE", "VOTING_AUTH_SHARED", "VOTING_AUTH_NONE",
]

MOODYS = "615369105"
PRICE = 452.92  # the median implied price of Moody's on 30 June 2026


def _tsv(cols: list[str], rows: list[dict[str, Any]]) -> str:
    lines = ["\t".join(cols)]
    for row in rows:
        lines.append("\t".join(str(row.get(c, "")) for c in cols))
    return "\n".join(lines) + "\n"


class DataSetBuilder:
    """One data set: filings with their cover, summary and holdings rows."""

    def __init__(self) -> None:
        self.submissions: list[dict[str, Any]] = []
        self.covers: list[dict[str, Any]] = []
        self.summaries: list[dict[str, Any]] = []
        self.holdings: list[dict[str, Any]] = []
        self._sk = 1000

    def filing(
        self,
        acc: str,
        cik: str,
        manager: str,
        *,
        period: str = "30-JUN-2026",
        filed: str = "14-AUG-2026",
        kind: str = "13F-HR",
        amendment: str = "",
        report: str = "13F HOLDINGS REPORT",
        included: int = 0,
        confidential: str = "N",
    ) -> "DataSetBuilder":
        self.submissions.append({
            "ACCESSION_NUMBER": acc, "FILING_DATE": filed, "SUBMISSIONTYPE": kind,
            "CIK": cik.zfill(10), "PERIODOFREPORT": period,
        })
        self.covers.append({
            "ACCESSION_NUMBER": acc, "REPORTCALENDARORQUARTER": period,
            "ISAMENDMENT": "Y" if amendment or kind.endswith("/A") else "N",
            "AMENDMENTTYPE": amendment, "FILINGMANAGER_NAME": manager, "REPORTTYPE": report,
        })
        self.summaries.append({
            "ACCESSION_NUMBER": acc, "OTHERINCLUDEDMANAGERSCOUNT": included,
            "ISCONFIDENTIALOMITTED": confidential,
        })
        return self

    def holding(
        self,
        acc: str,
        shares: int,
        value: int | None = None,
        *,
        cusip: str = MOODYS,
        issuer: str = "MOODYS CORP",
        title: str = "COM",
        kind: str = "SH",
        putcall: str = "",
        discretion: str = "SOLE",
    ) -> "DataSetBuilder":
        self._sk += 1
        if value is None:
            value = round(shares * PRICE)
        self.holdings.append({
            "ACCESSION_NUMBER": acc, "INFOTABLE_SK": self._sk, "NAMEOFISSUER": issuer,
            "TITLEOFCLASS": title, "CUSIP": cusip, "VALUE": value, "SSHPRNAMT": shares,
            "SSHPRNAMTTYPE": kind, "PUTCALL": putcall, "INVESTMENTDISCRETION": discretion,
        })
        return self

    def write(self, path: Path, *, drop: str | None = None, columns: dict[str, list[str]] | None = None) -> Path:
        members = {
            "SUBMISSION.tsv": (SUBMISSION_COLS, self.submissions),
            "COVERPAGE.tsv": (COVERPAGE_COLS, self.covers),
            "SUMMARYPAGE.tsv": (SUMMARY_COLS, self.summaries),
            "INFOTABLE.tsv": (INFOTABLE_COLS, self.holdings),
        }
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
            for name, (cols, rows) in members.items():
                if name == drop:
                    continue
                zf.writestr(name, _tsv((columns or {}).get(name, cols), rows))
        return path


def _bystanders(ds: DataSetBuilder, n: int, *, start: int = 9000) -> None:
    """Small managers with one Moody's position each, so the price median has
    a sample and the period a volume."""
    for i in range(n):
        acc = f"0000{start + i}-26-000001"
        ds.filing(acc, str(start + i), f"Small Adviser {i} LLC")
        ds.holding(acc, 100 + i)


def _moodys_quarter() -> DataSetBuilder:
    ds = DataSetBuilder()
    # Berkshire: one report, three rows (its included managers), DFND.
    ds.filing("0000950123-26-008611", "1067983", "Berkshire Hathaway Inc", included=14)
    for part in (20_000_000, 4_000_000, 669_778):
        ds.holding("0000950123-26-008611", part, discretion="DFND")
    # TCI, sole discretion.
    ds.filing("0001647251-26-000004", "1647251", "TCI FUND MANAGEMENT LTD")
    ds.holding("0001647251-26-000004", 14_334_027)
    # CalSTRS transposed shares and value.
    ds.filing("0001081019-26-000006", "1081019", "CALIFORNIA STATE TEACHERS RETIREMENT SYSTEM")
    ds.holding("0001081019-26-000006", 100_442_257, 221_766)
    # T. Rowe Price: value still in thousands.
    ds.filing("0000080255-26-000017", "80255", "PRICE T ROWE ASSOCIATES INC /MD/", report="13F COMBINATION REPORT")
    ds.holding("0000080255-26-000017", 1_545_468, 699_974)
    # An option position and a bond line, both left out.
    ds.filing("0000093751-26-000010", "93751", "STATE STREET CORP")
    ds.holding("0000093751-26-000010", 7_111_386, discretion="DFND")
    ds.holding("0000093751-26-000010", 50_000, putcall="Call")
    ds.holding("0000093751-26-000010", 2_000_000, 1_980_000, kind="PRN")
    _bystanders(ds, 6)
    return ds


@pytest.fixture
def built(tmp_path: Path) -> tuple[sqlite3.Connection, dict[str, str]]:
    latest = _moodys_quarter()
    # A late filing for an older quarter: present, never the period.
    latest.filing("0000999999-26-000001", "999999", "Late Filer LLC", period="30-SEP-2025", filed="02-JUL-2026")
    latest.holding("0000999999-26-000001", 5_000_000)
    zip_path = latest.write(tmp_path / "01jun2026-31aug2026_form13f.zip")
    data_sets = [builder.DataSet(zip_path.name, str(zip_path), dt.date(2026, 8, 31))]
    out = tmp_path / "form13f.sqlite"
    meta = builder.build_index(
        [zip_path], out, data_sets=data_sets, min_filings=5,
        frames={"CY2026Q2I": [{"cik": 1059556, "end": "2026-06-30", "val": 173_200_000,
                               "accn": "0001628280-26-049398", "entityName": "Moody’s Corporation"}]},
        started=dt.datetime(2026, 10, 14, 9, 44, 10, 123456, tzinfo=dt.timezone.utc),
    )
    return sqlite3.connect(out), meta


def _positions(conn: sqlite3.Connection, cusip: str = MOODYS) -> list[tuple[Any, ...]]:
    return conn.execute(
        "SELECT m.name, p.shares, p.value, p.value_check, p.discretion FROM positions p "
        "JOIN managers m USING (cik) WHERE p.cusip = ? ORDER BY p.rank", (cusip,),
    ).fetchall()


# ---------------------------------------------------------------------------
# The listing page
# ---------------------------------------------------------------------------


def test_links_are_read_off_the_page_from_either_directory_newest_first() -> None:
    html = (
        '<a href="/files/structureddata/data/form-13f-data-sets/01mar2026-31may2026_form13f.zip">x</a>'
        '<a href="/files/datastandardsinnovation/data/form-13f-data-sets/01jun2026-31aug2026_form13f.zip">y</a>'
        '<a href="/files/structureddata/data/form-13f-data-sets/2023q4_form13f.zip">z</a>'
        '<a href="/files/structureddata/data/form-13f-data-sets/01jun2026-31aug2026_form13f.zip">dup</a>'
    )
    sets = builder.data_sets_from_listing(html)
    assert [d.name for d in sets] == [
        "01jun2026-31aug2026_form13f.zip",
        "01mar2026-31may2026_form13f.zip",
        "2023q4_form13f.zip",
    ]
    # The newest moved directory in Sept 2026; the link is taken as published.
    assert sets[0].url == (
        "https://www.sec.gov/files/datastandardsinnovation/data/form-13f-data-sets/"
        "01jun2026-31aug2026_form13f.zip"
    )
    assert sets[0].filed_through == dt.date(2026, 8, 31)
    assert sets[2].filed_through == dt.date(2023, 12, 31)


def test_window_end_reads_both_naming_schemes() -> None:
    assert builder.window_end("01dec2025-28feb2026_form13f.zip") == dt.date(2026, 2, 28)
    assert builder.window_end("2024q1_form13f.zip") == dt.date(2024, 3, 31)
    assert builder.window_end("2024q3_form13f.zip") == dt.date(2024, 9, 30)
    assert builder.window_end("readme.zip") is None


def test_frames_are_the_period_quarter_and_the_next() -> None:
    assert builder.frames_for(dt.date(2026, 6, 30)) == ("CY2026Q2I", "CY2026Q3I")
    assert builder.frames_for(dt.date(2026, 12, 31)) == ("CY2026Q4I", "CY2027Q1I")


# ---------------------------------------------------------------------------
# What the index holds for Moody's
# ---------------------------------------------------------------------------


def test_berkshire_rows_are_one_position_and_the_largest(built) -> None:
    conn, _ = built
    rows = _positions(conn)
    assert rows[0][:2] == ("Berkshire Hathaway Inc", 24_669_778)
    assert rows[0][3:] == ("ok", "DFND")
    manager = conn.execute(
        "SELECT name, included_managers, report_type FROM managers WHERE cik = '1067983'"
    ).fetchone()
    assert manager == ("Berkshire Hathaway Inc", 14, "13F HOLDINGS REPORT")


def test_transposed_shares_and_value_are_left_out_and_counted(built) -> None:
    conn, meta = built
    names = [r[0] for r in _positions(conn)]
    assert "CALIFORNIA STATE TEACHERS RETIREMENT SYSTEM" not in names
    positions, excluded_price = conn.execute(
        "SELECT positions, excluded_price FROM securities WHERE cusip = ?", (MOODYS,)
    ).fetchone()
    assert excluded_price == 1
    assert int(meta["excluded_price"]) == 1
    assert positions == len(names)


def test_value_in_thousands_is_flagged_never_repaired(built) -> None:
    conn, meta = built
    row = next(r for r in _positions(conn) if r[0].startswith("PRICE T ROWE"))
    assert row[1:4] == (1_545_468, 699_974, "thousands")
    assert meta["value_in_thousands"] == "1"


def test_options_and_principal_amounts_are_not_positions(built) -> None:
    conn, _ = built
    state_street = [r for r in _positions(conn) if r[0] == "STATE STREET CORP"]
    assert [r[1] for r in state_street] == [7_111_386]


def test_a_late_filing_for_an_older_quarter_is_not_the_period(built) -> None:
    conn, meta = built
    assert meta["period"] == "2026-06-30"
    assert "Late Filer LLC" not in [r[0] for r in _positions(conn)]


def test_shares_outstanding_and_meta(built) -> None:
    conn, meta = built
    assert conn.execute("SELECT shares, as_of, frame FROM shares_outstanding WHERE cik = '1059556'").fetchone() == (
        173_200_000, "2026-06-30", "CY2026Q2I",
    )
    assert meta["built_at"] == "2026-10-14T09:44:10+00:00"
    assert meta["filed_through"] == "2026-08-31"
    assert json.loads(meta["data_sets"]) == ["01jun2026-31aug2026_form13f.zip"]
    assert meta["license"] == "US-PD"
    stored = dict(conn.execute("SELECT key, value FROM meta"))
    assert stored == meta


def test_the_index_holds_no_total(built) -> None:
    """Positions overlap (a manager's report can include another manager that
    also filed): there is no column a total could be read from."""
    conn, _ = built
    cols = {r[1] for r in conn.execute("PRAGMA table_info(securities)")}
    assert not {c for c in cols if "total" in c or "sum" in c}


# ---------------------------------------------------------------------------
# Amendments
# ---------------------------------------------------------------------------


def _amendment_set(tmp_path: Path, extra) -> sqlite3.Connection:
    ds = DataSetBuilder()
    _bystanders(ds, 6)
    extra(ds)
    zip_path = ds.write(tmp_path / "01jun2026-31aug2026_form13f.zip")
    out = tmp_path / "a.sqlite"
    builder.build_index(
        [zip_path], out, min_filings=5,
        data_sets=[builder.DataSet(zip_path.name, str(zip_path), dt.date(2026, 8, 31))],
    )
    return sqlite3.connect(out)


def test_a_restatement_replaces_the_original(tmp_path: Path) -> None:
    def extra(ds):
        ds.filing("0001948780-26-000001", "1948780", "Corient Private Wealth LP", filed="12-AUG-2026")
        ds.holding("0001948780-26-000001", 50_000)
        ds.filing("0001948780-26-000002", "1948780", "Corient Private Wealth LP",
                  filed="17-AUG-2026", kind="13F-HR/A", amendment="RESTATEMENT")
        ds.holding("0001948780-26-000002", 48_000)

    conn = _amendment_set(tmp_path, extra)
    row = [r for r in _positions(conn) if r[0] == "Corient Private Wealth LP"]
    assert [r[1] for r in row] == [48_000]
    assert conn.execute("SELECT amended, accession FROM managers WHERE cik = '1948780'").fetchone() == (
        1, "0001948780-26-000002",
    )


def test_new_holdings_add_to_the_report_they_follow(tmp_path: Path) -> None:
    def extra(ds):
        ds.filing("0000853758-26-000001", "853758", "MERCER GLOBAL ADVISORS INC /ADV", filed="11-AUG-2026")
        ds.holding("0000853758-26-000001", 10_000)
        ds.filing("0000853758-26-000002", "853758", "MERCER GLOBAL ADVISORS INC /ADV",
                  filed="19-AUG-2026", kind="13F-HR/A", amendment="NEW HOLDINGS")
        ds.holding("0000853758-26-000002", 2_500)

    conn = _amendment_set(tmp_path, extra)
    assert [r[1] for r in _positions(conn) if r[0].startswith("MERCER")] == [12_500]


def test_new_holdings_before_a_restatement_are_superseded(tmp_path: Path) -> None:
    def extra(ds):
        ds.filing("0000000001-26-000001", "1", "Restating Adviser", filed="01-AUG-2026")
        ds.holding("0000000001-26-000001", 1_000)
        ds.filing("0000000001-26-000002", "1", "Restating Adviser", filed="05-AUG-2026",
                  kind="13F-HR/A", amendment="NEW HOLDINGS")
        ds.holding("0000000001-26-000002", 500)
        ds.filing("0000000001-26-000003", "1", "Restating Adviser", filed="09-AUG-2026",
                  kind="13F-HR/A", amendment="RESTATEMENT")
        ds.holding("0000000001-26-000003", 1_200)

    conn = _amendment_set(tmp_path, extra)
    assert [r[1] for r in _positions(conn) if r[0] == "Restating Adviser"] == [1_200]


def test_an_amendment_with_no_stated_type_is_a_restatement(tmp_path: Path) -> None:
    def extra(ds):
        ds.filing("0000000002-26-000001", "2", "Untyped Amender", filed="01-AUG-2026")
        ds.holding("0000000002-26-000001", 1_000)
        ds.filing("0000000002-26-000002", "2", "Untyped Amender", filed="03-AUG-2026", kind="13F-HR/A")
        ds.holding("0000000002-26-000002", 900)

    conn = _amendment_set(tmp_path, extra)
    assert [r[1] for r in _positions(conn) if r[0] == "Untyped Amender"] == [900]


def test_notices_carry_no_holdings(tmp_path: Path) -> None:
    def extra(ds):
        ds.filing("0000000003-26-000001", "3", "Notice Filer", kind="13F-NT", report="13F NOTICE")

    conn = _amendment_set(tmp_path, extra)
    assert conn.execute("SELECT COUNT(*) FROM managers WHERE cik = '3'").fetchone() == (0,)


def test_the_previous_data_set_supplies_a_late_report_for_the_period(tmp_path: Path) -> None:
    """Late filers land in the next set; the newer set decides the period and
    the older set's filings for that period are read too."""
    newer = DataSetBuilder()
    _bystanders(newer, 6)
    older = DataSetBuilder()
    older.filing("0000000004-26-000001", "4", "Early Filer LLC", filed="29-MAY-2026", period="30-JUN-2026")
    older.holding("0000000004-26-000001", 7_000)
    older.filing("0000000005-26-000001", "5", "Previous Quarter LLC", filed="15-MAY-2026", period="31-MAR-2026")
    older.holding("0000000005-26-000001", 9_000)
    z1 = newer.write(tmp_path / "01jun2026-31aug2026_form13f.zip")
    z2 = older.write(tmp_path / "01mar2026-31may2026_form13f.zip")
    out = tmp_path / "two.sqlite"
    builder.build_index(
        [z1, z2], out, min_filings=5,
        data_sets=[builder.DataSet(z1.name, str(z1), dt.date(2026, 8, 31)),
                   builder.DataSet(z2.name, str(z2), dt.date(2026, 5, 31))],
    )
    names = [r[0] for r in _positions(sqlite3.connect(out))]
    assert "Early Filer LLC" in names
    assert "Previous Quarter LLC" not in names


# ---------------------------------------------------------------------------
# The issuer-name check, the price sample and the cap
# ---------------------------------------------------------------------------


def _berkshire_a(tmp_path: Path, extra) -> sqlite3.Connection:
    ds = DataSetBuilder()
    for i in range(6):
        acc = f"0000{7000 + i}-26-000001"
        ds.filing(acc, str(7000 + i), f"Holder {i}")
        ds.holding(acc, 10 + i, round((10 + i) * 750_000), cusip="084670108",
                   issuer="BERKSHIRE HATHAWAY INC DEL", title="CL A")
    extra(ds)
    zip_path = ds.write(tmp_path / "01jun2026-31aug2026_form13f.zip")
    out = tmp_path / "b.sqlite"
    builder.build_index(
        [zip_path], out, min_filings=5,
        data_sets=[builder.DataSet(zip_path.name, str(zip_path), dt.date(2026, 8, 31))],
    )
    return sqlite3.connect(out)


def test_a_wrong_cusip_that_prices_like_thousands_is_left_out(tmp_path: Path) -> None:
    """"Axon Enterprise" under Berkshire A's CUSIP prices at about a
    thousandth of it — the name decides it is not Berkshire."""
    def extra(ds):
        ds.filing("0000000006-26-000001", "6", "Axon Adviser")
        ds.holding("0000000006-26-000001", 1_000, 800_000, cusip="084670108", issuer="Axon Enterprise Inc")

    conn = _berkshire_a(tmp_path, extra)
    assert "Axon Adviser" not in [r[0] for r in _positions(conn, "084670108")]
    assert conn.execute("SELECT excluded_name FROM securities WHERE cusip = '084670108'").fetchone() == (1,)


def test_a_short_name_at_the_issuers_price_is_kept(tmp_path: Path) -> None:
    """"BRK.A" names no core token, but the price vouches for the row."""
    def extra(ds):
        ds.filing("0000000007-26-000001", "7", "Ticker Writer")
        ds.holding("0000000007-26-000001", 3, 2_250_000, cusip="084670108", issuer="BRK.A")

    conn = _berkshire_a(tmp_path, extra)
    row = next(r for r in _positions(conn, "084670108") if r[0] == "Ticker Writer")
    assert row[3] == "ok"


def test_a_thinly_held_security_is_unchecked_not_dropped(tmp_path: Path) -> None:
    def extra(ds):
        ds.filing("0000000008-26-000001", "8", "Only Holder")
        ds.holding("0000000008-26-000001", 1_000, 12, cusip="12345A106", issuer="TINY CO")

    conn = _berkshire_a(tmp_path, extra)
    assert [r[3] for r in _positions(conn, "12345A106")] == ["unchecked"]
    assert conn.execute("SELECT median_price FROM securities WHERE cusip = '12345A106'").fetchone() == (None,)


def test_core_tokens_and_run_together_names() -> None:
    core = builder.core_tokens(["CHEVRON CORP NEW", "Chevron Corp", "CHEVRON CORPORATION", "Caterpillar Inc"])
    assert core == {"CHEVRON"}
    assert builder.names_issuer("CHEVRONTEXACO CORP", core)
    assert not builder.names_issuer("Caterpillar Inc", core)
    # Too few names to judge: everything passes.
    assert builder.core_tokens(["A", "B"]) == set()


def test_top_n_caps_the_stored_positions_and_keeps_the_count(tmp_path: Path) -> None:
    ds = DataSetBuilder()
    _bystanders(ds, 12)
    zip_path = ds.write(tmp_path / "01jun2026-31aug2026_form13f.zip")
    out = tmp_path / "cap.sqlite"
    builder.build_index(
        [zip_path], out, min_filings=5, top_n=5,
        data_sets=[builder.DataSet(zip_path.name, str(zip_path), dt.date(2026, 8, 31))],
    )
    conn = sqlite3.connect(out)
    assert conn.execute("SELECT positions, stored FROM securities WHERE cusip = ?", (MOODYS,)).fetchone() == (12, 5)
    shares = [r[1] for r in _positions(conn)]
    assert shares == sorted(shares, reverse=True) and shares[0] == 111


def test_shares_outstanding_takes_the_fact_nearest_the_period() -> None:
    period = dt.date(2026, 6, 30)
    rows = builder._shares_outstanding(
        {
            "CY2026Q2I": [{"cik": 1, "end": "2026-07-23", "val": 900, "accn": "a", "entityName": "X"}],
            "CY2026Q3I": [
                {"cik": 1, "end": "2026-06-30", "val": 1000, "accn": "b", "entityName": "X"},
                {"cik": 2, "end": "2026-10-20", "val": 5, "accn": "c", "entityName": "Y"},
                {"cik": 3, "end": "2026-06-30", "val": 0, "accn": "d", "entityName": "Z"},
            ],
        },
        period,
    )
    by_cik = {r[0]: r for r in rows}
    assert by_cik["1"][1:3] == (1000, "2026-06-30")
    assert by_cik["2"][1] == 5  # kept; the reader judges the distance
    assert "3" not in by_cik


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------


def test_too_few_managers_is_refused_and_nothing_is_written(tmp_path: Path) -> None:
    ds = DataSetBuilder()
    _bystanders(ds, 3)
    zip_path = ds.write(tmp_path / "01jun2026-31aug2026_form13f.zip")
    out = tmp_path / "never.sqlite"
    with pytest.raises(builder.BuildRefused, match="only 3 managers"):
        builder.build_index(
            [zip_path], out, min_filings=5,
            data_sets=[builder.DataSet(zip_path.name, str(zip_path), dt.date(2026, 8, 31))],
        )
    assert not out.exists()


def test_a_missing_member_or_column_is_refused(tmp_path: Path) -> None:
    ds = DataSetBuilder()
    _bystanders(ds, 6)
    no_info = ds.write(tmp_path / "a_form13f.zip", drop="INFOTABLE.tsv")
    with pytest.raises(builder.BuildRefused, match="no INFOTABLE.tsv"):
        builder.build_index([no_info], tmp_path / "x.sqlite", min_filings=5, data_sets=[])
    cols = [c for c in INFOTABLE_COLS if c != "SSHPRNAMTTYPE"]
    renamed = ds.write(tmp_path / "b_form13f.zip", columns={"INFOTABLE.tsv": cols})
    with pytest.raises(builder.BuildRefused, match="lacks SSHPRNAMTTYPE"):
        builder.build_index([renamed], tmp_path / "y.sqlite", min_filings=5, data_sets=[])


def test_downloading_without_a_contact_address_is_refused(monkeypatch) -> None:
    monkeypatch.delenv("OPENCHECK_EDGAR_CONTACT_EMAIL", raising=False)

    def no_network(*a, **k):  # pragma: no cover - must not be reached
        raise AssertionError("asked the SEC without a contact address")

    monkeypatch.setattr(builder, "_get", no_network)
    assert builder.main(["--out", "/nonexistent/x.sqlite"]) == 2


def test_a_download_that_is_not_a_zip_is_refused(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(builder, "_get", lambda url, contact: b"<html>Request Rate Threshold Exceeded</html>")
    ds = builder.DataSet("01jun2026-31aug2026_form13f.zip", "https://www.sec.gov/x.zip", dt.date(2026, 8, 31))
    with pytest.raises(builder.BuildRefused, match="not a zip"):
        builder.download(ds, tmp_path, "someone@example.org")
    assert not (tmp_path / ds.name).exists()


def test_main_builds_from_local_zips_and_frames(tmp_path: Path, capsys) -> None:
    zip_path = _moodys_quarter().write(tmp_path / "01jun2026-31aug2026_form13f.zip")
    frames = tmp_path / "frames"
    frames.mkdir()
    (frames / "CY2026Q2I.json").write_text(json.dumps({"data": [
        {"cik": 1059556, "end": "2026-06-30", "val": 173200000, "accn": "x", "entityName": "Moody's"},
    ]}))
    out = tmp_path / "main.sqlite"
    code = builder.main([
        "--zip", str(zip_path), "--frames-dir", str(frames), "--out", str(out),
        "--gzip", "--min-filings", "5",
    ])
    assert code == 0
    assert (tmp_path / "main.sqlite.gz").exists()
    assert "period=2026-06-30" in capsys.readouterr().out
    assert sqlite3.connect(out).execute("SELECT COUNT(*) FROM shares_outstanding").fetchone() == (1,)


# ---------------------------------------------------------------------------
# The workflow
# ---------------------------------------------------------------------------

_WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "refresh-form13f-index.yml"


def test_schema_flag_prints_the_version_without_the_network(capsys) -> None:
    assert builder.main(["--schema"]) == 0
    assert capsys.readouterr().out.strip() == builder.SCHEMA_VERSION


def test_workflow_replaces_assets_safely_and_meta_last() -> None:
    wf = _WORKFLOW.read_text()
    assert "gh release upload" not in wf  # --clobber deletes before it uploads (Phase 302)
    index_at = wf.index("replace_release_asset.sh form13f-index \\\n            \"$RUNNER_TEMP/form13f.sqlite.gz\"")
    meta_at = wf.index("\"$RUNNER_TEMP/form13f-meta.json\" form13f-meta.json")
    assert index_at < meta_at  # a failed index upload leaves the old meta, so the next run rebuilds
    assert "secrets.OPENCHECK_EDGAR_CONTACT_EMAIL" in wf
    assert "timeout-minutes" in wf and "persist-credentials: false" in wf


def test_workflow_uses_flags_the_builder_has() -> None:
    wf = _WORKFLOW.read_text()
    for flag in ("--latest", "--schema", "--out", "--gzip"):
        assert flag in wf
        assert flag in _SCRIPT.read_text()


def test_a_wrong_share_class_is_not_mistaken_for_thousands(tmp_path: Path) -> None:
    """Phase 334: Berkshire B shares at the B price under the A CUSIP sit at
    about 1/1,500 of A's price — inside a factor of two of a thousandth, so
    the old band read them as value in thousands. The thousands band is
    narrower than the price band, and they are left out."""
    def extra(ds):
        ds.filing("0000000009-26-000001", "9", "Vinva-Shaped Adviser")
        ds.holding("0000000009-26-000001", 184_831, 138_622, cusip="084670108",
                   issuer="BERKSHIRE HATHAWAY INC DEL", title="CL A")
        ds.filing("0000000010-26-000001", "10", "Real Thousands Filer")
        ds.holding("0000000010-26-000001", 20, 15_000, cusip="084670108",
                   issuer="BERKSHIRE HATHAWAY INC DEL", title="CL A")

    conn = _berkshire_a(tmp_path, extra)
    rows = {r[0]: r for r in _positions(conn, "084670108")}
    assert "Vinva-Shaped Adviser" not in rows
    assert rows["Real Thousands Filer"][3] == "thousands"
    assert builder.THOUSANDS_BAND < builder.PRICE_BAND
