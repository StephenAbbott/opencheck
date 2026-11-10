"""Pydantic schema for Open DART (South Korea) answers.

Field names are Open DART's. Share counts and percentages arrive as strings
with thousands separators ("497,660,185", "8.51"); dates as ``YYYYMMDD`` or
``YYYY-MM-DD``. ``extra="allow"`` on ``_Base`` keeps anything DART adds later.
"""

from __future__ import annotations

from . import _Base


class DartCompany(_Base):
    """``company.json`` (or, when it did not answer, the crosswalk's row)."""

    corp_code: str | None = None
    corp_name: str | None = None
    corp_name_eng: str | None = None
    stock_name: str | None = None
    stock_code: str | None = None
    ceo_nm: str | None = None
    corp_cls: str | None = None
    jurir_no: str | None = None
    bizr_no: str | None = None
    adres: str | None = None
    hm_url: str | None = None
    induty_code: str | None = None
    est_dt: str | None = None
    acc_mt: str | None = None


class DartReport(_Base):
    """The latest periodic report, as chosen from ``list.json``."""

    rcept_no: str
    rcept_dt: str | None = None
    report_nm: str
    bsns_year: str
    reprt_code: str
    period: str


class DartShareholder(_Base):
    """One row of ``hyslrSttus.json`` (최대주주 및 특수관계인)."""

    nm: str | None = None
    relate: str | None = None
    stock_knd: str | None = None
    bsis_posesn_stock_co: str | None = None
    bsis_posesn_stock_qota_rt: str | None = None
    trmend_posesn_stock_co: str | None = None
    trmend_posesn_stock_qota_rt: str | None = None
    rm: str | None = None
    stlm_dt: str | None = None


class DartExecutive(_Base):
    """One row of ``exctvSttus.json`` (임원 현황). ``sexdstn`` and
    ``main_career`` are dropped by the mapper (decision D)."""

    nm: str | None = None
    birth_ym: str | None = None
    ofcps: str | None = None
    rgist_exctv_at: str | None = None
    fte_at: str | None = None
    chrg_job: str | None = None
    mxmm_shrholdr_relate: str | None = None
    hffc_pd: str | None = None
    tenure_end_on: str | None = None
    stlm_dt: str | None = None


class DartMajorHolder(_Base):
    """One row of ``majorstock.json`` (5% holding reports)."""

    rcept_no: str | None = None
    rcept_dt: str | None = None
    report_tp: str | None = None
    repror: str | None = None
    stkqy: str | None = None
    stkqy_irds: str | None = None
    stkrt: str | None = None
    stkrt_irds: str | None = None
    ctr_stkqy: str | None = None
    ctr_stkrt: str | None = None
    report_resn: str | None = None


class DartKoreaBundle(_Base):
    """Top-level shape returned by ``DartKoreaAdapter.fetch``."""

    source_id: str
    hit_id: str
    kr_number: str
    corp_code: str
    legal_name: str = ""
    company: DartCompany
    profile_read: bool
    report: DartReport | None = None
    reports_read: bool
    shareholders: list[DartShareholder] = []
    shareholders_read: bool
    executives: list[DartExecutive] = []
    executives_read: bool
    major_holders: list[DartMajorHolder] = []
    major_holders_read: bool
    link: str
    is_stub: bool
