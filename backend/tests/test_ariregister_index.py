"""Phase 330 — the Estonian e-Business Register, read from a daily index of
RIK's open-data files, with the printable page as the fallback.

Every index in this file is produced by the **real builder**
(``scripts/build_ariregister_index.py``) from zips in RIK's exact published
shape — the semicolon CSV and three pretty-printed JSON arrays — and read
back through the **real adapter** (the ONRC / JAR rule: a hand-written index
would encode what the adapter expects rather than what the builder writes).

The rows are modelled on the 13 Oct 2026 files: ``DD.MM.YYYY`` dates, ISO
alpha-3 country codes with the IOC-style strays the files carry (``DEN``,
``GER``), the pseudonymous ``isikukood_hash`` on natural persons and no
personal identification code anywhere.
"""

from __future__ import annotations

import importlib.util
import io
import json
import os
import sqlite3
import zipfile
from pathlib import Path
from typing import Any

import pytest
import respx
from httpx import Response

from opencheck import degradation, provenance
from opencheck.bods import validator
from opencheck.bods.mapper import map_ariregister
from opencheck.config import get_settings
from opencheck.findings import MAX_FINDING_CHARS, finding_ariregister
from opencheck.register_hops import hop_for, hop_schemes
from opencheck.routers.lookup import _LookupCtx, _build_derived, _build_result_hit, _dispatch
from opencheck.sources import REGISTRY, ariregister
from opencheck.sources.base import SearchKind
from opencheck.sources.schemas import SourceSchemaError, validate_raw
from opencheck.sources.schemas.ariregister import AriregisterBundle

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_ariregister_index.py"


def _load_builder():
    spec = importlib.util.spec_from_file_location("_build_ariregister_index", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = _load_builder()

CUT = "2026-10-12"  # a day in the past: the conftest guard reads a cut of today as a missing one
AGENT = "16752073"    # 007 Agent & Partners OÜ — one person on all three lists
EESTI = "10421629"    # Eesti Energia AS — no shareholders, board-member BOs
HOLD = "12345678"     # a holding OÜ in liquidation, with foreign parties
STATE = "70000000"    # a government agency → stateBody
HAUDE = "16765745"    # Haudemaja OÜ — the holding's corporate shareholder, with a person above it
ABSENT = "99999999"   # in no file
H_KALEV = "92a3b4c5-d6e7-58f9-0123-456789012345"

H_VERONIKA = "77a468f2-248a-5e41-8356-c975af9613a5"
H_ANDRUS = "0f2d0a8e-6a4c-5f7b-9a2d-3b1c4d5e6f70"
H_MARLEN = "1a2b3c4d-5e6f-5071-8293-a4b5c6d7e8f9"
H_LARS = "2b3c4d5e-6f70-5182-93a4-b5c6d7e8f901"
H_JANEK = "3c4d5e6f-7081-5293-a4b5-c6d7e8f90123"


def _csv_rows() -> list[str]:
    def row(name, code, form, sub, vat, st, st_text, first, addr, ads):
        return ";".join([
            name, code, form, sub, vat, st, st_text, first, "", addr, "0596",
            "Pirita linnaosa, Tallinn, Harju maakond", "11911", "2363082", "", ads,
            f"https://ariregister.rik.ee/est/company/{code}",
        ])
    return [
        row("007 Agent & Partners OÜ", AGENT, "Osaühing", "", "", "R", "Registrisse kantud",
            "05.06.2023", "Regati pst 12", "Harju maakond, Tallinn, Pirita linnaosa, Regati pst 12"),
        row("Eesti Energia AS", EESTI, "Aktsiaselts", "", "EE100366327", "R", "Registrisse kantud",
            "31.03.1998", "Lelle tn 22", "Harju maakond, Tallinn, Kesklinna linnaosa, Lelle tn 22"),
        row('"Holding ""Põhjala"" OÜ"', HOLD, "Osaühing", "", "", "L", "Likvideerimisel",
            "02.01.2015", "Narva mnt 5", "Harju maakond, Tallinn, Kesklinna linnaosa, Narva mnt 5"),
        row("Maa-amet", STATE, "Täidesaatva riigivõimu asutus või riigi muu institutsioon",
            "Ministeeriumi hallatav riigiasutus", "", "R", "Registrisse kantud",
            "01.01.1990", "Mustamäe tee 51", "Harju maakond, Tallinn, Mustamäe linnaosa, Mustamäe tee 51"),
        row("Haudemaja OÜ", HAUDE, "Osaühing", "", "", "R", "Registrisse kantud",
            "10.03.2010", "Tartu mnt 1", "Harju maakond, Tallinn, Kesklinna linnaosa, Tartu mnt 1"),
    ]


def _officer(**kw: Any) -> dict[str, Any]:
    base = {
        "kirje_id": 1, "kaardi_piirkond": 5, "kaardi_nr": 1, "kaardi_tyyp": "R", "kande_nr": 1,
        "isiku_tyyp": "F", "isiku_roll": "JUHL", "isiku_roll_tekstina": "Juhatuse liige",
        "eesnimi": None, "nimi_arinimi": None, "isikukood_hash": None,
        "isikukood_registrikood": None, "valis_kood": None, "valis_kood_riik": None,
        "valis_kood_riik_tekstina": None, "synniaeg": None, "osamaks": None,
        "osamaksu_valuuta": None, "osamaksu_valuuta_tekstina": None,
        "volituste_loppemise_kpv": "", "aadress_riik": None, "aadress_riik_tekstina": None,
        "aadress_ehak": None, "aadress_ehak_tekstina": "", "aadress_tanav_maja_korter": None,
        "aadress_postiindeks": None, "algus_kpv": "05.06.2023", "lopp_kpv": None,
        "email": "someone@example.ee",
    }
    base.update(kw)
    return base


def _shareholder(**kw: Any) -> dict[str, Any]:
    base = {
        "kirje_id": 1, "isiku_tyyp": "F", "isiku_roll": "OSAN", "isiku_roll_tekstina": "Osanik",
        "eesnimi": None, "nimi_arinimi": None, "isikukood_registrikood": None,
        "valis_kood": None, "valis_kood_riik_tekstina": None, "valis_kood_riik": None,
        "synniaeg": None, "aadress_riik": None, "aadress_riik_tekstina": None,
        "aadress_ehak": None, "aadress_ehak_tekstina": "", "aadress_tanav_maja_korter": None,
        "osaluse_protsent": "100.00", "osaluse_suurus": "0.01", "osaluse_valuuta": "EUR",
        "osamaksu_valuuta_tekstina": "euro", "osaluse_omandiliik": "L",
        "osaluse_omandiliik_tekstina": "Ainuomand", "osaluse_murdosa_lugeja": None,
        "osaluse_murdosa_nimetaja": None, "volituste_loppemise_kpv": None,
        "kontrolli_allikas": None, "kontrolli_allikas_tekstina": None, "kontrolli_allika_kpv": None,
        "algus_kpv": "02.09.2023", "lopp_kpv": None, "grupp": 265816, "isikukood_hash": None,
    }
    base.update(kw)
    return base


def _bo(**kw: Any) -> dict[str, Any]:
    base = {
        "kirje_id": 9001580186, "algus_kpv": "05.06.2023", "lopp_kpv": None,
        "eesnimi": None, "nimi": None, "isikukood": None, "valis_kood": None,
        "valis_kood_riik": None, "valis_kood_riik_tekstina": None, "synniaeg": None,
        "aadress_riik": "EST", "aadress_riik_tekstina": "Eesti",
        "kontrolli_teostamise_viis": "O", "kontrolli_teostamise_viis_tekstina": "otsene osalus",
        "lahknevusteade_esitatud": None, "isikukood_hash": None,
    }
    base.update(kw)
    return base


def _officers() -> list[dict[str, Any]]:
    return [
        {"ariregistri_kood": int(AGENT), "nimi": "007 Agent & Partners OÜ",
         "kaardile_kantud_isikud": [
             _officer(kirje_id=10687760, eesnimi="Veronika", nimi_arinimi="Oškina", isikukood_hash=H_VERONIKA),
         ],
         "esindusoiguse_normaalregulatsioonid": [], "esindusoiguse_eritingimused": [], "hooneyhistu_liikmed": []},
        {"ariregistri_kood": int(EESTI), "nimi": "Eesti Energia AS",
         "kaardile_kantud_isikud": [
             _officer(kirje_id=1, eesnimi="Andrus", nimi_arinimi="Durejko", isikukood_hash=H_ANDRUS, algus_kpv="03.04.2023"),
             _officer(kirje_id=2, eesnimi="Marlen", nimi_arinimi="Tamm", isikukood_hash=H_MARLEN, algus_kpv="03.04.2023"),
             _officer(kirje_id=3, isiku_tyyp="J", isiku_roll="ARP", isiku_roll_tekstina="Aktsiaraamatu pidaja",
                      nimi_arinimi="Nasdaq CSD SE Eesti filiaal", isikukood_registrikood="14306553"),
         ]},
        {"ariregistri_kood": int(HOLD), "nimi": 'Holding "Põhjala" OÜ',
         "kaardile_kantud_isikud": [
             _officer(kirje_id=11, isiku_roll="PROK", isiku_roll_tekstina="Prokurist",
                      eesnimi="Lars", nimi_arinimi="Jensen", isikukood_hash=H_LARS,
                      valis_kood_riik="DEN", valis_kood_riik_tekstina="Taani", synniaeg="20.01.1987"),
             _officer(kirje_id=12, isiku_roll="KISIK", isiku_roll_tekstina="Kontaktisik",
                      eesnimi="Kadri", nimi_arinimi="Kask", isikukood_hash="4d5e6f70-8192-53a4-b5c6-d7e8f9012345"),
             _officer(kirje_id=13, isiku_roll="LIKV", isiku_roll_tekstina="Likvideerija",
                      eesnimi="Janek", nimi_arinimi="Stalmeister", isikukood_hash=H_JANEK, algus_kpv="01.03.2026"),
         ]},
        {"ariregistri_kood": int(STATE), "nimi": "Maa-amet",
         "kaardile_kantud_isikud": [
             _officer(kirje_id=21, isiku_tyyp="J", isiku_roll="KOAS", isiku_roll_tekstina="Kõrgemalseisev asutus",
                      nimi_arinimi="Regionaal- ja Põllumajandusministeerium", isikukood_registrikood="70000734"),
         ]},
        {"ariregistri_kood": int(HAUDE), "nimi": "Haudemaja OÜ",
         "kaardile_kantud_isikud": [
             _officer(kirje_id=71, eesnimi="Kalev", nimi_arinimi="Kask", isikukood_hash=H_KALEV, algus_kpv="10.03.2010"),
         ]},
    ]


def _shareholders() -> list[dict[str, Any]]:
    return [
        {"ariregistri_kood": int(AGENT), "nimi": "007 Agent & Partners OÜ",
         "osanikud": [
             _shareholder(kirje_id=11555326, eesnimi="Veronika", nimi_arinimi="Oškina", isikukood_hash=H_VERONIKA),
         ],
         "osapandid_tingimuslikud_voorandamised": []},
        {"ariregistri_kood": int(EESTI), "nimi": "Eesti Energia AS", "osanikud": []},
        {"ariregistri_kood": int(HOLD), "nimi": 'Holding "Põhjala" OÜ',
         "osanikud": [
             _shareholder(kirje_id=31, isiku_tyyp="J", nimi_arinimi="Haudemaja OÜ",
                          isikukood_registrikood="16765745", osaluse_protsent="33.33", osaluse_suurus="834.00"),
             _shareholder(kirje_id=32, isiku_tyyp="J", nimi_arinimi="SIA Ziemeļi",
                          valis_kood="40003242879", valis_kood_riik="LVA", valis_kood_riik_tekstina="Läti",
                          osaluse_protsent="33.33", osaluse_suurus="834.00",
                          kontrolli_allikas="V1", kontrolli_allikas_tekstina="NASDAQ andmed",
                          kontrolli_allika_kpv="14.01.2025"),
             _shareholder(kirje_id=33, eesnimi="Lars", nimi_arinimi="Jensen", isikukood_hash=H_LARS,
                          valis_kood_riik="DEN", valis_kood_riik_tekstina="Taani", synniaeg="20.01.1987",
                          osaluse_protsent="16.67", osaluse_suurus="417.00",
                          osaluse_omandiliik="Y", osaluse_omandiliik_tekstina="Ühisomand"),
             _shareholder(kirje_id=34, eesnimi="Mari", nimi_arinimi="Mets",
                          isikukood_hash="5e6f7081-92a3-54b5-c6d7-e8f901234567",
                          osaluse_protsent="16.67", osaluse_suurus="417.00",
                          algus_kpv="02.01.2015", lopp_kpv="01.02.2024"),
         ]},
        {"ariregistri_kood": int(STATE), "nimi": "Maa-amet", "osanikud": []},
        {"ariregistri_kood": int(HAUDE), "nimi": "Haudemaja OÜ",
         "osanikud": [
             _shareholder(kirje_id=61, eesnimi="Kalev", nimi_arinimi="Kask", isikukood_hash=H_KALEV,
                          osaluse_protsent="60.00", osaluse_suurus="1500.00", algus_kpv="10.03.2010"),
             _shareholder(kirje_id=62, isiku_tyyp="J", nimi_arinimi='Holding "Põhjala" OÜ',
                          isikukood_registrikood=HOLD, osaluse_protsent="40.00", osaluse_suurus="1000.00",
                          algus_kpv="10.03.2010"),
         ]},
    ]


def _bos() -> list[dict[str, Any]]:
    return [
        {"ariregistri_kood": int(AGENT), "nimi": "007 Agent & Partners OÜ",
         "kasusaajad": [_bo(eesnimi="Veronika", nimi="Oškina", isikukood_hash=H_VERONIKA)]},
        {"ariregistri_kood": int(EESTI), "nimi": "Eesti Energia AS",
         "kasusaajad": [
             _bo(kirje_id=1, eesnimi="Andrus", nimi="Durejko", isikukood_hash=H_ANDRUS,
                 kontrolli_teostamise_viis="J",
                 kontrolli_teostamise_viis_tekstina="kõrgema juhtorgani liige. s.o juhatuse liige või nõukogu liige",
                 algus_kpv="03.09.2026"),
             _bo(kirje_id=2, eesnimi="Marlen", nimi="Tamm", isikukood_hash=H_MARLEN,
                 kontrolli_teostamise_viis="J",
                 kontrolli_teostamise_viis_tekstina="kõrgema juhtorgani liige. s.o juhatuse liige või nõukogu liige",
                 algus_kpv="03.09.2026", lahknevusteade_esitatud=True),
         ]},
        {"ariregistri_kood": int(HOLD), "nimi": 'Holding "Põhjala" OÜ',
         "kasusaajad": [
             _bo(kirje_id=3, eesnimi="Lars", nimi="Jensen", isikukood_hash=H_LARS,
                 valis_kood_riik="DEN", valis_kood_riik_tekstina="Taani", synniaeg="20.01.1987",
                 aadress_riik="GER", aadress_riik_tekstina="Saksamaa",
                 kontrolli_teostamise_viis="K", kontrolli_teostamise_viis_tekstina="kaudne osalus"),
             _bo(kirje_id=4, eesnimi="Nurlan", nimi="Abenov", isikukood_hash=None,
                 valis_kood_riik="XXK", valis_kood_riik_tekstina="Kosovo", synniaeg="11.09.1989",
                 aadress_riik="XXK", aadress_riik_tekstina="Kosovo",
                 kontrolli_teostamise_viis="F",
                 kontrolli_teostamise_viis_tekstina="Muul viisil kontroll või mõju (lepingulised, perekondlikud vm suhted)"),
         ]},
        {"ariregistri_kood": int(STATE), "nimi": "Maa-amet", "kasusaajad": []},
    ]


def _off_card(**kw: Any) -> dict[str, Any]:
    base = _shareholder(isiku_roll="A", isiku_roll_tekstina="Asutaja", osaluse_protsent=None,
                        osaluse_suurus="2500.00", grupp=None, kontrolli_allikas="ELK",
                        kontrolli_allikas_tekstina="Elektrooniliselt esitatud (kande)avaldus",
                        kontrolli_allika_kpv="05.06.2023", aadress_riik="EST", aadress_riik_tekstina="Eesti")
    base.update(kw)
    return base


H_TIINA = "6f708192-a3b4-55c6-d7e8-f90123456789"
H_RAIVO = "708192a3-b4c5-56d7-e8f9-012345678901"


def _off_cards() -> list[dict[str, Any]]:
    return [
        # The founder is still the sole shareholder: said on her holding, not a second record.
        {"ariregistri_kood": int(AGENT), "nimi": "007 Agent & Partners OÜ",
         "kaardivalised_isikud": [
             _off_card(kirje_id=9001580184, eesnimi="Veronika", nimi_arinimi="Oškina", isikukood_hash=H_VERONIKA,
                       osaluse_suurus="0.01", algus_kpv="03.06.2023"),
         ]},
        # A supervisory board, an audit firm (not an interest) and the register's own
        # record of the sole shareholder of an AS (role S) with a Nasdaq CSD source.
        {"ariregistri_kood": int(EESTI), "nimi": "Eesti Energia AS",
         "kaardivalised_isikud": [
             _off_card(kirje_id=41, isiku_roll="E", isiku_roll_tekstina="Nõukogu esimees",
                       eesnimi="Tiina", nimi_arinimi="Laane", isikukood_hash=H_TIINA, osaluse_suurus=None,
                       kontrolli_allikas=None, kontrolli_allikas_tekstina=None, kontrolli_allika_kpv=None,
                       algus_kpv="12.05.2024"),
             _off_card(kirje_id=42, isiku_roll="N", isiku_roll_tekstina="Nõukogu liige",
                       eesnimi="Raivo", nimi_arinimi="Vare", isikukood_hash=H_RAIVO, osaluse_suurus=None,
                       kontrolli_allikas=None, kontrolli_allikas_tekstina=None, kontrolli_allika_kpv=None,
                       algus_kpv="12.05.2024"),
             _off_card(kirje_id=43, isiku_tyyp="J", isiku_roll="D", isiku_roll_tekstina="Audiitorettevõtja",
                       nimi_arinimi="Aktsiaselts PricewaterhouseCoopers", isikukood_registrikood="10142876",
                       osaluse_suurus=None, kontrolli_allikas=None, kontrolli_allikas_tekstina=None,
                       kontrolli_allika_kpv=None, algus_kpv="14.11.2002"),
             _off_card(kirje_id=44, isiku_tyyp="J", isiku_roll="S", isiku_roll_tekstina="Aktsionär",
                       nimi_arinimi="Eesti Vabariik", isikukood_registrikood="70000001",
                       osaluse_protsent="100.00", osaluse_suurus="846645750.00",
                       kontrolli_allikas="V1", kontrolli_allikas_tekstina="NASDAQ andmed",
                       kontrolli_allika_kpv="09.07.2025", algus_kpv="31.03.1998"),
         ]},
        # A founder who no longer holds, with a contribution; one who made none.
        {"ariregistri_kood": int(HOLD), "nimi": 'Holding "Põhjala" OÜ',
         "kaardivalised_isikud": [
             _off_card(kirje_id=51, eesnimi="Mari", nimi_arinimi="Mets",
                       isikukood_hash="5e6f7081-92a3-54b5-c6d7-e8f901234567",
                       osaluse_suurus="2500.00", algus_kpv="02.01.2015"),
             _off_card(kirje_id=52, isiku_roll="B", isiku_roll_tekstina="Asutaja (sissemakseta)",
                       eesnimi="Peeter", nimi_arinimi="Paan",
                       isikukood_hash="8192a3b4-c5d6-57e8-f901-234567890123",
                       osaluse_suurus=None, kontrolli_allikas=None, kontrolli_allikas_tekstina=None,
                       kontrolli_allika_kpv=None, algus_kpv="02.01.2015"),
         ]},
        {"ariregistri_kood": int(STATE), "nimi": "Maa-amet", "kaardivalised_isikud": []},
    ]


def _cards() -> list[dict[str, Any]]:
    """The registry cards, in the file's shape: Eesti Energia's first and
    last entries as read on 16 Oct 2026, and a company with two cards."""
    def entry(nr: int, day: str, kind: str, text: str | None) -> dict[str, Any]:
        return {"kande_nr": nr, "kpv": day, "kandeliik": kind, "kandeliik_tekstina": text}
    return [
        {"ariregistri_kood": int(AGENT), "nimi": "007 Agent & Partners OÜ", "registrikaardid": [
            {"kaardi_piirkond": 5, "kaardi_nr": 1, "kaardi_tyyp": "R", "kanded": [
                entry(1, "05.06.2023", "2", "Esmakanne"),
                entry(2, "02.09.2023", "12", "Muutmiskanne"),
                entry(3, "15.07.2024", "33", "Muutmiskanne TsMS § 595¹ lg 1 alusel"),
            ]},
        ]},
        {"ariregistri_kood": int(EESTI), "nimi": "Eesti Energia AS", "registrikaardid": [
            {"kaardi_piirkond": 1, "kaardi_nr": 1, "kaardi_tyyp": "R", "kanded": [
                entry(1, "31.03.1998", "1", "Asutamiskanne"),
                entry(2, "25.06.1998", "10", "Mitteavaldatav muutmiskanne"),
                entry(56, "15.07.2026", "33", "Muutmiskanne TsMS § 595¹ lg 1 alusel"),
                entry(57, "02.10.2026", "12", "Muutmiskanne"),
            ]},
        ]},
        {"ariregistri_kood": int(HOLD), "nimi": 'Holding "Põhjala" OÜ', "registrikaardid": [
            {"kaardi_piirkond": 1, "kaardi_nr": 1, "kaardi_tyyp": "R", "kanded": [
                entry(1, "02.01.2015", "1", "Asutamiskanne"),
                entry(2, "01.03.2026", "14", "Ärinime muutmise kanne"),
            ]},
            {"kaardi_piirkond": 1, "kaardi_nr": 2, "kaardi_tyyp": "K", "kanded": [
                entry(1, "05.05.2025", "21", "Kommertspandi avamiskanne"),
            ]},
        ]},
        {"ariregistri_kood": int(STATE), "nimi": "Maa-amet", "registrikaardid": [
            {"kaardi_piirkond": 1, "kaardi_nr": 1, "kaardi_tyyp": "R", "kanded": [
                entry(1, "07.01.2018", "9999", None),
            ]},
        ]},
    ]


def _ruling(nr: str, day: str, kind: str, text: str, status: str = "J", deadline: str | None = None) -> dict[str, Any]:
    return {
        "maaruse_nr": nr, "maaruse_kpv": day, "kande_kpv": None, "lisatahtaeg": deadline,
        "maaruse_liik": kind, "maaruse_liik_tekstina": text, "maaruse_olek": status,
        "maaruse_olek_tekstina": {"J": "Jõustunud", "L": "Puudused kõrvaldatud"}.get(status),
        "kandeliik": "12", "kandeliik_tekstina": "Muutmiskanne", "joustumise_kpv": day,
        "joust_olek": status, "joust_olek_tekstina": None,
    }


def _rulings() -> list[dict[str, Any]]:
    """The rulings, in the file's shape (16 Oct 2026). 007 Agent's are its
    first and last as published — entry rulings and a remedied deficiency
    ruling, none kept. Eesti Energia's annual-report fine is answered by its
    2025 report. Haudemaja's deletion warning stands; its fine warning was
    remedied. The agency's net-assets warning is not about a report."""
    return [
        {"ariregistri_kood": int(AGENT), "nimi": "007 Agent & Partners OÜ", "maarused": [
            _ruling("Ä 50229792 / 3", "15.07.2024", "K1", "Kanne"),
            _ruling("Ä 50229792 / M1", "01.06.2023", "M1", "Puuduste kõrvaldamise määrus", "L", "16.06.2023"),
        ]},
        {"ariregistri_kood": int(EESTI), "nimi": "Eesti Energia AS", "maarused": [
            _ruling("Ä 1 / H", "01.09.2025", "H", "Aruandetrahvimäärus"),
        ]},
        {"ariregistri_kood": int(HAUDE), "nimi": "Haudemaja OÜ", "maarused": [
            _ruling("Ä 2 / X2", "12.03.2026", "X2",
                    "Hoiatusmäärus registrist kustutamiseks: majandusaasta aruanne esitamata",
                    deadline="12.05.2026"),
            _ruling("Ä 2 / H", "04.06.2024", "H", "Aruandetrahvimäärus"),
            _ruling("Ä 2 / HM", "02.02.2024", "HM", "Trahvihoiatusmäärus: majandusaasta aruanne esitamata", "L"),
        ]},
        {"ariregistri_kood": int(STATE), "nimi": "Maa-amet", "maarused": [
            _ruling("Ä 3 / Y3", "05.01.2026", "Y3", "Hoiatusmäärus sundlõpetamiseks: ebapiisav netovara"),
        ]},
    ]


def _pledgee(kind: str, name: str, code: str, country: str | None = None, ended: str | None = None) -> dict[str, Any]:
    return {
        "kirje_id": 1, "kaardi_tyyp": "K", "kande_nr": 1, "pidaja_isikuliik": kind,
        "pidaja_nimi": name, "pidaja_kood": code, "pidaja_riik": country,
        "pidaja_riik_tekstina": {"EST": "Eesti", "FIN": "Soome"}.get(country or ""),
        "pidaja_asukoht_ehak": None, "pidaja_asukoht_ehak_tekstina": "", "pidaja_murdosa": None,
        "algus_kpv": "19.06.2018", "lopp_kpv": ended,
    }


def _pledges() -> list[dict[str, Any]]:
    """Commercial pledges in the file's shape: the holding's first-rank
    pledge to AS LHV Pank (as 180 OÜ's reads on 16 Oct 2026), a second-rank
    pledge to a private individual filed with a personal code, and one that
    ended."""
    def pledge(no: int, rank: int, amount: str, currency: str, since: str, holders: list, ended: str | None = None):
        return {
            "kirje_id": no, "pandi_id": no, "pandi_number": no, "pandi_olek": "R", "olek_tekstina": None,
            "pandi_jarjekoht": rank, "pandi_jarjekoht_tekstina": "esimene", "pandi_summa": amount,
            "pandi_valuuta": currency, "pandi_valuuta_tekstina": "euro", "algus_kpv": since, "lopp_kpv": ended,
            "jarjekohad": [], "pandisummad": [], "pandipidajad": holders,
            "markused": [{"tyyp": "KMARK", "sisu": "1.kandes on muudetud pandipidaja andmeid"}],
        }
    return [
        {"ariregistri_kood": int(HOLD), "nimi": 'Holding "Põhjala" OÜ', "kommertspandiandmed": [
            pledge(1, 1, "520000.00", "EUR", "19.06.2018", [_pledgee("J", " AS LHV Pank", "10539549")]),
            pledge(2, 2, "100000.00", "EEK", "02.05.2009", [_pledgee("F", "Juku Tamm", "34701014283", "EST")]),
            pledge(3, 3, "5000.00", "EUR", "01.01.2015", [_pledgee("J", "Swedbank AS", "10060701")], ended="01.01.2020"),
        ]},
    ]


TOOTSI_TEXT = (
    "Eesti Energia Aktsiaselts (jagunev ühing) jaguneb eraldumise teel, mille käigus asutati "
    "Tootsi Windpark OÜ (jagunemise käigus asutatud ühing, registrikood 16053613, aadress Harju "
    "maakond, Tallinn, Kesklinna linnaosa, Lelle tn 22, 11318). Jagunemiskava on notariaalselt "
    "tõestatud 23.07.2020.a. Jagunemisotsus on vastu võetud 05.08.2020.a. Jagunemine loetakse "
    "toimunuks käesoleva jagunemiskande tegemisega."
)


def _yld(**kw: Any) -> dict[str, Any]:
    """One ``yldandmed`` object with the scalars every entity carries."""
    base: dict[str, Any] = {
        "esmaregistreerimise_kpv": "01.01.2020", "staatus": "R", "staatus_tekstina": "Registrisse kantud",
        "oiguslik_vorm": "OÜ", "esitab_kasusaajad": True, "lahknevusteade_puudumisest": False,
        "evks_registreeritud": None,
    }
    base.update(kw)
    return base


def _general() -> list[dict[str, Any]]:
    """The general data, in the file's shape: Eesti Energia's capital, main
    activity, last annual report, its 2020 division (naming Tootsi Windpark
    OÜ, a code the fixture index lacks) and a predecessor reference, as
    published on 16 Oct 2026; a holding in liquidation with a dissolution
    decision; a state agency not obliged to file
    beneficial owners."""
    def row(code: str, name: str, y: dict[str, Any]) -> dict[str, Any]:
        return {"ariregistri_kood": int(code), "nimi": name, "yldandmed": y}
    return [
        row(AGENT, "007 Agent & Partners OÜ", _yld(
            kapitalid=[{"kande_nr": 1, "kapitali_suurus": "2500.00", "kapitali_valuuta": "EUR",
                        "algus_kpv": "05.06.2023", "lopp_kpv": None}],
        )),
        row(EESTI, "Eesti Energia AS", _yld(
            oiguslik_vorm="AS",
            kapitalid=[{"kande_nr": 51, "kapitali_suurus": "846645750.00", "kapitali_valuuta": "EUR",
                        "algus_kpv": "09.07.2025", "lopp_kpv": None}],
            teatatud_tegevusalad=[
                {"emtak_kood": "35151", "emtak_tekstina": "Elektrienergia müük", "nace_kood": "35.15",
                 "on_pohitegevusala": True, "algus_kpv": "08.05.2025", "lopp_kpv": None},
                {"emtak_kood": "35231", "emtak_tekstina": "Gaasimüük magistraalvõrkude kaudu",
                 "nace_kood": "35.23", "on_pohitegevusala": False, "algus_kpv": "08.05.2025", "lopp_kpv": None},
            ],
            info_majandusaasta_aruannetest=[
                {"majandusaasta_perioodi_algus_kpv": "01.01.2024", "majandusaasta_perioodi_lopp_kpv": "31.12.2024",
                 "tootajate_arv": "780"},
                {"majandusaasta_perioodi_algus_kpv": "01.01.2025", "majandusaasta_perioodi_lopp_kpv": "31.12.2025",
                 "tootajate_arv": "793"},
            ],
            oigusjargsused=[{"kande_nr": 41, "tyyp": "JAGUN", "tyyp_tekstina": "Jagunemine tekstina",
                             "sisu": TOOTSI_TEXT, "algus_kpv": "15.09.2020", "lopp_kpv": None}],
            markused_kaardil=[
                {"kande_nr": 2, "tyyp": "EVREG", "tyyp_tekstina": "Viide eelkäijale",
                 "sisu": "Eesti Energoremont (reg. nr. 01005614) baasil.", "algus_kpv": "25.06.1998", "lopp_kpv": None},
                {"kande_nr": 31, "tyyp": "MARK", "tyyp_tekstina": "Märkus",
                 "sisu": "Aadressiandmed on muudetud.", "algus_kpv": "14.09.2016", "lopp_kpv": None},
            ],
            sidevahendid=[{"liik": "EMAIL", "sisu": "info@enefit.com", "lopp_kpv": None}],
        )),
        row(HOLD, 'Holding "Põhjala" OÜ', _yld(
            seisundi_muutused=[{"kande_nr": 3, "tyyp": "LOPET", "tyyp_tekstina": "Lõpetamine tekstina",
                                "sisu": "Osanikud otsustasid 30.04.2026 lõpetada äriühingu tegevuse.",
                                "algus_kpv": "01.03.2026", "lopp_kpv": None}],
        )),
        row(STATE, "Maa-amet", _yld(oiguslik_vorm="RAS", esitab_kasusaajad=False, lahknevusteade_puudumisest=None)),
        row(HAUDE, "Haudemaja OÜ", _yld()),
    ]


def _zip(path: Path, member: str, text: str) -> Path:
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(zipfile.ZipInfo(member, (2026, 10, 13, 12, 24, 0)), text.encode("utf-8"))
    return path


def _write_files(
    src: Path,
    *,
    csv_rows: list[str] | None = None,
    officers: list[dict[str, Any]] | None = None,
    shareholders: list[dict[str, Any]] | None = None,
    bos: list[dict[str, Any]] | None = None,
    off_cards: list[dict[str, Any]] | None = None,
    cards: list[dict[str, Any]] | None = None,
    general: list[dict[str, Any]] | None = None,
    rulings: list[dict[str, Any]] | None = None,
    pledges: list[dict[str, Any]] | None = None,
    header: list[str] | None = None,
) -> None:
    src.mkdir(parents=True, exist_ok=True)
    head = ";".join(header or builder.CSV_HEADER)
    rows = _csv_rows() if csv_rows is None else csv_rows
    _zip(src / builder.CSV_FILE, "ettevotja_rekvisiidid__lihtandmed.csv",
         "﻿" + "\n".join([head, *rows]) + "\n")
    for name, data in (
        (builder.OFFICERS_FILE, _officers() if officers is None else officers),
        (builder.SHAREHOLDERS_FILE, _shareholders() if shareholders is None else shareholders),
        (builder.BO_FILE, _bos() if bos is None else bos),
        (builder.OFF_CARD_FILE, _off_cards() if off_cards is None else off_cards),
        (builder.CARDS_FILE, _cards() if cards is None else cards),
        (builder.GENERAL_FILE, _general() if general is None else general),
        (builder.RULINGS_FILE, _rulings() if rulings is None else rulings),
        (builder.PLEDGES_FILE, _pledges() if pledges is None else pledges),
    ):
        _zip(src / name, name[:-4], json.dumps(data, ensure_ascii=False, indent=4))


def _build(tmp_path: Path, *, cut: str | None = CUT, name: str = "ariregister.sqlite", **files: Any) -> tuple[Path, dict]:
    src = tmp_path / "src"
    _write_files(src, **files)
    out = tmp_path / "data" / name
    meta = builder.build_index(src, out, cut_override=cut, min_rows={})
    return out, meta


def _point_at(path: Path | None) -> None:
    if path is None:
        os.environ.pop("ARIREGISTER_DB_FILE", None)
    else:
        os.environ["ARIREGISTER_DB_FILE"] = str(path)
    get_settings.cache_clear()
    ariregister.reset_connection()


@pytest.fixture()
def index(tmp_path: Path):
    """An index of the fixture rows, built by the real builder."""
    out, meta = _build(tmp_path)
    _point_at(out)
    yield out, meta
    _point_at(None)


@pytest.fixture()
def no_index(tmp_path: Path):
    """No index anywhere — neither named nor at the data-root default."""
    previous_root = os.environ.get("OPENCHECK_DATA_ROOT")
    os.environ["OPENCHECK_DATA_ROOT"] = str(tmp_path / "empty-data-root")
    _point_at(None)
    yield
    if previous_root is None:
        os.environ.pop("OPENCHECK_DATA_ROOT", None)
    else:
        os.environ["OPENCHECK_DATA_ROOT"] = previous_root
    _point_at(None)


async def _fetch(code: str, legal_name: str = "") -> dict[str, Any]:
    return await REGISTRY["ariregister"].fetch(code, legal_name=legal_name)


def _page_url(code: str) -> str:
    return ariregister._PRINT_URL.format(reg_code=code)


_PAGE = """<html><body>
<h2>Uus Firma OÜ (99999999)</h2>
<div class="row"><div class="col-md-4 text-muted">Name</div><div class="col font-weight-bold">Uus Firma OÜ</div></div>
<div class="row"><div class="col-md-4 text-muted">Status</div><div class="col font-weight-bold">Entered into the register</div></div>
<div class="row"><div class="col-md-4 text-muted">Legal form</div><div class="col font-weight-bold">Private limited company</div></div>
<div class="row"><div class="col-md-4 text-muted">Registered</div><div class="col font-weight-bold">01.10.2026</div></div>
<table><tr><th>Name</th><th>Personal identification code</th><th>Role</th><th>Start - end</th></tr>
<tr><td>Jaan Tamm</td><td>38001010011</td><td>Management board member</td><td>01.10.2026</td></tr></table>
<table><tr><th>Participation</th><th>Contribution</th><th>Name</th><th>Code</th><th>Start - End</th></tr>
<tr><td>100.00%</td><td>2500.00 EUR</td><td>Jaan Tamm</td><td>38001010011</td><td>01.10.2026</td></tr></table>
<table><tr><th>Name</th><th>Personal identification code / date of birth</th><th>Manner of exercising control</th><th>Start - end</th></tr>
<tr><td>Jaan Tamm</td><td>38001010011</td><td>Direct ownership</td><td>01.10.2026</td></tr></table>
</body></html>"""


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def test_the_builder_and_the_adapter_agree_on_the_schema() -> None:
    assert builder.SCHEMA_VERSION == ariregister.INDEX_SCHEMA_VERSION
    assert builder.TABLE_COLUMNS == ariregister._TABLE_COLUMNS


def test_the_builder_indexes_the_seven_files(index) -> None:
    out, meta = index
    assert meta["cut"] == CUT
    assert meta["entities"] == "5"
    assert meta["officers"] == "9"
    assert meta["shareholders"] == "7"
    assert meta["beneficial_owners"] == "5"
    assert meta["off_card_persons"] == "7"
    assert meta["card_entries"] == "11"
    assert meta["general"] == "5"
    # Haudemaja must file beneficial owners and has none in the fixture.
    assert meta["bo_obliged_none_filed"] == "1"
    assert meta["with_vat_number"] == "1"
    assert provenance.parse_moment(meta["built_at"]) is not None
    assert "T" in meta["built_at"] and meta["built_at"].endswith("+00:00")
    conn = sqlite3.connect(out)
    names = dict(conn.execute("SELECT code, name FROM entity"))
    assert names[HOLD] == 'Holding "Põhjala" OÜ', "doubled quotes are unescaped, never kept"
    vocab = dict(((k, c), t) for k, c, t in conn.execute("SELECT kind, code, text FROM vocab"))
    assert vocab[("control", "K")] == "kaudne osalus"
    assert vocab[("country", "DEN")] == "Taani"
    assert vocab[("status", "L")] == "Likvideerimisel"
    assert vocab[("role", "E")] == "Nõukogu esimees"
    # The hash is stored as 16 bytes, dates as ISO days, the e-mail not at all.
    row = conn.execute("SELECT isikukood_hash, algus_kpv FROM officer WHERE code = ?", (AGENT,)).fetchone()
    assert len(row[0]) == 16 and row[1] == "2023-06-05"
    assert "email" not in {r[1] for r in conn.execute("PRAGMA table_info(officer)")}
    conn.close()


def _refused(tmp_path: Path, match: str, **files: Any) -> None:
    with pytest.raises(builder.BuildRefused, match=match):
        _build(tmp_path, **files)
    assert not (tmp_path / "data" / "ariregister.sqlite").exists(), "a refusal must write nothing"


def test_a_wrong_csv_header_is_refused(tmp_path: Path) -> None:
    _refused(tmp_path, "header", header=["nimi", "kood"])


def test_a_short_file_is_refused(tmp_path: Path) -> None:
    src = tmp_path / "src"
    _write_files(src)
    with pytest.raises(builder.BuildRefused, match="fewer than"):
        builder.build_index(src, tmp_path / "data" / "x.sqlite", cut_override=CUT,
                            min_rows={builder.CSV_FILE: 1000})


def test_a_people_file_naming_an_unknown_company_is_refused(tmp_path: Path) -> None:
    bos = _bos() + [{"ariregistri_kood": 11111111, "nimi": "Ghost OÜ", "kasusaajad": []}]
    _refused(tmp_path, "is not in", bos=bos)


def test_a_code_that_is_not_eight_digits_is_refused(tmp_path: Path) -> None:
    rows = _csv_rows()
    rows[0] = rows[0].replace(AGENT, "1675207")
    _refused(tmp_path, "eight digits", csv_rows=rows)


def test_a_repeated_code_is_refused(tmp_path: Path) -> None:
    rows = _csv_rows()
    _refused(tmp_path, "appears twice", csv_rows=rows + [rows[0]])


def test_files_from_different_days_are_refused(tmp_path: Path) -> None:
    src = tmp_path / "src"
    _write_files(src)
    (src / (builder.CSV_FILE + ".last-modified")).write_text("Sun, 11 Oct 2026 09:25:00 GMT")
    (src / (builder.BO_FILE + ".last-modified")).write_text("Mon, 12 Oct 2026 09:30:19 GMT")
    with pytest.raises(builder.BuildRefused, match="different cut days"):
        builder.build_index(src, tmp_path / "data" / "x.sqlite", min_rows={})


def test_the_cut_is_read_from_the_last_modified_sidecars(tmp_path: Path) -> None:
    src = tmp_path / "src"
    _write_files(src)
    for name in builder.FILES:
        (src / (name + ".last-modified")).write_text("Mon, 12 Oct 2026 09:30:19 GMT")
    meta = builder.build_index(src, tmp_path / "data" / "x.sqlite", min_rows={})
    assert meta["cut"] == "2026-10-12"


def test_a_page_in_place_of_a_zip_is_refused(tmp_path: Path) -> None:
    src = tmp_path / "src"
    _write_files(src)
    (src / builder.OFFICERS_FILE).write_text("Error code: 1015", encoding="utf-8")
    with pytest.raises(builder.BuildRefused, match="not a zip"):
        builder.build_index(src, tmp_path / "data" / "x.sqlite", cut_override=CUT, min_rows={})


def test_a_truncated_json_array_is_refused(tmp_path: Path) -> None:
    text = json.dumps(_bos(), indent=4)[:-40]
    with pytest.raises(builder.BuildRefused, match="truncated|closing bracket"):
        list(builder.stream_json_array(io.StringIO(text)))


def test_the_streamer_reads_every_element_across_chunk_boundaries() -> None:
    data = [{"ariregistri_kood": i, "x": "é" * (i % 7)} for i in range(5000)]
    text = json.dumps(data, indent=4, ensure_ascii=False)
    out = list(builder.stream_json_array(io.StringIO(text), chunk=777))
    assert out == data


def test_the_builder_cli_from_a_directory(tmp_path: Path) -> None:
    src = tmp_path / "src"
    _write_files(src)
    out = tmp_path / "out" / "ariregister.sqlite"
    assert builder.main(["--from-dir", str(src), "--out", str(out), "--cut", CUT, "--min-rows", "0", "--gzip"]) == 0
    assert out.exists() and out.with_name("ariregister.sqlite.gz").exists()


# ---------------------------------------------------------------------------
# The adapter: index first
# ---------------------------------------------------------------------------


async def test_a_company_is_read_from_the_index_as_a_snapshot(index) -> None:
    with provenance.recording() as rec:
        bundle = await _fetch(AGENT)
    assert bundle["tier"] == "index" and bundle["cut"] == CUT and not bundle["is_stub"]
    assert bundle["name"] == "007 Agent & Partners OÜ"
    assert bundle["status"] == "Entered into the register" and bundle["status_text"] == "Registrisse kantud"
    assert bundle["registration_date"] == "2023-06-05"
    assert bundle["legal_form"] == "Osaühing"
    assert [o["isikukood_hash"] for o in bundle["officers"]] == [H_VERONIKA]
    assert bundle["officers"][0]["isiku_roll_tekstina"] == "Juhatuse liige"
    assert bundle["beneficial_owners"][0]["kontrolli_teostamise_viis_tekstina"] == "otsene osalus"
    assert bundle["beneficial_owners"][0]["aadress_riik_tekstina"] == "Eesti"
    resolved = rec.resolve(is_stub=False)
    assert resolved.liveness == "snapshot"
    assert resolved.source_as_of is not None and resolved.source_as_of.date().isoformat() == CUT


async def test_the_index_answers_without_touching_the_network(index) -> None:
    with respx.mock(assert_all_called=False) as mock:
        route = mock.get(_page_url(EESTI)).mock(return_value=Response(500))
        bundle = await _fetch(EESTI)
    assert not route.called
    assert bundle["shareholders"] == [] and len(bundle["officers"]) == 3 and len(bundle["beneficial_owners"]) == 2
    assert bundle["vat_number"] == "EE100366327"


async def test_a_code_the_index_lacks_falls_through_to_the_page(index) -> None:
    with respx.mock() as mock:
        mock.get(_page_url(ABSENT)).mock(return_value=Response(200, text=_PAGE))
        with provenance.recording() as rec:
            bundle = await _fetch(ABSENT, "UUS FIRMA OÜ")
    assert bundle["tier"] == "page" and bundle["index_miss"] is True and bundle["cut"] == CUT
    assert bundle["name"] == "Uus Firma OÜ" and not bundle.get("not_found")
    assert rec.resolve(is_stub=False).liveness == "live"
    assert "read from the page, not the open data" in (finding_ariregister(bundle) or "")


async def test_absent_from_both_is_a_note_card_not_a_degradation(index) -> None:
    with respx.mock() as mock:
        mock.get(_page_url(ABSENT)).mock(
            return_value=Response(302, headers={"location": "https://ariregister.rik.ee/eng?search=1"}),
        )
        mock.get("https://ariregister.rik.ee/eng?search=1").mock(return_value=Response(200, text="<html>"))
        with degradation.recording() as degraded:
            bundle = await _fetch(ABSENT, "GHOST OÜ")
    assert bundle["not_found"] is True and bundle["is_stub"] is False
    assert CUT in bundle["coverage_note"] and "holds no record" in bundle["coverage_note"]
    assert degraded == []


async def test_a_page_failure_on_an_index_miss_is_a_degradation(index) -> None:
    with respx.mock() as mock:
        mock.get(_page_url(ABSENT)).mock(return_value=Response(429, text="Error code: 1015"))
        with degradation.recording() as degraded:
            bundle = await _fetch(ABSENT, "NEW OÜ")
    assert bundle["not_found"] is True
    assert "HTTP 429" in bundle["coverage_note"]
    assert [d.source_id for d in degraded] == ["ariregister"]


async def test_without_an_index_the_page_answers_as_before(no_index) -> None:
    with respx.mock() as mock:
        mock.get(_page_url(ABSENT)).mock(return_value=Response(200, text=_PAGE))
        bundle = await _fetch(ABSENT)
    assert bundle["tier"] == "page" and "index_miss" not in bundle and "cut" not in bundle
    assert bundle["registry_code"] == ABSENT


async def test_without_an_index_a_page_failure_is_a_stub(no_index) -> None:
    with respx.mock() as mock:
        mock.get(_page_url(ABSENT)).mock(return_value=Response(429))
        bundle = await _fetch(ABSENT)
    assert bundle["is_stub"] is True and "not_found" not in bundle


async def test_an_index_of_another_schema_is_ignored(tmp_path: Path) -> None:
    out, _ = _build(tmp_path, name="other.sqlite")
    conn = sqlite3.connect(out)
    conn.execute("UPDATE meta SET value = '99' WHERE key = 'schema_version'")
    conn.commit()
    conn.close()
    _point_at(out)
    try:
        assert ariregister.index_available() is False
    finally:
        _point_at(None)


async def test_a_short_code_is_zero_filled_to_the_register_key(index) -> None:
    with respx.mock() as mock:
        mock.get(_page_url("6752073")).mock(return_value=Response(429))
        bundle = await _fetch("6752073")  # not a real key, so a miss on both tiers
    assert bundle["registry_code"] == "06752073"


# ---------------------------------------------------------------------------
# Personal identification codes never leave the adapter
# ---------------------------------------------------------------------------


async def test_the_page_path_strips_personal_identification_codes(no_index) -> None:
    with respx.mock() as mock:
        mock.get(_page_url(ABSENT)).mock(return_value=Response(200, text=_PAGE))
        bundle = await _fetch(ABSENT)
    assert "38001010011" not in json.dumps(bundle)
    assert bundle["officers"][0]["isiku_tyyp"] == "F"
    assert bundle["shareholders"][0]["isiku_tyyp"] == "F"


def test_the_schema_refuses_a_bundle_carrying_one() -> None:
    bundle = {
        "registry_code": AGENT, "name": "X", "officers": [],
        "shareholders": [], "beneficial_owners": [{"eesnimi": "A", "nimi": "B", "isikukood": "38001010011"}],
    }
    with pytest.raises(SourceSchemaError):
        validate_raw("ariregister", AriregisterBundle, bundle)


# ---------------------------------------------------------------------------
# Search, dispatch, hops, the hit
# ---------------------------------------------------------------------------


async def test_search_reads_the_index(index) -> None:
    hits = await REGISTRY["ariregister"].search("põhjala", SearchKind.ENTITY)
    assert [h.hit_id for h in hits] == [HOLD]
    assert hits[0].summary == f"EE-ARIREGISTER {HOLD} · Osaühing · in liquidation"
    assert hits[0].liveness == "snapshot"
    assert await REGISTRY["ariregister"].search("100%_", SearchKind.ENTITY) == [], "LIKE wildcards are escaped"


def _dispatched(ctx: _LookupCtx) -> set[str]:
    tasks = _dispatch(ctx, only="ariregister")
    for _, awaitable in tasks:
        close = getattr(awaitable, "close", None)
        if close:
            close()
    return {sid for sid, _ in tasks}


def test_dispatched_with_and_without_an_index(index) -> None:
    ctx = _LookupCtx(lei="5493005044RTLQ5RZU70", jurisdiction="EE", registered_as=EESTI)
    _build_derived(ctx, "RA000181")
    assert ctx.derived.get("ee_registry_code") == EESTI
    assert _dispatched(ctx) == {"ariregister"}


def test_the_register_hop_owns_the_scheme() -> None:
    hop = hop_for("EE-ARIREGISTER")
    assert hop is not None and hop.source_id == "ariregister"
    assert hop.normalise("6752073") == "06752073"
    assert hop_for("REG-EE") is not None and hop_for("REG-EE").source_id == "ariregister"


async def test_the_note_card_survives_the_pipelines_stub_drop(index) -> None:
    with respx.mock() as mock:
        mock.get(_page_url(ABSENT)).mock(return_value=Response(429))
        bundle = await _fetch(ABSENT, "GHOST OÜ")
    ctx = _LookupCtx(lei="X", legal_name="GHOST OÜ", jurisdiction="EE", registered_as=ABSENT)
    _build_derived(ctx, "RA000181")
    hit = _build_result_hit("ariregister", bundle, ctx)
    assert hit is not None
    assert hit.identifiers == {}, "the register published no identifier for us"
    assert hit.finding and hit.finding.startswith("Not in the e-Business Register's open data of 12 October 2026")


async def test_a_record_asserts_the_code_and_says_what_is_on_file(index) -> None:
    bundle = await _fetch(EESTI)
    ctx = _LookupCtx(lei="5493005044RTLQ5RZU70", legal_name="Eesti Energia AS", jurisdiction="EE", registered_as=EESTI)
    _build_derived(ctx, "RA000181")
    hit = _build_result_hit("ariregister", bundle, ctx)
    assert hit is not None and hit.identifiers == {"ee_registry_code": EESTI}
    # The 140-character cap drops the chain clause and the legal form here;
    # the counts come first on purpose.
    assert hit.finding == (
        "In the register since 31 March 1998; 1 shareholder, 3 on the registry card, "
        "2 on the supervisory board, no owner identified, 2 named."
    )
    assert len(hit.finding) <= MAX_FINDING_CHARS


async def test_a_liquidation_leads_the_finding(index) -> None:
    finding = finding_ariregister(await _fetch(HOLD)) or ""
    assert finding.startswith("In liquidation; 4 shareholders")
    assert "1 more company followed up the chain; 1 owner not followed" in finding


# ---------------------------------------------------------------------------
# BODS
# ---------------------------------------------------------------------------


async def _statements(code: str) -> list[dict[str, Any]]:
    with provenance.recording() as rec:
        bundle = await _fetch(code)
    with provenance.mapping_provenance(rec.resolve(is_stub=bundle.get("is_stub", False))):
        statements = list(map_ariregister(bundle))
    for s in statements:
        assert validator.jurisdiction_input_issues(s) == []
        assert validator.address_type_issues(s) == []
    return statements


def _by_type(statements: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [s for s in statements if s["recordType"] == kind]


def _interests(statements: list[dict[str, Any]], party_name: str) -> list[dict[str, Any]]:
    ids = {
        s["recordId"] for s in statements
        if (s["recordType"] == "entity" and s["recordDetails"].get("name") == party_name)
        or (s["recordType"] == "person"
            and any(n.get("fullName") == party_name for n in s["recordDetails"].get("names", [])))
    }
    return [
        i for s in _by_type(statements, "relationship")
        if s["recordDetails"]["interestedParty"] in ids
        for i in s["recordDetails"]["interests"]
    ]


async def test_one_person_on_three_lists_is_one_node(index) -> None:
    statements = await _statements(AGENT)
    people = _by_type(statements, "person")
    assert len(people) == 1 and len(_by_type(statements, "relationship")) == 3
    person = people[0]
    assert "identifiers" not in person["recordDetails"], "the hash is never an identifier"
    notes = [a for a in person.get("annotations", []) if a["motivation"] == "identifying"]
    assert len(notes) == 1 and H_VERONIKA in notes[0]["description"]
    types = sorted(i["type"] for i in _interests(statements, "Veronika Oškina"))
    assert types == ["boardMember", "shareholding", "shareholding"]
    bo = [i for i in _interests(statements, "Veronika Oškina") if i.get("beneficialOwnershipOrControl")]
    assert bo and bo[0]["directOrIndirect"] == "direct" and "otsene osalus" in bo[0]["details"]
    assert person["recordDetails"]["addresses"] == [
        {"type": "residence", "country": {"name": "Estonia", "code": "EE"}}
    ]


async def test_index_statements_are_dated_by_the_cut(index) -> None:
    statements = await _statements(AGENT)
    assert {s["statementDate"] for s in statements} == {CUT}
    assert all(s["source"]["retrievedAt"] for s in statements)


async def test_page_statements_are_not_dated_by_the_cut(index) -> None:
    with respx.mock() as mock:
        mock.get(_page_url(ABSENT)).mock(return_value=Response(200, text=_PAGE))
        with provenance.recording() as rec:
            bundle = await _fetch(ABSENT)
    with provenance.mapping_provenance(rec.resolve(is_stub=False)):
        statements = list(map_ariregister(bundle))
    assert statements and all(s["statementDate"] != CUT or CUT == statements[0]["source"]["retrievedAt"][:10] for s in statements)
    assert bundle["index_miss"] is True


def a_motivation_is_stated(note: str) -> bool:
    return "as filed in the e-Business Register" in note


async def test_foreign_parties_countries_and_codes(index) -> None:
    statements = await _statements(HOLD)
    entities = {s["recordDetails"]["name"]: s for s in _by_type(statements, "entity")}
    latvian = entities["SIA Ziemeļi"]
    assert latvian["recordDetails"]["jurisdiction"] == {"name": "Latvia", "code": "LV"}
    # The register states no scheme for a foreign code; one with the shape of
    # a Latvian registration number is read as one, and the annotation says so.
    assert latvian["recordDetails"]["identifiers"] == [
        {"id": "40003242879", "scheme": "LV-UR", "schemeName": "UR — Latvian Register of Enterprises"}
    ]
    (note,) = [a["description"] for a in latvian["annotations"] if "40003242879" in a["description"]]
    assert "derived from it" in note and "states no scheme" in note and a_motivation_is_stated(note)
    estonian = entities["Haudemaja OÜ"]
    assert estonian["recordDetails"]["identifiers"][0]["scheme"] == "EE-ARIREGISTER"
    nasdaq = [i for i in _interests(statements, "SIA Ziemeļi")]
    assert "NASDAQ andmed" in nasdaq[0]["details"] and "2025-01-14" in nasdaq[0]["details"]
    people = {s["recordDetails"]["names"][0]["fullName"]: s for s in _by_type(statements, "person")}
    lars = people["Lars Jensen"]
    assert lars["recordDetails"]["nationalities"] == [{"name": "Denmark", "code": "DK"}], "DEN is an IOC code"
    assert lars["recordDetails"]["birthDate"] == "1987-01-20"
    assert lars["recordDetails"]["addresses"] == [{"type": "residence", "country": {"name": "Germany", "code": "DE"}}]
    lars_interests = _interests(statements, "Lars Jensen")
    assert sorted(i["type"] for i in lars_interests) == ["seniorManagingOfficial", "shareholding", "shareholding"]
    joint = [i for i in lars_interests if i["type"] == "shareholding" and not i.get("beneficialOwnershipOrControl")][0]
    assert "joint ownership" in joint["details"] and "Ühisomand" in joint["details"]
    indirect = [i for i in lars_interests if i.get("beneficialOwnershipOrControl")][0]
    assert indirect["type"] == "shareholding" and indirect["directOrIndirect"] == "indirect"
    kosovan = people["Nurlan Abenov"]
    assert kosovan["recordDetails"]["nationalities"] == [{"name": "Kosovo"}], "XXK has no ISO code: the name alone"
    assert "addresses" not in kosovan["recordDetails"]
    assert "Kadri Kask" not in people, "a contact person is no interest"


async def test_an_ended_holding_closes_by_the_one_rule(index) -> None:
    statements = await _statements(HOLD)
    mari = [
        s for s in _by_type(statements, "relationship")
        if s["recordDetails"]["interests"][0].get("endDate") == "2024-02-01"
    ]
    assert len(mari) == 1 and mari[0]["recordStatus"] == "closed"


async def test_a_liquidation_is_pending_and_a_subtype_is_a_detail(index) -> None:
    statements = await _statements(HOLD)
    company = _by_type(statements, "entity")[0]
    assert company["recordDetails"]["name"] == 'Holding "Põhjala" OÜ'
    assert company["recordDetails"]["entityType"] == {"type": "registeredEntity", "details": "Osaühing"}
    assert company["recordDetails"]["foundingDate"] == "2015-01-02"
    assert company["recordDetails"]["identifiers"][0] == {
        "id": HOLD, "scheme": "EE-ARIREGISTER", "schemeName": "Estonian e-Business Register (Äriregister)",
    }
    notes = " ".join(a["description"] for a in company.get("annotations", []))
    assert "Likvideerimisel" in notes


async def test_a_government_agency_is_a_state_body_with_its_superior_authority(index) -> None:
    statements = await _statements(STATE)
    agency = _by_type(statements, "entity")[0]
    assert agency["recordDetails"]["entityType"] == {
        "type": "stateBody",
        "details": "Täidesaatva riigivõimu asutus või riigi muu institutsioon — Ministeeriumi hallatav riigiasutus",
    }
    ministry = [s for s in _by_type(statements, "entity") if s["recordDetails"]["name"].endswith("ministeerium")]
    assert ministry and ministry[0]["recordDetails"]["identifiers"][0]["id"] == "70000734"
    rel = _by_type(statements, "relationship")
    assert len(rel) == 1 and rel[0]["recordDetails"]["interests"][0]["details"].startswith("Superior authority")


async def test_board_member_bos_and_the_discrepancy_flag(index) -> None:
    statements = await _statements(EESTI)
    bo_interests = [
        (s, i) for s in _by_type(statements, "relationship")
        for i in s["recordDetails"]["interests"] if "beneficialOwnershipOrControl" in i
        and i["type"] == "seniorManagingOfficial"
    ]
    # Two board members on the BO list because no owner was identified (J):
    # listed, but not beneficial owners (20 Oct 2026).
    assert len(bo_interests) == 2
    assert {i["beneficialOwnershipOrControl"] for _, i in bo_interests} == {False}
    flagged = [s for s, _ in bo_interests if any("lahknevusteade" in a["description"] for a in s.get("annotations", []))]
    assert len(flagged) == 1
    assert flagged[0]["annotations"][0]["statementPointerTarget"] == "/recordDetails/interests/0"
    # The share registrar (ARP) is no interest; the two board members are.
    assert "Nasdaq CSD SE Eesti filiaal" not in {s["recordDetails"].get("name") for s in _by_type(statements, "entity")}
    # Two board members on the card, two on the supervisory board (off the card).
    assert len(_by_type(statements, "person")) == 4


# ---------------------------------------------------------------------------
# The persons outside the registry card (kaardivalised_isikud)
# ---------------------------------------------------------------------------


async def test_the_supervisory_board_and_the_as_shareholder_are_mapped(index) -> None:
    statements = await _statements(EESTI)
    tiina = _interests(statements, "Tiina Laane")
    assert len(tiina) == 1 and tiina[0]["type"] == "boardChair"
    assert tiina[0]["details"].startswith("Chair of the supervisory board") and tiina[0]["startDate"] == "2024-05-12"
    raivo = _interests(statements, "Raivo Vare")
    assert len(raivo) == 1 and raivo[0]["type"] == "boardMember" and "nõukogu liige" in raivo[0]["details"]
    # The register's own record of the AS's sole shareholder is a shareholding,
    # the OÜ shape: exact percentage, Nasdaq CSD as the entry's source.
    entities = {s["recordDetails"]["name"]: s for s in _by_type(statements, "entity")}
    state = entities["Eesti Vabariik"]
    assert state["recordDetails"]["identifiers"][0] == {
        "id": "70000001", "scheme": "EE-ARIREGISTER", "schemeName": "Estonian e-Business Register",
    }
    holding = _interests(statements, "Eesti Vabariik")
    assert len(holding) == 1 and holding[0]["type"] == "shareholding"
    assert holding[0]["share"] == {"exact": 100.0}
    assert holding[0]["details"].startswith("Shareholder of a public limited company (aktsionär)")
    assert "NASDAQ andmed" in holding[0]["details"] and "2025-07-09" in holding[0]["details"]
    assert holding[0]["startDate"] == "1998-03-31"
    rel = [s for s in _by_type(statements, "relationship") if s["recordDetails"]["interests"] == holding]
    assert rel[0]["recordStatus"] == "new", "a current holding"
    # The audit firm is no interest in the company.
    assert "Aktsiaselts PricewaterhouseCoopers" not in entities
    for s in statements:
        for a in s.get("annotations", []):
            assert "10142876" not in a["description"]


async def test_a_founder_still_holding_is_said_on_the_holding(index) -> None:
    statements = await _statements(AGENT)
    rels = _by_type(statements, "relationship")
    assert len(rels) == 3, "no second record for a founder who still holds"
    holding = [r for r in rels if r["recordDetails"]["interests"][0]["type"] == "shareholding"
               and not r["recordDetails"]["interests"][0].get("beneficialOwnershipOrControl")]
    assert len(holding) == 1 and holding[0]["recordStatus"] == "new"
    notes = [a for a in holding[0].get("annotations", []) if a["description"].startswith("Also recorded as a founder")]
    assert len(notes) == 1
    assert notes[0]["statementPointerTarget"] == "/recordDetails/interests/0"
    assert "founding contribution 0.01 EUR" in notes[0]["description"] and "2023-06-03" in notes[0]["description"]


async def test_a_founder_no_longer_holding_is_a_closed_record_with_no_end_date(index) -> None:
    statements = await _statements(HOLD)
    people = {s["recordDetails"]["names"][0]["fullName"] for s in _by_type(statements, "person")}
    assert "Peeter Paan" in people
    peeter = _interests(statements, "Peeter Paan")
    assert len(peeter) == 1 and peeter[0]["type"] == "shareholding"
    assert "share" not in peeter[0] and "endDate" not in peeter[0]
    assert peeter[0]["startDate"] == "2015-01-02"
    assert peeter[0]["details"].startswith("Founder, no contribution made")
    assert "no longer on the current list" in peeter[0]["details"]
    rel = [s for s in _by_type(statements, "relationship") if s["recordDetails"]["interests"] == peeter]
    assert rel[0]["recordStatus"] == "closed"
    # Mari Mets holds an ended shareholding (closed by its end date) and was a
    # founder with a contribution: one person node, two closed records.
    mari = _interests(statements, "Mari Mets")
    assert len(mari) == 2 and {i["type"] for i in mari} == {"shareholding"}
    founder = [i for i in mari if "founding contribution 2500.00 EUR" in i.get("details", "")]
    assert len(founder) == 1 and "endDate" not in founder[0]
    assert len([s for s in _by_type(statements, "person")
                if s["recordDetails"]["names"][0]["fullName"] == "Mari Mets"]) == 1


@pytest.mark.parametrize("code", [AGENT, EESTI, HOLD, STATE, HAUDE])
async def test_an_exact_share_stands_alone(index, code: str) -> None:
    """libcove fails ``exact`` beside ``minimum``/``maximum``
    (statement_relationship_interests_exact_has_min_max) — found by the BODS
    quality sweep on Eesti Energia AS's AS-shareholder and history records.
    Every shareholder-shaped row (OÜ, AS ``S``, founder) goes through one
    helper, so every company in the fixture is walked."""
    shares = [
        i["share"] for s in _by_type(await _statements(code), "relationship")
        for i in s["recordDetails"]["interests"] if "share" in i
    ]
    for share in shares:
        if "exact" in share:
            assert share == {"exact": share["exact"]}, share


async def test_the_page_path_has_no_off_card_list(index) -> None:
    with respx.mock() as mock:
        mock.get(_page_url(ABSENT)).mock(return_value=Response(200, text=_PAGE))
        bundle = await _fetch(ABSENT)
    assert bundle["tier"] == "page" and bundle.get("off_card_persons", []) == []


def test_an_off_card_row_naming_an_unknown_company_is_refused(tmp_path: Path) -> None:
    _refused(tmp_path, "is not in", off_cards=_off_cards() + [{"ariregistri_kood": int(ABSENT), "nimi": "Ghost", "kaardivalised_isikud": []}])


async def test_a_schema_one_index_is_read_with_no_off_card_list(tmp_path: Path) -> None:
    """An index built before the fifth file (schema 1) still answers — with
    an empty off-card list — so a deploy before the next rebuild does not
    send every Estonian lookup to the page."""
    out, _ = _build(tmp_path)
    conn = sqlite3.connect(out)
    conn.execute("DROP TABLE off_card_person")
    conn.execute("UPDATE meta SET value = '1' WHERE key = 'schema_version'")
    conn.commit()
    conn.close()
    _point_at(out)
    try:
        bundle = await _fetch(EESTI)
        assert bundle["tier"] == "index" and bundle["off_card_persons"] == []
        # With no AS shareholder row, the registrar on the card says where
        # the shareholders are (the üldandmed phase).
        assert finding_ariregister(bundle).startswith("In the register since 31 March 1998; shareholders kept at Nasdaq CSD")
    finally:
        _point_at(None)


# ---------------------------------------------------------------------------
# The chain above the company, from the index
# ---------------------------------------------------------------------------


async def test_the_chain_above_the_company_arrives_in_the_bundle(index) -> None:
    bundle = await _fetch(HOLD)
    assert set(bundle["related_companies"]) == {HAUDE}
    assert bundle["related_companies"][HAUDE]["tier"] == "index"
    chain = bundle["chain"]
    assert chain["depth"] == 1 and chain["related"] == 1
    # The Latvian shareholder is on a foreign register; the holding's own
    # 40% of Haudemaja is a cycle back to the subject and is not "unfollowed".
    reasons = {(u["name"], u["reason"]) for u in chain["unfollowed"]}
    assert reasons == {("SIA Ziemeļi", ariregister.UNFOLLOWED_FOREIGN)}
    # The AS whose registered shareholder's code is in no file.
    eesti = await _fetch(EESTI)
    assert eesti["related_companies"] == {}
    assert [u["reason"] for u in eesti["chain"]["unfollowed"]] == [ariregister.UNFOLLOWED_NOT_IN_INDEX]


async def test_the_chain_is_one_graph_with_the_register_record_for_each_company(index) -> None:
    statements = await _statements(HOLD)
    entities = {s["recordDetails"]["name"]: s for s in _by_type(statements, "entity")}
    haude = entities["Haudemaja OÜ"]
    # The related company's own record wins over the thin statement its
    # shareholder row builds: same statementId, founding date present.
    assert haude["recordDetails"]["foundingDate"] == "2010-03-10"
    assert sum(1 for s in _by_type(statements, "entity") if s["recordDetails"]["name"] == "Haudemaja OÜ") == 1
    # Kalev Kask holds 60% of Haudemaja and sits on its board — one node.
    kalev = _interests(statements, "Kalev Kask")
    assert sorted(i["type"] for i in kalev) == ["boardMember", "shareholding"]
    assert [i["share"]["exact"] for i in kalev if i["type"] == "shareholding"] == [60.0]
    rels = _by_type(statements, "relationship")
    haude_rels = [r for r in rels if r["recordDetails"]["subject"] == haude["recordId"]]
    assert len(haude_rels) == 3  # Kalev's share, Kalev's board seat, the holding's 40% (the cycle)
    holding_up = [r for r in haude_rels if r["recordDetails"]["interestedParty"] == entities['Holding "Põhjala" OÜ']["recordId"]]
    assert holding_up and holding_up[0]["recordDetails"]["interests"][0]["share"]["exact"] == 40.0
    assert {s["statementDate"] for s in statements} == {CUT}, "the chain is dated by the same cut"


async def test_a_related_companys_founders_are_not_the_subjects_history(index, tmp_path: Path) -> None:
    off_cards = _off_cards() + [{
        "ariregistri_kood": int(HAUDE), "nimi": "Haudemaja OÜ",
        "kaardivalised_isikud": [_off_card(kirje_id=81, eesnimi="Vana", nimi_arinimi="Asutaja",
                                           isikukood_hash="a3b4c5d6-e7f8-5901-2345-678901234567")],
    }]
    out, _ = _build(tmp_path / "ee-chain-founders", off_cards=off_cards)
    _point_at(out)
    try:
        statements = await _statements(HOLD)
        people = {s["recordDetails"]["names"][0]["fullName"] for s in _by_type(statements, "person")}
        assert "Vana Asutaja" not in people
        assert "Peeter Paan" in people, "the subject's own founders are still its history"
    finally:
        _point_at(None)


async def test_the_integrated_figure_reaches_the_person_above_the_chain(index) -> None:
    from opencheck import integrated_ownership

    statements = await _statements(HOLD)
    payload = integrated_ownership.for_lookup(f"EE-ARIREGISTER:{HOLD}", statements, "2026-10-16")
    by_name = {h["name"]: h for h in integrated_ownership.with_figures(payload)}
    kalev = by_name["Kalev Kask"]
    figs = kalev["figures"]
    assert len(figs) == 1 and not figs[0]["direct"], "one source, one indirect figure"
    # 60% of Haudemaja, which holds 33.33% of the holding, which holds 40% of
    # Haudemaja back: a cycle that settles above the simple 20% product.
    assert 23.0 <= figs[0]["exact"] <= 23.2, figs


# ---------------------------------------------------------------------------
# Reverse holdings: what an Estonian company holds (the Subsidiaries tab)
# ---------------------------------------------------------------------------


def test_holdings_of_reads_the_reverse_side_of_the_index(index) -> None:
    held = ariregister.holdings_of(HOLD)
    assert [(h["code"], h["relation"], h["percent"]) for h in held] == [(HAUDE, "shareholder", "40.00")]
    assert held[0]["name"] == "Haudemaja OÜ" and held[0]["status_code"] == "R"
    assert held[0]["status_text"], "the register's own wording of the status rides along"
    back = ariregister.holdings_of(HAUDE)
    assert [(h["code"], h["percent"]) for h in back] == [(HOLD, "33.33")]
    assert ariregister.holdings_of(AGENT) == [], "a company holding nothing has no rows, not an error"


def test_holdings_of_is_empty_without_an_index(no_index) -> None:
    assert ariregister.holdings_of(HOLD) == []


def _mirror(monkeypatch, rows: dict[str, dict[str, Any]] | None) -> None:
    """A GLEIF mirror that knows the given LEIs' ``registeredAs`` details."""
    from opencheck import entity_pages

    if rows is None:
        monkeypatch.setattr(entity_pages, "get_store", lambda: None)
        return

    class _Row:
        def __init__(self, detail: dict[str, Any]) -> None:
            self.detail = detail

    class _Store:
        def get(self, lei: str):
            return _Row(rows[lei]) if lei in rows else None

    monkeypatch.setattr(entity_pages, "get_store", lambda: _Store())


EE_LEI = "5493001XVZDYQ2S0OE42"
NL_LEI = "724500PMK2A2M1SQQ228"


def test_the_declared_block_lists_what_an_estonian_company_holds(index, monkeypatch) -> None:
    from opencheck.subsidiaries_declared import _ariregister_block

    _mirror(monkeypatch, {
        EE_LEI: {"registeredAt": {"id": "RA000181"}, "registeredAs": HOLD},
        NL_LEI: {"registeredAt": {"id": "RA000463"}, "registeredAs": "12345678"},
    })
    block = _ariregister_block(EE_LEI)
    assert block["available"] and block["covered"] and block["total"] == 1
    assert block["context"] == {"registry_code": HOLD, "cut": CUT}
    (row,) = block["rows"]
    assert row["name"] == "Haudemaja OÜ" and row["percent"] == 40.0 and row["lei"] is None
    assert row["country"] == "EST" and row["relation"] == "shareholder"
    assert row["register_id"] == {"scheme": "EE-ARIREGISTER", "id": HAUDE}, "the key a register lookup takes"
    # A company on another register is outside the source, not a failure.
    other = _ariregister_block(NL_LEI)
    assert other["available"] and not other["covered"] and other["reason"]
    # An LEI the mirror does not know reads the same way.
    unknown = _ariregister_block("5493000000000000ZZ11")
    assert unknown["available"] and not unknown["covered"]


def test_the_declared_block_says_when_it_cannot_read(no_index, monkeypatch) -> None:
    from opencheck.subsidiaries_declared import _ariregister_block

    _mirror(monkeypatch, None)
    block = _ariregister_block(EE_LEI)
    assert block["available"] is False and "mirror" in block["reason"]
    _mirror(monkeypatch, {EE_LEI: {"registeredAt": {"id": "RA000181"}, "registeredAs": HOLD}})
    block = _ariregister_block(EE_LEI)
    assert block["available"] is False and "index" in block["reason"]
    # Only an Estonian subject needs the index.
    _mirror(monkeypatch, {NL_LEI: {"registeredAt": {"id": "RA000463"}, "registeredAs": "1"}})
    assert _ariregister_block(NL_LEI)["available"] is True


# ---------------------------------------------------------------------------
# Cross-border hops: a foreign code as a number on its own register
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("code", "alpha2", "expected"),
    [
        ("40003242879", "LV", ("LV-UR", "40003242879")),
        ("LV40003242879", "LV", ("LV-UR", "40003242879")),   # a country prefix is removed
        ("1234567-8", "FI", ("FI-PRH", "1234567-8")),
        ("556677-8899", "SE", ("SE-BLV", "5566778899")),     # the hop's own normalisation
        ("SC123456", "GB", ("GB-COH", "SC123456")),
        ("ABC", "LV", None),                                 # not the shape of a Latvian number
        ("40003242879", "DE", None),                         # no single German register hop
        ("HRB 12345", "DE", None),
        ("40003242879", None, None),                         # no country, no register
        ("", "LV", None),
    ],
)
def test_a_foreign_code_becomes_a_register_identifier_only_in_shape(code, alpha2, expected) -> None:
    from opencheck.bods.mappers.estonia import _ee_foreign_register_identifier

    identifiers, derived = _ee_foreign_register_identifier(code, alpha2)
    if expected is None:
        assert identifiers == [] and derived is None
    else:
        scheme, number = expected
        assert derived == identifiers[0] and (derived["scheme"], derived["id"]) == (scheme, number)
        assert derived["schemeName"] and derived["schemeName"] != scheme


def test_every_foreign_code_shape_names_a_register_hop() -> None:
    """A shape for a scheme no hop owns is dead; a hop that owns a scheme in
    the table must be its country's only register (the ``REG-`` alias)."""
    from opencheck.bods.mappers.estonia import _EE_FOREIGN_CODE_SHAPES

    aliased = {h.scheme for k, h in hop_schemes().items() if k.startswith("REG-")}
    assert set(_EE_FOREIGN_CODE_SHAPES) <= aliased, set(_EE_FOREIGN_CODE_SHAPES) - aliased


async def test_a_foreign_owner_with_a_derived_identifier_is_a_register_hop(index) -> None:
    """What the chain walk could not follow inside the index, FullCheck can:
    the Latvian owner's statement carries the scheme ``register_hops`` owns."""
    statements = await _statements(HOLD)
    latvian = {s["recordDetails"]["name"]: s for s in _by_type(statements, "entity")}["SIA Ziemeļi"]
    ident = latvian["recordDetails"]["identifiers"][0]
    hop = hop_for(ident["scheme"])
    assert hop is not None and hop.source_id == "ur_latvia"
    assert hop.normalise(ident["id"]) == "40003242879"
    # The walk still reports it as not followed — the index is Estonia only.
    bundle = await _fetch(HOLD)
    assert {(u["name"], u["reason"]) for u in bundle["chain"]["unfollowed"]} >= {("SIA Ziemeļi", ariregister.UNFOLLOWED_FOREIGN)}
