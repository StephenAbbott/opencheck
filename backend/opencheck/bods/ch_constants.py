"""Companies House ``constants.yml`` enumerations → human-readable labels.

AUTO-GENERATED from the official Companies House ``constants.yml``:
https://github.com/companieshouse/api-enumerations/blob/master/constants.yml

Do NOT edit by hand. Regenerate with::

    python backend/scripts/revendor_ch_constants.py

Public sector information licensed under the Open Government Licence v3.0.
"""
from __future__ import annotations



OFFICER_ROLE_LABELS: dict[str, str] = {
    'cic-manager': 'CIC Manager',
    'corporate-director': 'Director',
    'corporate-llp-designated-member': 'LLP Designated Member',
    'corporate-llp-member': 'LLP Member',
    'corporate-manager-of-an-eeig': 'Manager of an EEIG',
    'corporate-managing-officer': 'Managing Officer',
    'corporate-member-of-a-management-organ': 'Member of a Management Organ',
    'corporate-member-of-a-supervisory-organ': 'Member of a Supervisory Organ',
    'corporate-member-of-an-administrative-organ': 'Member of an Administrative Organ',
    'corporate-nominee-director': 'Nominee Director',
    'corporate-nominee-secretary': 'Nominee Secretary',
    'corporate-secretary': 'Secretary',
    'director': 'Director',
    'general-partner-in-a-limited-partnership': 'General partner',
    'corporate-general-partner-in-a-limited-partnership': 'General partner',
    'limited-partner-in-a-limited-partnership': 'Limited partner',
    'corporate-limited-partner-in-a-limited-partnership': 'Limited partner',
    'judicial-factor': 'Judicial Factor',
    'llp-designated-member': 'LLP Designated Member',
    'llp-member': 'LLP Member',
    'manager-of-an-eeig': 'Manager of an EEIG',
    'managing-officer': 'Managing Officer',
    'member-of-a-management-organ': 'Member of a Management Organ',
    'member-of-a-supervisory-organ': 'Member of a Supervisory Organ',
    'member-of-an-administrative-organ': 'Member of an Administrative Organ',
    'nominee-director': 'Nominee Director',
    'nominee-secretary': 'Nominee Secretary',
    'person-authorised-to-accept': 'Person Authorised to Accept',
    'person-authorised-to-represent': 'Person Authorised to Represent',
    'person-authorised-to-represent-and-accept': 'Person Authorised to Represent and Accept',
    'receiver-and-manager': 'Receiver and Manager',
    'secretary': 'Secretary',
}


COMPANY_TYPE_LABELS: dict[str, str] = {
    'private-unlimited': 'Private unlimited company',
    'ltd': 'Private limited company',
    'plc': 'Public limited company',
    'old-public-company': 'Old public company',
    'private-limited-guarant-nsc-limited-exemption': "Private Limited Company by guarantee without share capital, use of 'Limited' exemption",
    'limited-partnership': 'Limited partnership',
    'private-limited-guarant-nsc': 'Private limited by guarantee without share capital',
    'converted-or-closed': 'Converted / closed',
    'private-unlimited-nsc': 'Private unlimited company without share capital',
    'private-limited-shares-section-30-exemption': "Private Limited Company, use of 'Limited' exemption",
    'protected-cell-company': 'Protected cell company',
    'assurance-company': 'Assurance company',
    'oversea-company': 'Overseas company',
    'eeig-establishment': 'European Economic Interest Grouping Establishment (EEIG)',
    'icvc-securities': 'Investment company with variable capital',
    'icvc-warrant': 'Investment company with variable capital',
    'icvc-umbrella': 'Investment company with variable capital',
    'registered-society-non-jurisdictional': 'Registered society',
    'industrial-and-provident-society': 'Industrial and Provident society',
    'northern-ireland': 'Northern Ireland company',
    'northern-ireland-other': 'Credit union (Northern Ireland)',
    'llp': 'Limited liability partnership',
    'royal-charter': 'Royal charter company',
    'investment-company-with-variable-capital': 'Investment company with variable capital',
    'unregistered-company': 'Unregistered company',
    'other': 'Other company type',
    'european-public-limited-liability-company-se': 'European public limited liability company (SE)',
    'united-kingdom-societas': 'United Kingdom Societas',
    'uk-establishment': 'UK establishment company',
    'scottish-partnership': 'Scottish qualifying partnership',
    'charitable-incorporated-organisation': 'Charitable incorporated organisation',
    'scottish-charitable-incorporated-organisation': 'Scottish charitable incorporated organisation',
    'further-education-or-sixth-form-college-corporation': 'Further education or sixth form college corporation',
    'eeig': 'European Economic Interest Grouping (EEIG)',
    'ukeig': 'United Kingdom Economic Interest Grouping',
    'registered-overseas-entity': 'Overseas entity',
}


COMPANY_STATUS_LABELS: dict[str, str] = {
    'active': 'Active',
    'dissolved': 'Dissolved',
    'liquidation': 'Liquidation',
    'receivership': 'Receiver Action',
    'converted-closed': 'Converted / Closed',
    'voluntary-arrangement': 'Voluntary Arrangement',
    'insolvency-proceedings': 'Insolvency Proceedings',
    'administration': 'In Administration',
    'open': 'Open',
    'closed': 'Closed',
    'registered': 'Registered',
    'removed': 'Removed',
}


def describe_officer_role(code: str | None) -> str | None:
    """Official label for a CH officer-role code (or None if unknown)."""
    return OFFICER_ROLE_LABELS.get((code or "").lower())


def describe_company_type(code: str | None) -> str | None:
    """Official label for a CH company-type code (or None if unknown)."""
    return COMPANY_TYPE_LABELS.get((code or "").lower())


def describe_company_status(code: str | None) -> str | None:
    """Official label for a CH company-status code (or None if unknown)."""
    return COMPANY_STATUS_LABELS.get((code or "").lower())
