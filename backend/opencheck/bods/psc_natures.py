"""Companies House ``natures_of_control`` code -> human-readable descriptor.

Vendored from the official Companies House enumeration file
``psc_descriptions.yml`` (``short_description`` block):
https://github.com/companieshouse/api-enumerations/blob/master/psc_descriptions.yml

Public sector information licensed under the Open Government Licence v3.0.

Used by ``mapper._parse_nature`` to populate the BODS ``interest.details``
field so the human meaning of every PSC nature-of-control code is preserved —
especially important for codes that map to the generic ``otherInfluenceOrControl``
interest type, where the specific meaning would otherwise be lost.

Covers all 86 nature-of-control codes (company, LLP, Scottish partnership, and
registered overseas entity variants).
"""

from __future__ import annotations

PSC_NATURE_DESCRIPTIONS: dict[str, str] = {
    # --- Ownership of shares (company) ---
    "ownership-of-shares-25-to-50-percent": "Ownership of shares – More than 25% but not more than 50%",
    "ownership-of-shares-50-to-75-percent": "Ownership of shares – More than 50% but less than 75%",
    "ownership-of-shares-75-to-100-percent": "Ownership of shares – 75% or more",
    "ownership-of-shares-25-to-50-percent-as-trust": "Ownership of shares – More than 25% but not more than 50% with control over the trustees of a trust",
    "ownership-of-shares-50-to-75-percent-as-trust": "Ownership of shares – More than 50% but less than 75% with control over the trustees of a trust",
    "ownership-of-shares-75-to-100-percent-as-trust": "Ownership of shares – 75% or more with control over the trustees of a trust",
    "ownership-of-shares-25-to-50-percent-as-firm": "Ownership of shares – More than 25% but not more than 50% as a member of a firm",
    "ownership-of-shares-50-to-75-percent-as-firm": "Ownership of shares – More than 50% but less than 75% as a member of a firm",
    "ownership-of-shares-75-to-100-percent-as-firm": "Ownership of shares – 75% or more as a member of a firm",
    # --- Ownership of shares (registered overseas entity) ---
    "ownership-of-shares-more-than-25-percent-registered-overseas-entity": "Ownership of shares - More than 25%",
    "ownership-of-shares-more-than-25-percent-as-trust-registered-overseas-entity": "Ownership of shares - More than 25% as trustees of a trust",
    "ownership-of-shares-more-than-25-percent-as-firm-registered-overseas-entity": "Ownership of shares - More than 25% as a member of a firm",
    "ownership-of-shares-more-than-25-percent-as-control-over-trust-registered-overseas-entity": "Has significant influence over a trust which holds more than 25% of the shares in the entity",
    "ownership-of-shares-more-than-25-percent-as-control-over-firm-registered-overseas-entity": "Has significant influence over a firm which holds more than 25% of the shares in the entity",
    # --- Voting rights (company) ---
    "voting-rights-25-to-50-percent": "Ownership of voting rights - More than 25% but not more than 50%",
    "voting-rights-50-to-75-percent": "Ownership of voting rights - More than 50% but less than 75%",
    "voting-rights-75-to-100-percent": "Ownership of voting rights - 75% or more",
    "voting-rights-25-to-50-percent-as-trust": "Ownership of voting rights - More than 25% but not more than 50% with control over the trustees of a trust",
    "voting-rights-50-to-75-percent-as-trust": "Ownership of voting rights - More than 50% but less than 75% with control over the trustees of a trust",
    "voting-rights-75-to-100-percent-as-trust": "Ownership of voting rights - 75% or more with control over the trustees of a trust",
    "voting-rights-25-to-50-percent-as-firm": "Ownership of voting rights - More than 25% but not more than 50% as a member of a firm",
    "voting-rights-50-to-75-percent-as-firm": "Ownership of voting rights - More than 50% but less than 75% as a member of a firm",
    "voting-rights-75-to-100-percent-as-firm": "Ownership of voting rights - 75% or more as a member of a firm",
    # --- Voting rights (registered overseas entity) ---
    "voting-rights-more-than-25-percent-registered-overseas-entity": "Ownership of voting rights - More than 25%",
    "voting-rights-more-than-25-percent-as-trust-registered-overseas-entity": "Holds voting rights - More than 25% as trustees of a trust",
    "voting-rights-more-than-25-percent-as-firm-registered-overseas-entity": "Ownership of voting rights - More than 25% as a member of a firm",
    "voting-rights-more-than-25-percent-as-control-over-trust-registered-overseas-entity": "Has significant influence over a trust which holds more than 25% of the voting rights in the entity",
    "voting-rights-more-than-25-percent-as-control-over-firm-registered-overseas-entity": "Has significant influence over a firm which has more than 25% of the voting rights in the entity",
    # --- Right to appoint and remove directors (company / ROE) ---
    "right-to-appoint-and-remove-directors": "Right to appoint or remove directors",
    "right-to-appoint-and-remove-directors-as-trust": "Right to appoint or remove directors with control over the trustees of a trust",
    "right-to-appoint-and-remove-directors-as-firm": "Right to appoint or remove directors as a member of a firm",
    "right-to-appoint-and-remove-directors-as-control-over-trust-registered-overseas-entity": "Has significant influence over a trust which has the right to appoint or remove directors of the entity",
    "right-to-appoint-and-remove-directors-as-control-over-firm-registered-overseas-entity": "Has significant influence over a firm which has the right to appoint or remove directors of the entity",
    "right-to-appoint-and-remove-directors-registered-overseas-entity": "Right to appoint or remove directors",
    "right-to-appoint-and-remove-directors-as-trust-registered-overseas-entity": "Right to appoint or remove directors as the trustees of a trust",
    "right-to-appoint-and-remove-directors-as-firm-registered-overseas-entity": "Right to appoint or remove members as a member of a firm",
    # --- Significant influence or control (company) ---
    "significant-influence-or-control": "Has significant influence or control",
    "significant-influence-or-control-as-trust": "Has significant influence or control over the trustees of a trust",
    "significant-influence-or-control-as-firm": "Has significant influence or control as a member of a firm",
    "significant-influence-or-control-as-control-over-trust-registered-overseas-entity": "Has significant influence over a trust which has control of the entity",
    "significant-influence-or-control-as-control-over-firm-registered-overseas-entity": "Has significant influence over a firm which controls the entity",
    # --- Significant influence or control (registered overseas entity) ---
    "significant-influence-or-control-registered-overseas-entity": "Has significant influence or control",
    "significant-influence-or-control-as-trust-registered-overseas-entity": "Has significant influence over the entity as the trustees of a trust",
    "significant-influence-or-control-as-firm-registered-overseas-entity": "Has significant influence or control as a member of a firm",
    # --- Right to share surplus assets (LLP) ---
    "right-to-share-surplus-assets-25-to-50-percent-limited-liability-partnership": "Right to surplus assets - More than 25% but not more than 50%",
    "right-to-share-surplus-assets-50-to-75-percent-limited-liability-partnership": "Right to surplus assets - More than 50% but less than 75%",
    "right-to-share-surplus-assets-75-to-100-percent-limited-liability-partnership": "Right to surplus assets - 75% or more",
    "right-to-share-surplus-assets-25-to-50-percent-as-trust-limited-liability-partnership": "Right to surplus assets - More than 25% but not more than 50% with control over the trustees of a trust",
    "right-to-share-surplus-assets-50-to-75-percent-as-trust-limited-liability-partnership": "Right to surplus assets - More than 50% but less than 75% with control over the trustees of a trust",
    "right-to-share-surplus-assets-75-to-100-percent-as-trust-limited-liability-partnership": "Right to surplus assets - 75% or more with control over the trustees of a trust",
    "right-to-share-surplus-assets-25-to-50-percent-as-firm-limited-liability-partnership": "Right to surplus assets - More than 25% but not more than 50% as a member of a firm",
    "right-to-share-surplus-assets-50-to-75-percent-as-firm-limited-liability-partnership": "Right to surplus assets - More than 50% but less than 75% as a member of a firm",
    "right-to-share-surplus-assets-75-to-100-percent-as-firm-limited-liability-partnership": "Right to surplus assets - 75% or more as a member of a firm",
    # --- Voting rights (LLP) ---
    "voting-rights-25-to-50-percent-limited-liability-partnership": "Ownership of voting rights - More than 25% but not more than 50%",
    "voting-rights-50-to-75-percent-limited-liability-partnership": "Ownership of voting rights - More than 50% but less than 75%",
    "voting-rights-75-to-100-percent-limited-liability-partnership": "Ownership of voting rights - 75% or more",
    "voting-rights-25-to-50-percent-as-trust-limited-liability-partnership": "Ownership of voting rights - More than 25% but not more than 50% with control over the trustees of a trust",
    "voting-rights-50-to-75-percent-as-trust-limited-liability-partnership": "Ownership of voting rights - More than 50% but less than 75% with control over the trustees of a trust",
    "voting-rights-75-to-100-percent-as-trust-limited-liability-partnership": "Ownership of voting rights - 75% or more with control over the trustees of a trust",
    "voting-rights-25-to-50-percent-as-firm-limited-liability-partnership": "Ownership of voting rights - More than 25% but not more than 50% as a member of a firm",
    "voting-rights-50-to-75-percent-as-firm-limited-liability-partnership": "Ownership of voting rights - More than 50% but less than 75% as a member of a firm",
    "voting-rights-75-to-100-percent-as-firm-limited-liability-partnership": "Ownership of voting rights - 75% or more as a member of a firm",
    # --- Right to appoint and remove members (LLP) ---
    "right-to-appoint-and-remove-members-limited-liability-partnership": "Right to appoint or remove members",
    "right-to-appoint-and-remove-members-as-trust-limited-liability-partnership": "Right to appoint or remove members with control over the trustees of a trust",
    "right-to-appoint-and-remove-members-as-firm-limited-liability-partnership": "Right to appoint or remove members as a member of a firm",
    # --- Significant influence or control (LLP) ---
    "significant-influence-or-control-limited-liability-partnership": "Has significant influence or control",
    "significant-influence-or-control-as-trust-limited-liability-partnership": "Has significant influence or control over the trustees of a trust",
    "significant-influence-or-control-as-firm-limited-liability-partnership": "Has significant influence or control as a member of a firm",
    # --- Right to share surplus assets (Scottish partnership) ---
    "part-right-to-share-surplus-assets-25-to-50-percent": "Right to surplus assets - More than 25% but not more than 50%",
    "part-right-to-share-surplus-assets-50-to-75-percent": "Right to surplus assets - More than 50% but less than 75%",
    "part-right-to-share-surplus-assets-75-to-100-percent": "Right to surplus assets - 75% or more",
    "part-right-to-share-surplus-assets-25-to-50-percent-as-trust": "Right to surplus assets - More than 25% but not more than 50% with control over the trustees of a trust",
    "part-right-to-share-surplus-assets-50-to-75-percent-as-trust": "Right to surplus assets - More than 50% but less than 75% with control over the trustees of a trust",
    "part-right-to-share-surplus-assets-75-to-100-percent-as-trust": "Right to surplus assets - 75% or more with control over the trustees of a trust",
    "part-right-to-share-surplus-assets-25-to-50-percent-as-firm": "Right to surplus assets - More than 25% but not more than 50% as a member of a firm",
    "part-right-to-share-surplus-assets-50-to-75-percent-as-firm": "Right to surplus assets - More than 50% but less than 75% as a member of a firm",
    "part-right-to-share-surplus-assets-75-to-100-percent-as-firm": "Right to surplus assets - 75% or more as a member of a firm",
    # --- Right to appoint and remove persons (Scottish partnership) ---
    "right-to-appoint-and-remove-person": "Right to appoint or remove persons",
    "right-to-appoint-and-remove-person-as-firm": "Right to appoint or remove persons as a member of a firm",
    "right-to-appoint-and-remove-person-as-trust": "Right to appoint or remove persons with control over the trustees of a trust",
    # --- Registered owner as nominee (registered overseas entity) ---
    "registered-owner-as-nominee-person-england-wales-registered-overseas-entity": "Overseas Entity holds land or property in England and Wales as a nominee for this person",
    "registered-owner-as-nominee-person-scotland-registered-overseas-entity": "Overseas Entity holds land or property in Scotland, as nominee for this person",
    "registered-owner-as-nominee-person-northern-ireland-registered-overseas-entity": "Overseas Entity holds land or property in Northern Ireland, as nominee for this person",
    "registered-owner-as-nominee-another-entity-england-wales-registered-overseas-entity": "Overseas Entity holds land or property in England and Wales as a nominee for another entity for which this person is the registered beneficial owner",
    "registered-owner-as-nominee-another-entity-scotland-registered-overseas-entity": "Overseas Entity holds land or property in Scotland as a nominee for another entity for which this person is the registered beneficial owner",
    "registered-owner-as-nominee-another-entity-northern-ireland-registered-overseas-entity": "Overseas Entity holds land or property in Northern Ireland as a nominee for another entity for which this person is the registered beneficial owner",
}


def describe_nature(code: str) -> str | None:
    """Return the human-readable descriptor for a PSC nature-of-control code.

    Matching is case-insensitive. Returns ``None`` for unknown codes so callers
    can fall back to the raw code string.
    """
    return PSC_NATURE_DESCRIPTIONS.get((code or "").lower())
