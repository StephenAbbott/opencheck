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


# Companies House ``super_secure_description`` block (2 codes), vendored from the
# same ``psc_descriptions.yml``. A super-secure PSC withholds *all* particulars
# under a court protection order; this is CH's official explanatory text.
SUPER_SECURE_DESCRIPTIONS: dict[str, str] = {
    "super-secure-persons-with-significant-control": (
        "The person with significant control's details are not shown because "
        "restrictions on disclosing any of the individual's details are in force"
    ),
    "super-secure-beneficial-owner": (
        "The beneficial owners details are not shown because restrictions on using "
        "or disclosing any of the individual’s particulars are in force"
    ),
}

_DEFAULT_SUPER_SECURE = SUPER_SECURE_DESCRIPTIONS["super-secure-persons-with-significant-control"]


def describe_super_secure(code: str | None) -> str:
    """Return CH's official explanatory text for a super-secure PSC.

    Falls back to the generic PSC wording for an unknown/empty code, so callers
    always get a meaningful descriptor.
    """
    return SUPER_SECURE_DESCRIPTIONS.get((code or "").lower(), _DEFAULT_SUPER_SECURE)


# Companies House ``statement_description`` block, vendored from the same
# ``psc_descriptions.yml``. These are *PSC statements* — notices a company files
# instead of / alongside a PSC (e.g. "no PSC exists", "PSC not yet identified").
# Fetched from /company/{n}/persons-with-significant-control-statements.
PSC_STATEMENT_DESCRIPTIONS: dict[str, str] = {
    "no-individual-or-entity-with-signficant-control": "The company knows or has reasonable cause to believe that there is no registrable person or registrable relevant legal entity in relation to the company",
    "psc-exists-but-not-identified": "The company knows or has reasonable cause to believe that there is a registrable person in relation to the company but it has not identified the registrable person",
    "psc-details-not-confirmed": "The company has identified a registrable person in relation to the company but all the required particulars of that person have not been confirmed",
    "steps-to-find-psc-not-yet-completed": "The company has not yet completed taking reasonable steps to find out if there is anyone who is a registrable person or a registrable relevant legal entity in relation to the company",
    "psc-contacted-but-no-response": "The company has given a notice under section 790D or DA of the Act which has not been complied with",
    "psc-has-failed-to-confirm-changed-details": "{linked_psc_name} has failed to comply with a notice given by the company under section 790E or EA of the Act",
    "restrictions-notice-issued-to-psc": "The company has issued a restrictions notice under paragraph 1 of Schedule 1B to the Act",
    "no-individual-or-entity-with-signficant-control-partnership": "The partnership knows or has reasonable cause to believe that there is no registrable person or registrable relevant legal entity in relation to the partnership",
    "psc-exists-but-not-identified-partnership": "The partnership knows or has reasonable cause to believe that there is a registrable person in relation to the partnership but it has not identified the registrable person",
    "psc-details-not-confirmed-partnership": "The partnership has identified a registrable person in relation to the partnership but all the required particulars of that person have not been confirmed",
    "steps-to-find-psc-not-yet-completed-partnership": "The partnership has not yet completed taking reasonable steps to find out if there is anyone who is a registrable person or a registrable relevant legal entity in relation to the partnership",
    "psc-contacted-but-no-response-partnership": "The partnership has given a notice under Regulation 10 of The Scottish Partnerships (Register of People with Significant Control) Regulations 2017 which has not been complied with",
    "psc-has-failed-to-confirm-changed-details-partnership": "The partnership has given a notice under Regulation 11 of The Scottish Partnerships (Register of People with Significant Control) Regulations 2017 which has not been complied with",
    "restrictions-notice-issued-to-psc-partnership": "The partnership has issued a restrictions notice under paragraph 1 of Schedule 2 to The Scottish Partnerships (Register of People with Significant Control) Regulations 2017",
    "all-beneficial-owners-identified": "All beneficial owners have been identified and all required information can be provided",
    "no-beneficial-owner-identified": "No beneficial owners have been identified",
    "at-least-one-beneficial-owner-unidentified": "Some beneficial owners have been identified and all required information can be provided",
    "information-not-provided-for-at-least-one-beneficial-owner": "All beneficial owners have been identified but only some required information can be provided",
    "at-least-one-beneficial-owner-unidentified-and-information-not-provided-for-at-least-one-beneficial-owner": "Some beneficial owners have been identified and only some required information can be provided",
    "nobody-has-become-or-ceased-to-be-a-beneficial-owner": "Nobody has become or ceased to be a beneficial owner during the update period",
    "somebody-has-become-or-ceased-to-be-a-beneficial-owner": "Somebody has become or ceased to be a beneficial owner during the update period",
    "no-change-beneficial-owner-relevant-period": "The entity believes nobody has become or ceased to be a beneficial owner during the relevant period",
    "change-beneficial-owner-relevant-period": "The entity believes somebody has become or ceased to be a beneficial owner during the relevant period",
    "no-trust-involved-relevant-period": "The entity believes nobody has become or ceased to be a beneficial owner due to being a trustee during the relevant period",
    "trust-involved-relevant-period": "The entity believes somebody has become or ceased to be a beneficial owner due to being a trustee during the relevant period",
    "no-change-beneficiary-relevant-period": "The entity believes nobody has become or ceased to be a beneficiary during the relevant period",
    "change-beneficiary-relevant-period": "The entity believes somebody has become or ceased to be a beneficiary during the relevant period",
    "awaiting-confirmation-from-psc": "The company knows or believes it has a PSC but has not yet had confirmation",
}


def describe_statement(code: str) -> str | None:
    """Return the human-readable descriptor for a PSC *statement* code.

    Returns ``None`` for unknown codes so callers can fall back to the raw code.
    """
    return PSC_STATEMENT_DESCRIPTIONS.get((code or "").lower())
