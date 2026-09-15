"""
Regex pattern dictionaries for deterministic CAO document lifecycle classification.

This module holds the raw pattern strings and precompiled ``re.compile`` objects
used by ``classifier.py`` to score filenames and parsed-markdown snippets. Patterns
are grouped by semantic cue so they can be tuned without changing classifier logic.

Filename stems use ``_`` and digits as separators; ``\\b`` word boundaries do *not*
split on underscores (Python treats ``_`` as a word character). Short tokens therefore
use letter-only lookaround: ``(?<![a-zA-Z])token(?![a-zA-Z])``.

Exports:
    FILENAME_PATTERNS, CONTENT_PATTERNS — dict[str, list[str]].
    FILENAME_PATTERNS_RE, CONTENT_PATTERNS_RE — compiled with ``re.IGNORECASE``.
"""

from __future__ import annotations

import re
from typing import Dict, List


def _fn_token(token: str) -> str:
    """
    Wrap a filename token so it matches when separated by ``_``, ``-``, or punctuation.

    Parameters:
        token: Literal regex fragment (no surrounding groups).

    Returns:
        Pattern string with letter-only lookaround (underscore-safe).
    """
    return rf"(?<![a-zA-Z]){token}(?![a-zA-Z])"


# All matches case-insensitive. Use re.compile with re.IGNORECASE.
FILENAME_PATTERNS: Dict[str, List[str]] = {
    "definitive": [
        _fn_token("definitief"),
        _fn_token("def"),
        _fn_token("DEF"),
        _fn_token("definitive"),
        _fn_token("final"),
        r"definitieve[_\s-]?versie",
        r"definitieve[_\s-]?tekst",
        r"def[_\s-]?versie",
    ],
    "signed": [
        _fn_token("ondertekend"),
        _fn_token("getekend"),
        _fn_token("signed"),
        r"met[\s_-]+handtekening",
        r"completed and signed",
        r"met[\s_-]+paraaf",
    ],
    "unsigned_negation": [
        r"zonder[\s_-]+namen",
        r"zonder[\s_-]+handtekeningen",
        r"niet[\s_-]+ondertekend",
    ],
    "filed_szw": [
        _fn_token("aanmelding"),
        _fn_token("aangemeld"),
        _fn_token("SZW"),
        _fn_token("TTW"),
    ],
    "draft": [
        _fn_token("concept"),
        _fn_token("draft"),
        r"voorlopig(?:e)?(?![a-zA-Z])",
        _fn_token("tussenstand"),
    ],
    "negotiation_result": [
        r"onderhandelingsresultaat",
        r"principeakkoord",
        r"akkoord[\s_-]+op[\s_-]+hoofdlijnen",
        r"cao[\s_-]?akkoord",
    ],
    "consolidated_amendments": [
        r"wijzigingen[\s_-]+geaccepteerd",
        r"wijzigingen[\s_-]+doorgevoerd",
        _fn_token("integraal"),
        r"geconsolideerd",
    ],
    "avv_declared": [
        _fn_token("AVV"),
        r"algemeen[\s_-]+verbindend",
    ],
}

CONTENT_PATTERNS: Dict[str, List[str]] = {
    "szw_stamp": [
        r"ONTVANGEN[^\n]{0,80}\d{4}",
        r"Postbus[\s]+Aanmelden",
        r"Aanmeldingsformulier",
        r"Ministerie[\s]+van[\s]+Sociale[\s]+Zaken",
        r"(?i)\bttw\s+aanmelding\b",
        r"(?i)\baanmelding\b[^\n]{0,30}\bttw\b",
    ],
    "signature_block": [
        r"Aldus[\s]+overeengekomen",
        r"Aldus[\s]+ondertekend",
        r"namens[\s]+(de[\s]+)?partij(en)?",
        r"Was getekend",
        r"Getekend[\s]+te",
        r"De[\s]+ondergetekenden",
        r"plaats[\s]+en[\s]+datum",
        r"ondergetekenden",
        r"voor[\s]+zover[\s]+ondertekend",
        r"door[\s]+ondertekening",
        r"met[\s]+handtekening",
    ],
    # Title-only cues. Do not match in-body phrases such as "Met dit onderhandelingsresultaat
    # hebben partijen …" on filed CAO text (see mismatch rule A spot-checks).
    "negotiation_header": [
        r"^akkoord op hoofdlijnen\b",
    ],
    "definitive_header": [
        r"definitieve[\s]+tekst",
        r"definitieve[\s]+versie",
        r"vastgesteld[\s]+op",
        r"totale[\s]+tekst",
        r"cao[\s-]?tekst",
        # Bouw & Infra (and similar): running page header "… 26 oktober 2018  (def)"
        r"\(def\)",
        r"\(DEF\)",
        r"cao[^\n]{0,160}\(def\)",
        r"tussentijdse[\s]+wijziging",
        r"Wijziging[\s]+per",
        r"Wijziging\s+\d",
        # Typical CAO title / cover block (first pages)
        r"collectieve\s+arbeidsovereenkomst",
        r"COLLECTIEVE\s+ARBEIDSOVEREENKOMST",
        r"\d{1,2}\s+\w+(\s+\w+)?\s+\d{4}\s+tot\s+en\s+met\s+\d{1,2}",
        r"geldt\s+vanaf",
        r"werkt\s+vanaf",
        r"ingaande?\s+op",
        # Published interactive / reader versions (still final issued text, not negotiation)
        r"LEESWIJZER",
        r"INTERACTIEF",
        r"Doorlopende\s+tekst",
    ],
    "draft_header": [
        r"^concept(versie)?\b",
        r"^voorlopige[\s]+tekst",
        r"\(concept\)",
        r"\(voorlopig\)",
        r"\(CONCEPT\)",
        r"conceptversie",
    ],
    "avv_decision": [
        r"algemeen[\s]+verbindend[\s]+verklaard",
        r"\bStaatscourant\b",
        r"Besluit[\s]+van[\s]+de[\s]+Minister[\s]+van[\s]+(SZW|Sociale[\s]+Zaken)",
    ],
    "consolidated_marker": [
        r"wijzigingen[\s]+geaccepteerd",
        r"geconsolideerde[\s]+versie",
        r"wijzigingen[\s]+doorgevoerd",
    ],
}


def _compile_dict(patterns: Dict[str, List[str]]) -> Dict[str, List[re.Pattern[str]]]:
    """Compile each pattern string with IGNORECASE; one list of Pattern per cue key."""
    return {
        key: [re.compile(p, re.IGNORECASE) for p in plist]
        for key, plist in patterns.items()
    }


FILENAME_PATTERNS_RE: Dict[str, List[re.Pattern[str]]] = _compile_dict(FILENAME_PATTERNS)
CONTENT_PATTERNS_RE: Dict[str, List[re.Pattern[str]]] = _compile_dict(CONTENT_PATTERNS)
