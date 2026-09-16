"""Source-text resolver for the full-audit verification stage.

Two source corpora, both ENGLISH (the upstream extractor translated the Dutch
CAOs and we verify against that translation, consistent with the rest of qa/):

  - IN-SCOPE (95 curated CAOs), 13 content topics:
        qa.shared.source_text_loader.load_source_text(topic, rid)
        → the curated `inputs/by_topic/<topic>_information.md` block.

  - OUT-OF-SCOPE (the other CAOs) + the `general` block + `meta`:
        CAOsDataExtraction/outputs/llm_extracted/new_flow/<cao>/<file>_extract.json
        → the same English extraction the by_topic files were built from.
        READ-ONLY (hard rule: never write under CAOsDataExtraction/).

The extract JSON is a dict whose keys are the schema prefixes
(`leave_information`, `termination_information`, …) and whose values are
list-of-list-of-strings passages — we flatten them to a text block that mirrors
the by_topic layout, so the verification prompt is uniform across both corpora.
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from qa.full_audit import common
from qa.shared import source_text_loader

EXTRACT_ROOT = Path(
    "/Users/lorenzpiazolo/Documents/Python/CAOsDataExtraction"
    "/outputs/llm_extracted/new_flow"
)
try:
    from repo_paths import LLM_EXTRACTED_DIR
    EXTRACT_ROOT = LLM_EXTRACTED_DIR
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from repo_paths import LLM_EXTRACTED_DIR
    EXTRACT_ROOT = LLM_EXTRACTED_DIR

# topic -> top-level key inside an *_extract.json (and the by_topic filename stem)
_EXTRACT_KEY = {
    t: fn[:-3] if fn.endswith(".md") else fn
    for t, fn in source_text_loader.TOPIC_TO_FILENAME.items()
}
_EXTRACT_KEY["general"] = "general_information"
_EXTRACT_KEY["meta"] = "general_information"   # meta date fields ↔ general block


def _norm_fn(fn: str) -> str:
    return source_text_loader._normalize_filename(fn)


@lru_cache(maxsize=None)
def _extract_index(cao: str) -> dict[str, Path]:
    """For one CAO dir, {normalized_filename_stem -> extract.json path}."""
    d = EXTRACT_ROOT / str(cao)
    out: dict[str, Path] = {}
    if not d.is_dir():
        return out
    for p in d.glob("*_extract.json"):
        stem = p.name[: -len("_extract.json")]
        out[_norm_fn(stem)] = p
    return out


def _flatten(section) -> str:
    lines: list[str] = []

    def rec(x):
        if isinstance(x, str):
            lines.append(x)
        elif isinstance(x, list):
            for e in x:
                rec(e)
    rec(section)
    return "\n".join(lines)


def _match_extract(cao: str, file_name: str) -> Path | None:
    idx = _extract_index(str(cao))
    if not idx:
        return None
    nf = _norm_fn(file_name)
    if nf in idx:
        return idx[nf]
    # tolerant fallback: unique containment either direction (handles a trailing
    # ".pdf" baked into the stem, "_1"/"_2" disambiguators, stray spaces, etc.)
    cands = [p for k, p in idx.items() if k and (nf in k or k in nf)]
    if len(cands) == 1:
        return cands[0]
    return None


@lru_cache(maxsize=4096)
def _extract_section(cao: str, file_name: str, topic: str) -> str:
    key = _EXTRACT_KEY.get(topic)
    if not key:
        return ""
    p = _match_extract(cao, file_name)
    if p is None:
        return ""
    try:
        o = json.loads(p.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return ""
    return _flatten(o.get(key, []))


def resolve(topic: str, rid: str, cao: str, file_name: str) -> tuple[str, str]:
    """Return (source_text, origin). origin ∈ {in_scope_by_topic,
    out_of_scope_extract, no_source}."""
    inscope = str(cao) in common.inscope_caos()
    # in-scope content topics → curated by_topic block
    if inscope and topic in source_text_loader.TOPIC_TO_FILENAME:
        txt = source_text_loader.load_source_text(topic, rid)
        if txt.strip():
            return txt, "in_scope_by_topic"
    # everything else (out-of-scope, general, meta) → new_flow extract
    txt = _extract_section(str(cao), file_name, topic)
    if txt.strip():
        return txt, "out_of_scope_extract"
    return "", "no_source"


def clear_cache() -> None:
    _extract_index.cache_clear()
    _extract_section.cache_clear()
    source_text_loader.clear_cache()
