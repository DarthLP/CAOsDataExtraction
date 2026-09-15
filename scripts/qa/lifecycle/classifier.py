"""
Deterministic lifecycle classification for one parsed CAO markdown file.

Reads filename stem plus bounded head/tail of the markdown body (after the
standard p2 header), applies regex cues from ``patterns.py``, and returns
``lifecycle_stage``, boolean flags, comma-separated evidence, and evidence source.

Public API:
    classify_file(cao_number, file_path) -> dict[str, object]

Parameters:
    cao_number: CAO folder id (string); echoed for callers that batch-walk folders.
    file_path: Path to ``*.md`` under ``parsed_pdfs_markdown``.

Returns:
    Dict with keys: ``lifecycle_stage``, ``is_signed``, ``is_filed_szw``,
    ``is_avv_declared``, ``lifecycle_evidence``, ``lifecycle_source``.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Literal, Tuple

from .patterns import FILENAME_PATTERNS_RE, CONTENT_PATTERNS_RE

ContentMode = Literal["full", "filename_only"]

# Strip p2 ``save_as_markdown`` banner (see ``pipelines/p2_extract.save_as_markdown``).
_MD_HEADER_RE = re.compile(
    r"^# CAO Document - Extracted Content\s*"
    r"\n\s*\*Source:[^\n]*\*\s*"
    r"\n\s*---\s*\n\s*",
    re.MULTILINE | re.IGNORECASE,
)

_EVIDENCE_MAX_LEN = 500
_SNIPPET_MAX = 60
_HEAD_MAX_BYTES = 400_000
_BODY_SAMPLE_MAX_CHARS = 300_000


def _read_first_lines(path: Path, max_lines: int = 200) -> List[str]:
    """
    Read up to ``max_lines`` lines from the start of the file without scanning the whole file.

    Reads at most ``_HEAD_MAX_BYTES`` from the beginning so a single huge OCR line cannot
    force reading multi-megabyte files line-by-line.

    Parameters:
        path: Text file path.
        max_lines: Cap on lines returned from the beginning.

    Returns:
        List of lines without trailing newline characters.
    """
    with path.open("rb") as f:
        raw = f.read(_HEAD_MAX_BYTES)
    text = raw.decode("utf-8", errors="replace")
    return [ln.rstrip("\r") for ln in text.splitlines()[:max_lines]]


def _read_tail_lines(path: Path, max_lines: int = 80, chunk_size: int = 262_144) -> List[str]:
    """
    Read up to ``max_lines`` physical lines from the end of the file without a full scan.

    Reads backwards in binary chunks until enough newline-separated rows are
    captured, then decodes as UTF-8 with replacement. For tiny files, reads the
    whole file once.

    Parameters:
        path: Text file path.
        max_lines: Maximum number of lines to return from the file suffix.
        chunk_size: Binary chunk size when seeking from EOF.

    Returns:
        List of up to ``max_lines`` lines (no trailing newline chars), in file order.
    """
    max_total_bytes = 2_000_000
    with path.open("rb") as f:
        f.seek(0, 2)
        size = f.tell()
        if size == 0:
            return []
        data = b""
        pos = size
        newline_total = 0
        # Collect enough bytes to contain max_lines line breaks (plus partial first line)
        while pos > 0 and newline_total < max_lines + 1 and len(data) < max_total_bytes:
            step = min(chunk_size, pos)
            pos -= step
            f.seek(pos)
            chunk = f.read(step)
            newline_total += chunk.count(b"\n")
            data = chunk + data
    text = data.decode("utf-8", errors="replace")
    lines = text.splitlines()
    return [ln.rstrip("\r") for ln in lines[-max_lines:]]


def _read_bounded_lines(path: Path) -> Tuple[List[str], List[str]]:
    """
    Read at most the first 200 and last 80 physical lines of a text file.

    The tail is recovered via bounded reads from EOF so large markdown files are
    not scanned line-by-line in full.

    Parameters:
        path: UTF-8 text file.

    Returns:
        (first_up_to_200, last_up_to_80) each line without trailing newline.
    """
    first = _read_first_lines(path, 200)
    last = _read_tail_lines(path, 80)
    return first, last


def _non_empty(lines: List[str]) -> List[str]:
    """Return lines with stripped content non-empty."""
    return [ln for ln in lines if ln.strip()]


def _truncate_evidence(s: str) -> str:
    """Cap evidence string length with ellipsis."""
    if len(s) <= _EVIDENCE_MAX_LEN:
        return s
    return s[: _EVIDENCE_MAX_LEN - 1] + "…"


def _match_filename(stem: str) -> Tuple[Dict[str, bool], List[str]]:
    """
    Run all filename pattern groups against stem.

    Returns:
        (flags_by_group, evidence_parts) where evidence uses ``filename:<group>``.
    """
    hits: Dict[str, bool] = {}
    evidence: List[str] = []
    for group, regexes in FILENAME_PATTERNS_RE.items():
        matched = False
        for rx in regexes:
            if rx.search(stem):
                matched = True
                break
        hits[group] = matched
        if matched:
            evidence.append(f"filename:{group}")
    return hits, evidence


def _first_match_snippet(text: str, rx: re.Pattern[str]) -> str | None:
    """Return up to _SNIPPET_MAX chars of the first match, whitespace-collapsed."""
    m = rx.search(text)
    if not m:
        return None
    snippet = m.group(0).strip()
    if len(snippet) > _SNIPPET_MAX:
        snippet = snippet[: _SNIPPET_MAX - 1] + "…"
    return snippet


def _match_content_body_sample(
    body_sample: str, evidence_parts: List[str]
) -> Dict[str, bool]:
    """Apply content patterns that are searched on the combined body_sample."""
    hits: Dict[str, bool] = {}
    line_only_groups = frozenset({"negotiation_header", "draft_header"})
    for group, regexes in CONTENT_PATTERNS_RE.items():
        if group in line_only_groups:
            continue
        matched = False
        for rx in regexes:
            snip = _first_match_snippet(body_sample, rx)
            if snip is not None:
                matched = True
                evidence_parts.append(f"content:{group}:{snip}")
                break
            if rx.search(body_sample):
                matched = True
                evidence_parts.append(f"content:{group}")
                break
        hits[group] = matched
    return hits


def _combine_lifecycle(
    fn_hits: Dict[str, bool],
    content_hits: Dict[str, bool],
    evidence: List[str],
) -> Dict[str, Any]:
    """
    Apply §6.3 combining rules from filename and content hit dicts.

    Parameters:
        fn_hits: Filename pattern groups that matched.
        content_hits: Content pattern groups that matched.
        evidence: Mutable evidence token list (filename + content entries).

    Returns:
        Lifecycle result dict (six keys).
    """
    def any_fn(groups: List[str]) -> bool:
        return any(fn_hits.get(g, False) for g in groups)

    def any_content(groups: List[str]) -> bool:
        return any(content_hits.get(g, False) for g in groups)

    is_signed = any_fn(["signed"]) or any_content(["signature_block"])
    if any_fn(["unsigned_negation"]):
        is_signed = False
    is_filed_szw = any_fn(["filed_szw"]) or any_content(["szw_stamp"])
    is_avv_declared = any_fn(["avv_declared"]) or any_content(["avv_decision"])

    definitive_hit = any_fn(["definitive"]) or any_content(["definitive_header"])
    draft_hit = any_fn(["draft"]) or any_content(["draft_header"])
    negotiation_hit = any_fn(["negotiation_result"]) or any_content(["negotiation_header"])
    consolidated_hit = any_fn(["consolidated_amendments"]) or any_content(["consolidated_marker"])

    # Filed / aanmelding PDFs carry final operative text; historical "principeakkoord"
    # or in-intro "onderhandelingsresultaat" must not downgrade them to negotiation.
    if negotiation_hit and not definitive_hit and not is_filed_szw:
        lifecycle_stage = "negotiation_result"
    elif draft_hit and not definitive_hit:
        lifecycle_stage = "draft"
    elif consolidated_hit:
        lifecycle_stage = "consolidated_amendments"
    elif definitive_hit and is_signed:
        lifecycle_stage = "definitive_signed"
    elif definitive_hit and not is_signed:
        lifecycle_stage = "definitive_unsigned"
    elif is_signed and not definitive_hit:
        lifecycle_stage = "definitive_signed"
    elif is_filed_szw and not draft_hit and not negotiation_hit:
        # Aanmelding / TTW filings usually contain the operative CAO text (plan §6.3 note).
        lifecycle_stage = "definitive_unsigned"
    else:
        lifecycle_stage = "unspecified"

    filename_fired = any(fn_hits.values())
    content_fired = any(content_hits.values())
    if filename_fired and content_fired:
        lifecycle_source = "both"
    elif filename_fired:
        lifecycle_source = "filename"
    elif content_fired:
        lifecycle_source = "content"
    else:
        lifecycle_source = "none"

    evidence_str = ", ".join(evidence)
    if not evidence_str and lifecycle_stage == "unspecified" and not (
        is_signed or is_filed_szw or is_avv_declared
    ):
        evidence_str = ""
    elif not evidence_str:
        evidence_str = "no_cues_recorded"

    evidence_str = _truncate_evidence(evidence_str)

    return {
        "lifecycle_stage": lifecycle_stage,
        "is_signed": bool(is_signed),
        "is_filed_szw": bool(is_filed_szw),
        "is_avv_declared": bool(is_avv_declared),
        "lifecycle_evidence": evidence_str,
        "lifecycle_source": lifecycle_source,
    }


def _match_content_head_lines(
    head_nonempty: List[str], evidence_parts: List[str]
) -> Dict[str, bool]:
    """
    Apply line-anchored patterns on the first cover lines (negotiation, draft, definitief).

    Cover lines are short title/header rows where ``definitief`` at end-of-line is a
    strong signal (avoids many in-body ``definitie`` false positives).
    """
    hits = {
        "negotiation_header": False,
        "draft_header": False,
        "definitive_header": False,
    }
    top = head_nonempty[:40]
    line_groups = ("negotiation_header", "draft_header")
    for group in line_groups:
        for rx in CONTENT_PATTERNS_RE[group]:
            for ln in top:
                if rx.search(ln):
                    hits[group] = True
                    snip = ln.strip()
                    if len(snip) > _SNIPPET_MAX:
                        snip = snip[: _SNIPPET_MAX - 1] + "…"
                    evidence_parts.append(f"content:{group}:{snip}")
                    break
            if hits[group]:
                break

    # "… definitief" on a short line (e.g. Bouw cover header)
    definitief_eol = re.compile(
        r"(?i)(?:d\.d\.|dd\.?|datum)?\s*.{0,80}\bdefinitief\s*\.?\s*$"
    )
    for ln in top:
        s = ln.strip()
        if len(s) > 160:
            continue
        if definitief_eol.search(s) or re.search(r"(?i)\bdefinitief\s*[\.\)]?\s*$", s):
            hits["definitive_header"] = True
            snip = s if len(s) <= _SNIPPET_MAX else s[: _SNIPPET_MAX - 1] + "…"
            evidence_parts.append(f"content:definitive_header:{snip}")
            break

    def_count = sum(1 for ln in head_nonempty if re.search(r"\(def\)", ln, re.IGNORECASE))
    if def_count >= 2:
        hits["definitive_header"] = True
        evidence_parts.append(f"content:definitive_header:(def)x{def_count}")

    return hits


def classify_file(
    cao_number: str,
    file_path: Path,
    *,
    content_mode: ContentMode = "full",
) -> Dict[str, Any]:
    """
    Classify lifecycle fields from filename and optionally parsed markdown snippets.

    Parameters:
        cao_number: CAO identifier (unused in logic; kept for API symmetry).
        file_path: Path to markdown file.
        content_mode: ``full`` reads bounded head/tail and applies content regexes
            (original plan). ``filename_only`` uses filename tokens only when any
            filename group matches — no disk read (for cloud placeholders / quick QA).

    Returns:
        Dict with keys ``lifecycle_stage``, ``is_signed``, ``is_filed_szw``,
        ``is_avv_declared``, ``lifecycle_evidence``, ``lifecycle_source``.
    """
    _ = cao_number
    missing = {
        "lifecycle_stage": "unspecified",
        "is_signed": False,
        "is_filed_szw": False,
        "is_avv_declared": False,
        "lifecycle_evidence": "file_empty_or_missing",
        "lifecycle_source": "none",
    }
    stem = file_path.stem
    fn_hits, evidence = _match_filename(stem)

    if content_mode == "filename_only" and any(fn_hits.values()):
        return _combine_lifecycle(fn_hits, {}, list(evidence))

    if not file_path.is_file():
        return missing.copy()

    try:
        first_lines, last_lines = _read_bounded_lines(file_path)
    except OSError:
        return missing.copy()

    if not first_lines and not last_lines:
        return missing.copy()

    head_raw = "\n".join(first_lines)
    head_raw = _MD_HEADER_RE.sub("", head_raw, count=1)
    head_nonempty = _non_empty(head_raw.splitlines())
    tail_nonempty = _non_empty(last_lines)
    tail_nonempty = tail_nonempty[-40:] if len(tail_nonempty) > 40 else tail_nonempty
    head_nonempty = head_nonempty[:80]

    head_text = "\n".join(head_nonempty)
    tail_text = "\n".join(tail_nonempty)
    body_sample = f"{head_text}\n{tail_text}".strip()
    if len(body_sample) > _BODY_SAMPLE_MAX_CHARS:
        body_sample = body_sample[:_BODY_SAMPLE_MAX_CHARS]

    content_body_hits = _match_content_body_sample(body_sample, evidence)
    content_line_hits = _match_content_head_lines(head_nonempty, evidence)
    all_keys = set(content_body_hits) | set(content_line_hits)
    content_hits = {
        k: bool(content_body_hits.get(k)) or bool(content_line_hits.get(k)) for k in all_keys
    }

    return _combine_lifecycle(fn_hits, content_hits, evidence)
