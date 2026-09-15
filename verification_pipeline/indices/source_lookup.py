"""source_lookup.py — resolve a (cao_number, file_name) to its raw LLM-extraction JSON in the
new_flow folder, and pull topic passages. This is the CANONICAL verification source (2026-07-08):
it covers ALL 242 CAOs (the curated inputs/by_topic/ files only cover 95).

new_flow layout:  <SRC>/<cao_number>/<file-stem>_extract.json
  — one JSON per document, a dict of 13 topic keys -> list of English-translated passages.

Matching is by cao_number FOLDER + normalised filename, so files that share a name under
DIFFERENT cao numbers never collide (Hanna's requirement). READ-ONLY — never writes to SRC.
"""
import os, glob, re, json
from pathlib import Path

try:
    from repo_paths import LLM_EXTRACTED_DIR
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from repo_paths import LLM_EXTRACTED_DIR

SRC = str(LLM_EXTRACTED_DIR)

# index-topic -> primary extract-JSON key (agents may also scan the whole doc)
TOPIC_KEY = {
    "leave": "leave_information", "absence": "leave_information",
    "term": "termination_information", "bonus": "wage_information",
    "contract": "contract_type_information", "overtime": "overtime_information",
    "training": "training_information", "pension": "pension_information",
    "fringe": "fringe_benefits_information", "homeoffice": "homeoffice_information",
    "safety": "safety_information", "childcare": "childcare_information",
    "ai": "AI_information", "wage": "wage_information", "general": "general_information",
}

def _norm(s): return re.sub(r"[^a-z0-9]", "", str(s).lower())

_CACHE = {}
def _folder(cao):
    """norm-key -> LIST of paths. Distinct filenames can normalise identically (underscore/space/
    hyphen count differences — 9 real collision groups found 2026-07-09); keeping a list lets
    resolve() disambiguate by raw-string closeness instead of silently shadowing one file."""
    cao = str(cao)
    if cao not in _CACHE:
        d = os.path.join(SRC, cao); m = {}
        if os.path.isdir(d):
            for f in glob.glob(d + "/*_extract.json"):
                m.setdefault(_norm(os.path.basename(f)[:-len("_extract.json")]), []).append(f)
        _CACHE[cao] = m
    return _CACHE[cao]


def _pick(paths, want_raw):
    """Among same-norm-key paths, pick the one whose RAW basename is closest to the requested
    raw file_name (difflib ratio). Single candidate -> itself."""
    if len(paths) == 1:
        return paths[0]
    import difflib
    def score(p):
        stem = os.path.basename(p)[:-len("_extract.json")]
        return difflib.SequenceMatcher(None, stem, want_raw).ratio()
    return max(paths, key=score)

_DOC_EXT = {".pdf", ".docx", ".doc", ".txt", ".rtf"}

def resolve(cao, file_name):
    """Absolute path to the extract JSON for this (cao, file_name), or None. Matches on the
    cao_number folder + normalised filename: exact first, then RANKED partial matches.
    FIXED 2026-07-09 (caught by a second-reader campaign): (a) only strip REAL document
    extensions — os.path.splitext used to eat legitimate suffixes like '…2024.def', breaking
    the exact match; (b) partial matching used to be first-match-wins in glob order, which
    silently returned a DIFFERENT EDITION when several filenames share a long prefix (e.g.
    NBBU_CAO_Uitzendkrachten 2015 vs …-NL 2024.def). Now all candidates are scored and the
    LONGEST/CLOSEST match wins; ties by smallest total-length difference."""
    s = str(file_name).strip()
    root, ext = os.path.splitext(s)
    stem = root if ext.lower() in _DOC_EXT else s
    idx = _folder(cao)
    n_full = _norm(s)                                # keeps a real extension (…docx) — some extract
    if n_full in idx:                                # stems include it, so try the exact FULL name first
        return _pick(idx[n_full], s)
    n = _norm(stem)
    if n in idx:
        return _pick(idx[n], s)
    best, best_score = None, (0, -10**9)
    for k, paths in idx.items():
        if k.startswith(n) or n.startswith(k):
            overlap = min(len(k), len(n))            # length of the agreed prefix
        elif len(n) > 15 and n[:20] in k:
            overlap = 20
        else:
            continue
        score = (overlap, -abs(len(k) - len(n)))     # longest overlap, then closest total length
        if score > best_score:
            best, best_score = _pick(paths, s), score
    return best

def full_extract(cao, file_name):
    """The whole extract dict for one document (or {} if unmatched/unreadable)."""
    p = resolve(cao, file_name)
    if not p:
        return {}
    try:
        return json.load(open(p))
    except Exception:
        return {}

def passages(cao, file_name, topic):
    """Passages for a topic in one document (primary key, falling back to general)."""
    d = full_extract(cao, file_name)
    key = TOPIC_KEY.get(topic, topic if str(topic).endswith("_information") else str(topic) + "_information")
    v = d.get(key) or d.get("general_information") or []
    return v if isinstance(v, list) else [v]


if __name__ == "__main__":  # coverage self-test against the dataset
    import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import index_lib as il
    df = il.load_full_cao(); full = df[df["file_name"].notna()]
    hit = sum(1 for _, r in full.iterrows() if resolve(r["cao_number"], r["file_name"]))
    print(f"source_lookup coverage: {hit}/{len(full)} = {hit/len(full):.1%} of full-CAO records; "
          f"{df['cao_number'].nunique()} CAOs")
