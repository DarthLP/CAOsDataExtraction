"""fb6_triage.py — fast automated presence-check of fixer_evidence quotes against each record's
own full extract JSON (all 13 sections). Flags rows where the cited evidence text does NOT
appear verbatim anywhere in the record's own document -- a strong prior signal for FLIP
(cross-document citation / unsupported), to prioritize manual review.
Read-only. Does not decide verdicts by itself -- output is a triage hint only.
"""
import csv, os, re, sys, json, difflib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import source_lookup as sl

WORKLIST = "/private/tmp/claude-501/-Users-lorenzpiazolo-Documents-Claude-Projects-Dutch-Bargaining-Agreements/6c4c5066-ca8d-4723-82f4-3ad058918cf4/scratchpad/fb_batch_6.tsv"
OUT = "/private/tmp/claude-501/-Users-lorenzpiazolo-Documents-Claude-Projects-Dutch-Bargaining-Agreements/6c4c5066-ca8d-4723-82f4-3ad058918cf4/scratchpad/fb6_triage.tsv"


def extract_quotes(evidence):
    """Pull double-quoted substrings out of the fixer_evidence field."""
    # normalize curly quotes
    s = evidence.replace('“', '"').replace('”', '"')
    quotes = re.findall(r'"([^"]{15,})"', s)
    if not quotes:
        # fallback: use the whole evidence string, stripped
        quotes = [s.strip("'\" ")]
    return quotes


def norm(s):
    s = s.lower()
    s = re.sub(r'\s+', ' ', s)
    s = re.sub(r'[^a-z0-9%.,\- ]', '', s)
    return s.strip()


def check(cao, file_name, evidence):
    d = sl.full_extract(cao, file_name)
    if not d:
        return "NO_EXTRACT", ""
    blob = norm(json.dumps(d, ensure_ascii=False))
    quotes = extract_quotes(evidence)
    results = []
    for q in quotes:
        nq = norm(q)
        if len(nq) < 10:
            continue
        # try full quote, then a core 40-char window
        if nq in blob:
            results.append((q, True, 1.0))
            continue
        # sliding best-match ratio against blob in chunks (cheap heuristic: check 60-char windows)
        core = nq[:80]
        if core in blob:
            results.append((q, True, 1.0))
            continue
        # fuzzy: check if most words of the quote appear close together
        words = [w for w in nq.split() if len(w) > 4]
        hits = sum(1 for w in words if w in blob)
        ratio = hits / max(1, len(words))
        results.append((q, ratio > 0.7, ratio))
    if not results:
        return "NO_QUOTES", ""
    best = max(results, key=lambda r: r[2])
    return ("PRESENT" if best[1] else "ABSENT"), f"best_ratio={best[2]:.2f} q={best[0][:60]!r}"


def main():
    with open(WORKLIST, encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    with open(OUT, "w", encoding="utf-8") as out:
        out.write("idx\trecord_id\tfield\tstatus\tdetail\n")
        for i, row in enumerate(rows, start=1):
            status, detail = check(row["cao"], row["file_name"], row["fixer_evidence"])
            out.write(f"{i}\t{row['record_id']}\t{row['field']}\t{status}\t{detail}\n")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
