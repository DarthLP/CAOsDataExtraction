"""fb6_dump.py — dump per-row source context for the field-boundary re-verification batch 6.
Read-only helper. For each worklist row: pulls the full extract JSON via source_lookup, writes
the PRIMARY topic section in full plus any OTHER sections referenced by name in fixer_reason/
fixer_evidence, plus a keyword sweep across all 13 sections for extra corroboration/contradiction.
Output: one text file per row in scratchpad/fb6_dumps/<row_idx>_<record_id>_<field>.txt
"""
import csv, os, re, sys, json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import source_lookup as sl

WORKLIST = "/private/tmp/claude-501/-Users-lorenzpiazolo-Documents-Claude-Projects-Dutch-Bargaining-Agreements/6c4c5066-ca8d-4723-82f4-3ad058918cf4/scratchpad/fb_batch_6.tsv"
OUTDIR = "/private/tmp/claude-501/-Users-lorenzpiazolo-Documents-Claude-Projects-Dutch-Bargaining-Agreements/6c4c5066-ca8d-4723-82f4-3ad058918cf4/scratchpad/fb6_dumps"

PREFIX_TO_SECTION = {
    "bonus_": "wage_information",
    "contract_": "contract_type_information",
    "fringe_": "fringe_benefits_information",
    "leave_": "leave_information",
    "overtime_": "overtime_information",
    "pension_": "pension_information",
    "safety_": "safety_information",
    "term_": "termination_information",
    "training_": "training_information",
    "homeoffice_": "homeoffice_information",
    "childcare_": "childcare_information",
    "ai_": "AI_information",
    "general_": "general_information",
}

ALL_SECTIONS = [
    "general_information", "wage_information", "pension_information", "leave_information",
    "termination_information", "overtime_information", "training_information",
    "homeoffice_information", "contract_type_information", "fringe_benefits_information",
    "safety_information", "childcare_information", "AI_information",
]

STOP = set("""the a an of to for in on and or with is are be by as at this that these those
own document extract own's just filed under instead misfiled mis-bucketed mis-filed field
same passage clause states explicit explicitly current blank literal literally figure value
text matches matching present unsupported reads reading own text""".split())


def primary_section(field):
    for pfx, sec in PREFIX_TO_SECTION.items():
        if field.startswith(pfx):
            return sec
    return None


def keywords(s, minlen=6, maxn=12):
    words = re.findall(r"[A-Za-z][A-Za-z\-']{%d,}" % (minlen - 1), s)
    out = []
    for w in words:
        lw = w.lower()
        if lw in STOP:
            continue
        if lw not in out:
            out.append(lw)
        if len(out) >= maxn:
            break
    return out


def dump_row(i, row):
    record_id = row["record_id"]
    cao = row["cao"]
    file_name = row["file_name"]
    field = row["field"]
    d = sl.full_extract(cao, file_name)
    lines = []
    lines.append(f"record_id={record_id} cao={cao} field={field} layer={row['layer']}")
    lines.append(f"file_name={file_name}")
    lines.append(f"pre_value={row['pre_value']!r} current_value={row['current_value']!r}")
    lines.append(f"fixer_reason={row['fixer_reason']}")
    lines.append(f"fixer_evidence={row['fixer_evidence']}")
    lines.append("")
    if not d:
        lines.append("!!! NO EXTRACT FOUND for this (cao, file_name) via source_lookup !!!")
        return "\n".join(lines)

    psec = primary_section(field)
    lines.append(f"=== PRIMARY SECTION: {psec} ===")
    pv = d.get(psec) or []
    if not pv:
        lines.append("(empty/absent)")
    else:
        for j, item in enumerate(pv):
            lines.append(f"[{j}] {item}")
    lines.append("")

    # sections explicitly named in fixer_reason/evidence
    blob = (row["fixer_reason"] or "") + " " + (row["fixer_evidence"] or "")
    named = []
    for sec in ALL_SECTIONS:
        if sec == psec:
            continue
        short = sec.replace("_information", "")
        if sec in blob or short in blob:
            named.append(sec)
    if named:
        lines.append(f"=== NAMED-IN-EVIDENCE SECTIONS: {named} ===")
        for sec in named:
            v = d.get(sec) or []
            lines.append(f"--- {sec} ---")
            if not v:
                lines.append("(empty/absent)")
            else:
                for j, item in enumerate(v):
                    lines.append(f"[{j}] {item}")
        lines.append("")

    # keyword sweep across all OTHER sections not already dumped
    kws = keywords(blob)
    dumped = set([psec] + named)
    hits = {}
    for sec in ALL_SECTIONS:
        if sec in dumped:
            continue
        v = d.get(sec) or []
        for j, item in enumerate(v):
            text = str(item).lower()
            for kw in kws:
                if kw in text:
                    hits.setdefault(sec, []).append((j, item))
                    break
    if hits:
        lines.append(f"=== KEYWORD-SWEEP HITS (kws={kws}, truncated to 260 chars/passage) ===")
        for sec, items in hits.items():
            lines.append(f"--- {sec} ---")
            for j, item in items:
                s = str(item)
                if len(s) > 260:
                    s = s[:260] + " …[TRUNCATED]"
                lines.append(f"[{j}] {s}")
        lines.append("")
    else:
        lines.append(f"=== KEYWORD-SWEEP: no hits in other sections (kws={kws}) ===")

    # list all section keys present, with passage counts, for completeness-of-scan visibility
    lines.append("=== SECTION INVENTORY (all 13) ===")
    for sec in ALL_SECTIONS:
        v = d.get(sec) or []
        lines.append(f"{sec}: {len(v)} passages")
    return "\n".join(lines)


def main():
    os.makedirs(OUTDIR, exist_ok=True)
    with open(WORKLIST, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows = list(reader)
    for i, row in enumerate(rows, start=1):
        text = dump_row(i, row)
        fname = f"{i:03d}_{row['record_id']}_{row['field']}.txt"
        with open(os.path.join(OUTDIR, fname), "w", encoding="utf-8") as out:
            out.write(text)
    print(f"Dumped {len(rows)} rows to {OUTDIR}")


if __name__ == "__main__":
    main()
