"""p0: fetch earlier AVV (algemeen verbindendverklaring) CAO PDFs from officielebekendmakingen.nl.

Stages (subcommands); catalogue state lives in ``--work``, selection artifacts in
``pipelines/p0_avv_selection/`` (see its README for the LLM steps between the stages):
  catalog         download metadata of every Staatscourant AVV publication via the SRU API -> catalog.json
  candidates      per in-scope code: name keywords + LLM name variants + LLM subject map -> candidates.json
  select          merge LLM classifications, apply cutoff, conflicts and removals -> selection.json, unsure.json
  download        fetch selected PDFs ({code: [identifier, ...]}) into --dest/<code>/<id>.pdf
  verify-batches  first-page text of downloaded PDFs -> batch files for the verification agent
  merge           amendments + the decision they amend -> <code>/merged/<base>_merged.pdf, merged.json
  write-excel     append rows to a sheet of SectoralCAOs_Update.xlsx and set Overview "Earlier Start Update"

Existing files are never overwritten or deleted.
"""

import argparse
import datetime as dt
import json
import re
import time
import unicodedata
import urllib.parse
import urllib.request
from pathlib import Path

SRU = "https://repository.overheid.nl/sru"
AVV_TYPE = "Algemeenverbindendverklaring van CAO-bepalingen"
PAGE = 1000
# Publications of the AVV type that are not an AVV decision itself.
NON_AVV_TITLE = re.compile(r"verzoek|intrekking|aanmelding", re.I)
MONTHS = {m: i for i, m in enumerate(
    "januari februari maart april mei juni juli augustus september oktober november december".split(), 1)}


def sru(query: str, start: int) -> str:
    url = f"{SRU}?httpAccept=application/xml&maximumRecords={PAGE}&startRecord={start}&query={urllib.parse.quote(query)}"
    for attempt in range(5):
        try:
            return urllib.request.urlopen(url, timeout=180).read().decode()
        except Exception:
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"SRU failed: {url}")


def tag(rec: str, name: str) -> str:
    m = re.search(rf"<{name}(?: [^>]*)?>([^<]*)</{name}>", rec)
    return unescape(m.group(1)) if m else ""


def unescape(s: str) -> str:
    return s.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"').replace("&apos;", "'")


def cmd_catalog(a):
    query = f'c.product-area==officielepublicaties AND w.publicatienaam==Staatscourant AND dt.type="{AVV_TYPE}"'
    recs, start = [], 1
    while start:
        page = sru(query, start)
        for r in page.split("<sru:record>")[1:]:
            recs.append({
                "id": tag(r, "dcterms:identifier"),
                "title": tag(r, "dcterms:title"),
                "alt": tag(r, "dcterms:alternative"),
                "date": tag(r, "dcterms:date") or tag(r, "dcterms:available"),
                "creator": tag(r, "dcterms:creator"),
                "jaargang": tag(r, "overheidwetgeving:jaargang"),
                "nummer": tag(r, "overheidwetgeving:publicatienummer"),
                "pdf": tag(r, "gzd:url"),
                "html": tag(r, "gzd:preferredUrl"),
            })
        m = re.search(r"<sru:nextRecordPosition>(\d+)", page)
        start = int(m.group(1)) if m else 0
        print(f"{len(recs)} records", flush=True)
    recs = [r for r in recs if not NON_AVV_TITLE.search(r["title"])]
    out = Path(a.work) / "catalog.json"
    out.write_text(json.dumps(recs, ensure_ascii=False, indent=0))
    print(f"kept {len(recs)} AVV decisions -> {out}")


def norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


# Title boilerplate before the sector name ("... collectieve arbeidsovereenkomst voor de").
BOILERPLATE = re.compile(r"^.*?(collectieve arbeidsovereenkomst(en)?|c a o|cao)\s+(voor|van|inzake|betreffende)?\s*"
                         r"(de|het|het personeel in dienst van de|het personeel in)?\s*")
# Words too generic to retrieve a sector by.
STOP = set("van voor het de en in der des cao groothandel detailhandel bedrijf bedrijven industrie sector nederland "
           "nederlandse personeel vh v h handel werk branche artikelen food grooth technische technisch verwerkende "
           "sociaal vervoer energie aanverwante proces services maatschappelijke dienstverlening gemengde mode open".split())


def subject(r: dict) -> str:
    t = norm(r["title"])
    m = BOILERPLATE.match(t)
    return t[m.end():] if m else t


def load_catalog(work: str) -> list[dict]:
    cat = json.loads((Path(work) / "catalog.json").read_text())
    for r in cat:
        r["subj"] = subject(r)
    return cat


def load_overview(xlsx: str) -> dict[int, tuple]:
    import openpyxl
    wb = openpyxl.load_workbook(xlsx, read_only=True)
    return {r[0]: r for r in list(wb["Overview"].iter_rows(values_only=True))[1:] if isinstance(r[0], int)}


def cutoff(row: tuple) -> str | None:
    """Overview col I ("Earliest start date of downloaded files", dd.mm.yyyy) -> ISO date; empty = open end."""
    if not row[8]:
        return None
    d, m, y = str(row[8]).split(".")
    return f"{y}-{m}-{d}"


def name_keys(name: str) -> list[str]:
    ws = [w for w in norm(name).split() if len(w) >= 4 and w not in STOP]
    return [w[:7] for w in ws] or [w[:7] for w in norm(name).split() if len(w) >= 4]


def cmd_candidates(a):
    """In scope = Overview col G ("Some files already downloaded?") is True. A catalogue record published
    before the code's cutoff is a candidate when its subject contains a name keyword, contains all words of
    an LLM name variant (variants.json: {code: [terms]}), or is one of the LLM-mapped subject lines
    (subject_map/*.json: {code: [subject[:100]]})."""
    cat, ov = load_catalog(a.work), load_overview(a.xlsx)
    sel = Path(a.selection_dir)
    variants = json.loads((sel / "llm" / "variants.json").read_text())
    mapped = {}
    for f in sorted((sel / "llm" / "subject_map").glob("*.json")):
        for k, v in json.loads(f.read_text()).items():
            mapped.setdefault(int(k), set()).update(v)
    out = {}
    for code, row in ov.items():
        if row[6] is not True:
            continue
        b, keys = cutoff(row), name_keys(row[1])
        terms = [norm(t).split() for t in variants.get(str(code), [])]
        hits = []
        for r in cat:
            if b and r["date"] >= b:
                continue
            s = f" {r['subj']} "
            if (any(k in s for k in keys) or any(t and all(w in s for w in t) for t in terms)
                    or r["subj"][:100] in mapped.get(code, ())):
                hits.append(r["id"])
        out[str(code)] = hits
    Path(sel / "candidates.json").write_text(json.dumps(out, indent=0))
    print(f"{len(out)} codes, {sum(map(len, out.values()))} candidates")


def cmd_select(a):
    """Merge LLM classifications (llm/classify/pass<N>_*.json: {code: {"include": [...], "unsure": {id: why}}}).
    Includes are unioned over passes; "unsure" comes from the highest pass (it re-judges earlier unsure ids).
    Ids not in the code's candidates are dropped (LLM typos). Then conflicts.json is applied: shared ids go
    to one code (pairs / by_date), out-of-lineage subjects are removed (postfilter / remove / remove_after),
    single ids rejected by the verification agent are dropped (remove_ids), and moves are applied. Removed ids land in unsure.json with the reason."""
    cat = {r["id"]: r for r in load_catalog(a.work)}
    ov = load_overview(a.xlsx)
    sel = Path(a.selection_dir)
    cand = {k: set(v) for k, v in json.loads((sel / "candidates.json").read_text()).items()}
    conf = json.loads((sel / "conflicts.json").read_text())
    final, unsure, dropped = {}, {}, []
    files = sorted((sel / "llm" / "classify").glob("pass*_*.json"))
    top = max(int(re.match(r"pass(\d+)", f.name).group(1)) for f in files)
    for f in files:
        npass = int(re.match(r"pass(\d+)", f.name).group(1))
        for k, v in json.loads(f.read_text()).items():
            ok = cand.get(k, set())
            for i in v["include"]:
                (final.setdefault(int(k), set()).add(i) if i in ok else dropped.append((f.name, k, i)))
            if npass == top:
                for i, why in v.get("unsure", {}).items():
                    if i in ok:
                        unsure.setdefault(int(k), {})[i] = why
    for c in final:
        b = cutoff(ov[c])
        final[c] = {i for i in final[c] if not b or cat[i]["date"] < b}
    owners = {}
    for c, ids in final.items():
        for i in ids:
            owners.setdefault(i, set()).add(c)
    for i, cs in owners.items():
        if len(cs) < 2:
            continue
        key = ",".join(map(str, sorted(cs)))
        if key in conf["pairs"]:
            keep = conf["pairs"][key]
        elif key in conf["by_date"]:
            d = conf["by_date"][key]
            keep = d["early"] if cat[i]["date"] < d["before"] else d["late"]
        else:
            raise SystemExit(f"unresolved conflict {key} {i}: add it to conflicts.json")
        for c in cs - {keep}:
            final[c].discard(i)
    for k, rx in conf.get("postfilter", {}).items():  # keep only matching subjects
        for i in list(final.get(int(k), ())):
            if not re.search(rx, cat[i]["subj"]):
                final[int(k)].discard(i)
                unsure.setdefault(int(k), {})[i] = "other retail sub-sector; own CAO before merger"
    for k, rxs in conf.get("remove", {}).items():  # "*" = every code
        for i in [x for c in (final if k == "*" else [int(k)]) for x in list(final.get(c, ()))]:
            if k == "*":
                c_of = [c for c in final if i in final[c]]
                if any(re.search(rx, cat[i]["subj"]) for rx in rxs):
                    for c in c_of:
                        final[c].discard(i)
                        unsure.setdefault(c, {})[i] = "excluded scheme type (SWA / general landbouw-tuinbouw)"
                continue
            if any(re.search(rx, cat[i]["subj"]) for rx in rxs):
                final[int(k)].discard(i)
                unsure.setdefault(int(k), {})[i] = "belongs to another Overview code / out of lineage"
    for k, d in conf.get("remove_after", {}).items():
        for i in list(final.get(int(k), ())):
            if cat[i]["date"] >= d:
                final[int(k)].discard(i)
                unsure.setdefault(int(k), {})[i] = "successor CAO period"
    for k, ids in conf.get("add_ids", {}).items():  # verified (stage 2) additions outside the LLM selection
        b = cutoff(ov[int(k)])
        final.setdefault(int(k), set()).update(i for i in ids if not b or cat[i]["date"] < b)
    for i, why in conf.get("remove_ids", {}).items():  # from the verification agent, after review
        for c in final:
            if i in final[c]:
                final[c].discard(i)
                unsure.setdefault(c, {})[i] = why
    for k, rx in conf.get("move", {}).items():
        src, dst = map(int, k.split("->"))
        for i in list(final.get(src, ())):
            b = cutoff(ov[dst])
            if re.search(rx, cat[i]["subj"]) and (not b or cat[i]["date"] < b):
                final[src].discard(i)
                final.setdefault(dst, set()).add(i)
    for c in unsure:
        for i in list(unsure[c]):
            if i in final.get(c, ()):
                del unsure[c][i]
    (sel / "selection.json").write_text(json.dumps({str(c): sorted(v) for c, v in sorted(final.items())}, indent=0))
    (sel / "unsure.json").write_text(json.dumps({str(c): v for c, v in sorted(unsure.items())}, indent=1, ensure_ascii=False))
    print(f"dropped invalid ids: {dropped}")
    print(f"selected {sum(map(len, final.values()))}, unsure {sum(map(len, unsure.values()))}")


def cmd_verify_batches(a):
    """Text of the first --pages pages (max --chars characters) of each PDF, grouped per code with the code's
    Overview name, into batch files of ~--size documents for the verification agent. Default set = all of
    selection.json; --ids {code: [ids]} restricts it (e.g. stage-1 flags). A PDF is looked up in the code dir,
    its _review/ subfolder, then --extra-dir/<code>/."""
    import subprocess
    ov = load_overview(a.xlsx)
    ids_file = Path(a.ids) if a.ids else Path(a.selection_dir) / "selection.json"
    todo = json.loads(ids_file.read_text())
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    batch, n, nb = [], 0, 0

    def flush():
        nonlocal batch, n, nb
        if batch:
            nb += 1
            (out / f"verify_{nb:02d}.txt").write_text("\n".join(batch))
            batch, n = [], 0

    for code, ids in todo.items():
        c = int(code)
        dirs = [Path(a.dest) / code, Path(a.dest) / code / "_review"]
        if a.extra_dir:
            dirs.append(Path(a.extra_dir) / code)
        if n + len(ids) > a.size:
            flush()
        batch.append(f"\n## CODE {c}: {ov[c][1]} | info: {ov[c][5] or '-'} | cutoff: {ov[c][8] or 'open end'}")
        for i in ids:
            f = next((d / f"{i}.pdf" for d in dirs if (d / f"{i}.pdf").exists()), None)
            txt = subprocess.run(["pdftotext", "-l", str(a.pages), str(f), "-"], capture_output=True, text=True).stdout if f else "MISSING FILE"
            batch.append(f"{i} :: {re.sub(r'\s+', ' ', txt)[:a.chars]}")
            n += 1
    flush()
    print(f"{nb} batches -> {out}")


def cmd_download(a):
    cat = {r["id"]: r for r in json.loads((Path(a.work) / "catalog.json").read_text())}
    approved = json.loads(Path(a.approved).read_text())
    for code, ids in approved.items():
        d = Path(a.dest) / str(code)
        for i in ids:
            f = d / f"{i}.pdf"
            if f.exists():
                print("exists", f)
                continue
            d.mkdir(parents=True, exist_ok=True)
            data = b""
            for attempt in range(4):
                try:
                    # newer publications list an XML url; the zoek site serves the PDF for every id
                    url = cat[i]["pdf"] if cat[i]["pdf"].endswith(".pdf") else f"https://zoek.officielebekendmakingen.nl/{i}.pdf"
                    data = urllib.request.urlopen(url, timeout=180).read()
                    break
                except Exception as e:
                    print("retry", i, e)
                    time.sleep(5 * (attempt + 1))
            if not data.startswith(b"%PDF"):
                print("NOT A PDF", i)
                continue
            f.write_bytes(data)
            print("saved", f)
            time.sleep(0.5)


AMENDMENT = re.compile(r"tot wijziging van het besluit", re.I)
STCRT_REF = re.compile(r"(?:Stcrt\.?|Staatscourant)\s*(\d{4}),?\s*(?:nr\.?|no\.?)?\s*(\d+)", re.I)


def cmd_merge(a):
    """Link each true amendment ("... tot wijziging van het besluit ...") to the decision it amends, using the
    Staatscourant references on its first 3 pages ("Stcrt. 2007, nr. 56"), restricted to selected ids of the same
    code. References to earlier amendments are followed back to the original decision; when one issue holds
    several decisions, the one whose subject shares most words with the amendment's wins (tie -> unlinked).
    Writes <dest>/<code>/merged/<base>_merged.pdf (base + amendments by publication date) and merged.json
    {id: merged file name}. Single files stay untouched."""
    import subprocess
    from PyPDF2 import PdfMerger
    cat = {r["id"]: r for r in load_catalog(a.work)}
    sel_dir = Path(a.selection_dir)
    selection = {int(k): v for k, v in json.loads((sel_dir / "selection.json").read_text()).items()}
    merged, unlinked = {}, []
    for c, ids in selection.items():
        root_dir = Path(a.dest) / str(c)
        refs = {}
        for i in ids:
            if not AMENDMENT.search(cat[i]["title"]):
                continue
            txt = subprocess.run(["pdftotext", "-l", "3", str(root_dir / f"{i}.pdf"), "-"], capture_output=True, text=True).stdout
            own = tuple(i.split("-")[1:3])
            refs[i] = {r for r in STCRT_REF.findall(re.sub(r"\s+", " ", txt)) if r != own}

        def candidates(i):
            return [b for b in ids if b != i and cat[b]["date"] < cat[i]["date"]
                    and any(b == f"stcrt-{y}-{n}" or b.startswith(f"stcrt-{y}-{n}-") for y, n in refs[i])]

        def root_of(i, seen=()):
            roots = set()
            for b in candidates(i):
                if b in refs and b not in seen:
                    roots |= root_of(b, seen + (i,))
                elif b not in refs:
                    roots.add(b)
            return roots

        groups = {}
        for i in refs:
            roots = root_of(i)
            if len(roots) > 1:
                words = set(cat[i]["subj"].split())
                score = {b: len(words & set(cat[b]["subj"].split())) for b in roots}
                best = max(score.values())
                roots = {b for b, s in score.items() if s == best}
            if len(roots) == 1:
                groups.setdefault(roots.pop(), []).append(i)
            else:
                unlinked.append((c, i, sorted(roots)))
        for base, amends in groups.items():
            out = root_dir / "merged" / f"{base}_merged.pdf"
            out.parent.mkdir(exist_ok=True)
            if not out.exists():
                m = PdfMerger()
                for i in [base] + sorted(amends, key=lambda x: cat[x]["date"]):
                    m.append(str(root_dir / f"{i}.pdf"))
                m.write(str(out))
                m.close()
            for i in [base] + amends:
                merged[i] = f"merged/{out.name}"
    (sel_dir / "merged.json").write_text(json.dumps(merged, indent=0))
    (sel_dir / "merge_unlinked.json").write_text(json.dumps(unlinked, indent=0))
    print(f"{len(set(merged.values()))} merged files covering {len(merged)} ids; {len(unlinked)} amendments unlinked")


def decree_date(r: dict) -> str | None:
    """Start = signing date of the decree as stated in the title ("... van 22 mei 2006 tot ...").
    Older titles carry no date -> None. A title date more than a year before publication is
    treated as a metadata error -> None."""
    m = re.search(r"werkgelegenheid\s+van\s+(\d{1,2})\s+([a-z]+)\s+(\d{4})", r["title"].lower())
    if not m or m.group(2) not in MONTHS:
        return None
    d = f"{m.group(3)}-{MONTHS[m.group(2)]:02d}-{int(m.group(1)):02d}"
    pub = dt.date.fromisoformat(r["date"])
    return d if (pub - dt.date.fromisoformat(d)).days <= 366 else None


def cmd_write_excel(a):
    """rows.json: [{code, id, comment}]; Start = decree date from the title; columns follow the Lorenz sheet."""
    import openpyxl
    cat = {r["id"]: r for r in json.loads((Path(a.work) / "catalog.json").read_text())}
    rows = json.loads(Path(a.rows).read_text())
    wb = openpyxl.load_workbook(a.xlsx)
    header = ["CAO", "File_name", "Link", "Datum publicatie", "Organisatie", "Jaargang en nummer", "Rubriek", "Start", "Comments"]
    if a.sheet not in wb.sheetnames:
        wb.create_sheet(a.sheet).append(header)
    ws = wb[a.sheet]
    existing = {(str(r[0]), r[1]) for r in ws.iter_rows(min_row=2, values_only=True)}
    d = lambda s: dt.datetime.strptime(s, "%Y-%m-%d") if s else None
    added = 0
    for row in rows:
        r = cat[row["id"]]
        if (str(row["code"]), r["id"]) in existing:
            continue
        ws.append([int(row["code"]), r["id"], r["html"], d(r["date"]), r["creator"],
                   f"Staatscourant {r['jaargang']}, {r['nummer']}", AVV_TYPE, d(decree_date(r)), row.get("comment")])
        added += 1
    if a.merged:
        merged = json.loads(Path(a.merged).read_text())
        if ws.cell(1, 10).value is None:
            ws.cell(1, 10).value = "Merged file"
        for cells in ws.iter_rows(min_row=2):
            if cells[1].value in merged:
                ws.cell(cells[0].row, 10).value = merged[cells[1].value]
    # Overview: col J = earliest publication date found; comment when nothing found.
    ov = wb["Overview"]
    earliest = {}
    for row in rows:
        c, date = int(row["code"]), cat[row["id"]]["date"]
        earliest[c] = min(earliest.get(c, date), date)
    for code_str in (a.codes or "").split(","):
        if not code_str:
            continue
        c = int(code_str)
        for cells in ov.iter_rows(min_row=2):
            if cells[0].value == c:
                if c in earliest:
                    cur = cells[9].value
                    new = d(earliest[c])
                    if isinstance(cur, dt.datetime):
                        old = [cur]
                    else:
                        old = [dt.datetime.strptime(x, "%d/%m/%Y") for x in re.findall(r"\d{2}/\d{2}/\d{4}", str(cur or ""))]
                    if not old or new < min(old):
                        if cur not in (None, ""):
                            note = f"Earlier Start Update was {cur:%d/%m/%Y} " if isinstance(cur, dt.datetime) else f"Earlier Start Update was {cur} "
                            note += f"before {a.today}"
                            cells[10].value = f"{cells[10].value}; {note}" if cells[10].value else note
                        cells[9].value = new
                else:
                    before = cells[8].value or "open end"
                    note = f"none found on officielebekendmakingen before {before} ({a.today})"
                    cells[10].value = f"{cells[10].value}; {note}" if cells[10].value else note
    wb.save(a.xlsx)
    print(f"added {added} rows to {a.sheet}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--work", required=True, help="work directory for catalog.json and intermediate files")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("catalog")
    sel_dir = str(Path(__file__).parent / "p0_avv_selection")
    for name in ("candidates", "select", "verify-batches", "merge"):
        c = sub.add_parser(name)
        c.add_argument("--selection-dir", default=sel_dir)
        if name != "merge":
            c.add_argument("--xlsx", required=True)
        if name in ("verify-batches", "merge"):
            c.add_argument("--dest", required=True, help="input_pdfs_new folder")
        if name == "verify-batches":
            c.add_argument("--out", required=True)
            c.add_argument("--size", type=int, default=120, help="documents per batch")
            c.add_argument("--ids", help="json {code: [ids]} to check instead of selection.json")
            c.add_argument("--pages", type=int, default=1)
            c.add_argument("--chars", type=int, default=700)
            c.add_argument("--extra-dir", help="fallback folder <extra-dir>/<code>/<id>.pdf")
    c = sub.add_parser("download"); c.add_argument("--approved", required=True); c.add_argument("--dest", required=True)
    c = sub.add_parser("write-excel"); c.add_argument("--xlsx", required=True); c.add_argument("--sheet", required=True)
    c.add_argument("--rows", required=True); c.add_argument("--codes", help="comma list of codes to update in Overview")
    c.add_argument("--merged", help="merged.json {id: merged file}; fills column J 'Merged file' on all rows of the sheet")
    c.add_argument("--today", default=dt.date.today().isoformat())
    a = p.parse_args()
    {"catalog": cmd_catalog, "candidates": cmd_candidates, "select": cmd_select, "download": cmd_download,
     "verify-batches": cmd_verify_batches, "merge": cmd_merge, "write-excel": cmd_write_excel}[a.cmd](a)


if __name__ == "__main__":
    main()
