"""Diagnose markdown blocks that didn't find a CSV match."""
import re
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

md = (ROOT / "inputs" / "leave_information.md").read_text(encoding="utf-8")
df = pd.read_csv(ROOT / "inputs" / "extracted_data_non_salary.csv", sep=";", dtype=str, low_memory=False)

HEADER_RE = re.compile(r"^##\s+(\d+)\.\s+CAO\s+(\d+)\s+-\s+(.+?)\s*$", re.MULTILINE)
META_RE = re.compile(r"^- (cao_number|source_file_name|ingangsdatum|datum_kennisgeving):\s*`([^`]*)`\s*$", re.MULTILINE)


def norm(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\.pdf$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s)
    return s.lower()


md_records = []
matches = list(HEADER_RE.finditer(md))
for i, m in enumerate(matches):
    start = m.start()
    end = matches[i + 1].start() if i + 1 < len(matches) else len(md)
    chunk = md[start:end]
    meta = dict(META_RE.findall(chunk))
    if "cao_number" in meta and "source_file_name" in meta:
        md_records.append((meta["cao_number"], meta["source_file_name"]))

csv_keys_by_cao: dict[str, set[str]] = {}
for _, r in df.iterrows():
    cao = str(r.get("cao_number") or "").strip()
    fn = str(r.get("file_name") or "").strip()
    csv_keys_by_cao.setdefault(cao, set()).add(norm(fn))

missing = [(c, f) for c, f in md_records if norm(f) not in csv_keys_by_cao.get(c, set())]
print(f"Markdown blocks without a CSV match: {len(missing)}")
print()
for c, f in missing[:15]:
    csv_files_for_cao = sorted(csv_keys_by_cao.get(c, set()))
    print(f"CAO {c}: md_file = {f!r}  (CSV has {len(csv_files_for_cao)} files for this CAO)")
    nf = norm(f)
    for cf in csv_files_for_cao:
        # Heuristic: substantial overlap on the first 25 chars
        if nf[:25] and (nf[:25] in cf or cf[:25] in nf):
            tag = "near"
            print(f"    {tag}: {cf!r}")
