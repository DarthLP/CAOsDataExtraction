"""
FORMAT_STATUTORY — beautify statutory_timeline.xlsx consistently AND add the last
Tier-1..3 scored fields that carry a statutory limit but lacked a row. Operates on the
workbook IN PLACE: every cell VALUE and HYPERLINK is preserved; only styling + a few
appended rows change. (The workbook remains Hanna's ground truth; statutory_sync.py
re-derives the machine CSVs afterwards.)

Consistent style applied to every topic tab:
  row1 title (merged, dark) · row2 Direction legend · row3 header (dark, wrapped) ·
  data rows: thin borders, variable-group banding, Direction colour chip, blue
  hyperlink cells for Source 1/2, small grey machine-field. Column widths unified,
  freeze at the first data row (Variable frozen), autofilter on the header.
  Wage's WML series sub-block gets its own sub-header + compact styling.

Run: python3.13 indices/format_statutory.py  (then python3 statutory_sync.py)
"""
import os, sys
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import index_lib as il

XLSX = il.locate("statutory_timeline.xlsx")   # Hanna's hand-maintained ground truth -> review/
BW = "https://wetten.overheid.nl/BWBR0005290"
WFW = "https://wetten.overheid.nl/BWBR0011173"

# scored fields with a statutory limit that still lacked a row (2026-07-06 completeness pass)
ADD_ROWS = {
 "Term": [
  ("Employer notice - max tier (>=15 yr)", "1999-01-01", "(current)", "4", "months",
   "informational", "BW 7:672", BW, "", "Top tenure tier (>=15 yr) = 4 months employer notice. Tenure-graded, not imputed.",
   "term_employer_notice_range_max"),
  ("Employer minimum notice floor", "1999-01-01", "(current)", "1", "months",
   "floor", "BW 7:672", BW, "", "Statutory employer minimum notice 1 month (pre tenure-grading). Maps to the CAO stated notice floor.",
   "term_notice_min_floor_value"),
 ],
 "Contract": [
  ("Working-hours adjustment - tenure requirement (Wfw)", "2000-07-01", "(current)", "6", "months",
   "informational", "Wet flexibel werken (was WAA 2000)", WFW, "",
   "Employee may request an hours change after 26 weeks (~6 months) employment. Lower CAO requirement = more generous. Reference, not imputed.",
   "contract_workhours_adjustment_tenure_requirement_value"),
 ],
}

ARIAL = "Arial"
HEAD_FILL = PatternFill("solid", fgColor="1F3864")
TITLE_FILL = PatternFill("solid", fgColor="D6E4F0")
LEGEND_FILL = PatternFill("solid", fgColor="F2F2F2")
BAND = PatternFill("solid", fgColor="F7F9FC")
DIR_FILL = {
    "floor_lift": "C6E0B4", "floor": "E2EFDA", "cap": "F8CBAD",
    "default": "FFF2CC", "informational": "EDEDED", "informational_formula": "EDEDED",
    "none": "EDEDED", "": "FFFFFF",
}
THIN = Side(style="thin", color="D0D0D0")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
LINK_FONT = Font(name=ARIAL, size=9, color="0563C1", underline="single")
WIDTHS = [34, 12, 12, 15, 22, 15, 34, 13, 13, 60, 30]   # A..K
COLS = 11
DIRECTIONS = "floor_lift = hard minimum (blank filled + below-floor lifted)  ·  floor/default = blank filled only  ·  cap = maximum  ·  informational = documented, not imputed  ·  none = no statute"


def hdr_row(ws):
    for r in range(1, 9):
        if any(ws.cell(row=r, column=c).value == "Variable" for c in range(1, 4)):
            return r
    return None


def style_topic(ws, title):
    for mc in list(ws.merged_cells.ranges):
        ws.unmerge_cells(str(mc))
    h = hdr_row(ws)
    if h is None:
        return
    # append the completeness rows (before restyling so they get styled too)
    for row in ADD_ROWS.get(ws.title, []):
        r = ws.max_row + 1
        for c, v in enumerate(row, 1):
            cell = ws.cell(row=r, column=c, value=v)
            if c in (8, 9) and v:            # source columns -> hyperlink
                cell.hyperlink = v; cell.value = "link"
    # title
    t = ws.cell(row=1, column=1, value=f"{title}")
    t.font = Font(name=ARIAL, bold=True, size=13, color="1F3864")
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=COLS)
    for c in range(1, COLS + 1):
        ws.cell(row=1, column=c).fill = TITLE_FILL
    ws.row_dimensions[1].height = 20
    # legend
    lg = ws.cell(row=2, column=1, value=DIRECTIONS)
    lg.font = Font(name=ARIAL, italic=True, size=8, color="595959")
    ws.merge_cells(start_row=2, start_column=1, end_row=2, end_column=COLS)
    # header
    for c in range(1, COLS + 1):
        cell = ws.cell(row=h, column=c)
        cell.font = Font(name=ARIAL, bold=True, color="FFFFFF", size=9)
        cell.fill = HEAD_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = BORDER
    ws.row_dimensions[h].height = 28
    # data
    prev, band = None, False
    in_series = False
    for r in range(h + 1, ws.max_row + 1):
        a = ws.cell(row=r, column=1).value
        if a is None or str(a).strip() == "":
            continue
        # Wage series sub-header
        if ws.title == "Wage" and str(a).strip() == "Valid from":
            in_series = True
            for c in range(1, 7):
                cell = ws.cell(row=r, column=c)
                cell.font = Font(name=ARIAL, bold=True, color="FFFFFF", size=9)
                cell.fill = PatternFill("solid", fgColor="2E5395")
                cell.border = BORDER
                cell.alignment = Alignment(horizontal="center", wrap_text=True)
            continue
        if ws.title == "Wage" and str(a).startswith("Statutory minimum wage — full"):
            cell = ws.cell(row=r, column=1)
            cell.font = Font(name=ARIAL, bold=True, italic=True, size=10, color="1F3864")
            continue
        if ws.title == "Wage" and str(a).startswith("Note"):
            ws.cell(row=r, column=1).font = Font(name=ARIAL, italic=True, size=8, color="808080")
            continue
        if a != prev and not in_series:
            band = not band
            prev = a
        fill = BAND if (band and not in_series) else PatternFill("solid", fgColor="FFFFFF")
        ncol = 6 if in_series else COLS
        for c in range(1, ncol + 1):
            cell = ws.cell(row=r, column=c)
            cell.border = BORDER
            if not (cell.hyperlink):
                cell.font = Font(name=ARIAL, size=(8 if in_series else 9),
                                 color=("808080" if c == 11 else "000000"))
            cell.alignment = Alignment(vertical="top", wrap_text=(c in (1, 7, 10, 11)),
                                       horizontal=("center" if c in (2, 3, 4, 6) else "left"))
            if c != 6 or in_series:
                cell.fill = fill
        # direction chip (col F) for topic tables
        if not in_series:
            role = str(ws.cell(row=r, column=6).value or "").strip()
            dc = ws.cell(row=r, column=6)
            dc.fill = PatternFill("solid", fgColor=DIR_FILL.get(role, "FFFFFF"))
            dc.alignment = Alignment(horizontal="center", vertical="top")
            dc.font = Font(name=ARIAL, size=9, bold=(role == "floor_lift"))
        # hyperlink cells (H, I)
        for c in (8, 9):
            cell = ws.cell(row=r, column=c)
            if cell.hyperlink:
                cell.value = "link"; cell.font = LINK_FONT
                cell.alignment = Alignment(horizontal="center", vertical="top")
    for i, w in enumerate(WIDTHS):
        ws.column_dimensions[get_column_letter(i + 1)].width = w
    ws.freeze_panes = f"B{h + 1}"
    ws.auto_filter.ref = f"A{h}:{get_column_letter(COLS)}{ws.max_row}"


def style_overview(ws):
    for mc in list(ws.merged_cells.ranges):
        ws.unmerge_cells(str(mc))
    ws.cell(row=1, column=1, value="STATUTORY REFERENCE — which topics have a statutory anchor and how the index uses it")\
        .font = Font(name=ARIAL, bold=True, size=13, color="1F3864")
    ws.merge_cells("A1:E1")
    for c in range(1, 6):
        ws.cell(row=1, column=c).fill = TITLE_FILL
    # find header row (Topic)
    hr = None
    for r in range(1, 8):
        if any(str(ws.cell(row=r, column=c).value).strip() == "Topic" for c in range(1, 4)):
            hr = r; break
    if hr:
        for c in range(1, 6):
            cell = ws.cell(row=hr, column=c)
            cell.font = Font(name=ARIAL, bold=True, color="FFFFFF", size=10)
            cell.fill = HEAD_FILL
            cell.alignment = Alignment(horizontal="center", wrap_text=True)
            cell.border = BORDER
        band = False
        for r in range(hr + 1, ws.max_row + 1):
            if not ws.cell(row=r, column=1).value:
                continue
            band = not band
            for c in range(1, 6):
                cell = ws.cell(row=r, column=c)
                cell.border = BORDER
                cell.font = Font(name=ARIAL, size=10)
                cell.alignment = Alignment(vertical="top", wrap_text=True)
                cell.fill = BAND if band else PatternFill("solid", fgColor="FFFFFF")
            anc = str(ws.cell(row=r, column=2).value or "")
            ws.cell(row=r, column=2).fill = PatternFill(
                "solid", fgColor="E2EFDA" if anc.startswith("Yes")
                else "FFF2CC" if anc == "Partial" else "F8CBAD")
        ws.freeze_panes = f"A{hr + 1}"
    for col, w in zip("ABCDE", [12, 18, 28, 32, 48]):
        ws.column_dimensions[col].width = w


def main():
    wb = load_workbook(XLSX)
    TITLES = {
        "Leave": "LEAVE — statutory leave floors (parental-leave FRE index)",
        "Absence": "ABSENCE — vacation / sick pay / care-leave floors (floor_lift = hard minimum right)",
        "Wage": "WAGE — statutory minimum wage (WML) reference + full revision series",
        "Term": "TERM — probation caps, notice, severance (BW / WWZ / WAB)",
        "Contract": "CONTRACT — ketenregeling defaults + working-hours adjustment (WWZ / WAB / Wfw)",
        "Overtime": "OVERTIME — ATW caps + minimum rest (no statutory pay premium)",
        "Pension": "PENSION — Witteveen accrual cap, AOW age, franchise (mostly fund-deferred)",
        "Fringe": "FRINGE — untaxed commuting allowance (fiscal norm, informational)",
        "Training": "TRAINING — 2022 transparency-directive rights (non-numeric)",
        "Homeoffice": "HOMEOFFICE — right to request flex work + thuiswerkvergoeding norm",
        "Safety": "SAFETY — Arbowet-mandated provisions (coverage caveat)",
        "Childcare": "CHILDCARE — universal employer levy (not CAO-specific)",
    }
    for tab in wb.sheetnames:
        if tab == "Overview":
            style_overview(wb[tab])
        else:
            style_topic(wb[tab], TITLES.get(tab, tab.upper()))
    wb.save(XLSX)
    print(f"reformatted {len(wb.sheetnames)} tabs; added "
          f"{sum(len(v) for v in ADD_ROWS.values())} completeness rows (Term 2, Contract 1)")


if __name__ == "__main__":
    main()
