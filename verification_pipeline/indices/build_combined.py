"""
BUILD_COMBINED v3 — one merged CSV + one polished Excel workbook over all v2 index outputs.

  all_indices_combined.csv : all topic index columns merged on id (doc grain).
  all_indices.xlsx         : a designed workbook —
     • Deck        — blue title slide: what this is, the two scales/two tracks, tab index + colour key.
     • Dictionary  — EVERY column of EVERY tab explained (what it is · kind · how it is built).
     • data tabs   — frozen header (row 2), autofilter, and a NOTE on each header cell (hover to
                     read its definition — no more unreadable frozen legend block).
     • Statutory   — WML folded in; columns ordered SCORES (z / coverage / pct) | thick line | VALUES.
     • tab strip   — colour-graded by role (Deck darkest → topics lightest).

Run (after the drivers + mw + statutory + panel + composite + factor analysis):
  python3.13 indices/build_combined.py
"""
import os, sys
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, Color
from openpyxl.comments import Comment
from openpyxl.utils import get_column_letter
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il

TOPICS = ["absence", "term", "contract", "overtime", "training", "bonus", "fringe",
          "homeoffice", "pension", "childcare", "safety", "ai"]

# ---------------------------------------------------------------------------
# COLUMN DEFINITIONS
#   Each definition answers three things: WHAT it is · WHAT KIND of measure
#   (boolean-share / numeric-z / percentile / raw value) · HOW it is computed.
# ---------------------------------------------------------------------------
EXACT = {
    "cao_number": "KEY. CAO identifier — one sector or company-wide collective agreement.",
    "id": "KEY. Record/document id (one extracted PDF edition of a CAO).",
    "month": "KEY. Calendar month YYYY-MM — the panel grain (one row per CAO per month).",
    "year": "Calendar year of file_date (documents) or, on wage rows, of the wage table's OWN effective "
            "date (salary_N_start_date, the 'per 1 July 2023' printed on the scale) — not the filing date, "
            "not the agreement's ingangsdatum.",
    "file_date": "THE v2 time axis = datum_kennisgeving (edition/publication date; falls back to ingangsdatum).",
    "ingangsdatum": "Term start date (Looptijd) — constant across all editions of one term.",
    "Date_first_is_Ingangsdatum": "READY-MADE START DATE (ISO), first-file rule: the term's FIRST "
                                  "file (term_edition_seq 1) starts at its ingangsdatum, every later file in "
                                  "the term starts at its own file_date. Always filled.",
    "Date_retro_datum": "READY-MADE START DATE (ISO): Date_first_is_Ingangsdatum, OVERRIDDEN by the explicit "
                        "retroactivity start date (retro_start_date, from the agreement text) where one exists; "
                        "otherwise identical to Date_first_is_Ingangsdatum.",
    "file_name": "Source PDF filename.",
    "document_type": "general_document_type (all scored rows are full_cao_*).",
    "term_group": "cao_number|ingangsdatum — links every edition of one term.",
    "is_full_cao": "TRUE = a full_cao_* document (partial docs carry no z-scores).",
    "n_fields_pop": "How many magnitude fields are populated (full variant) — score reliability.",
    "low_support": "TRUE = fewer than 2 populated fields, so the score is fragile.",
    "n_topics_scored": "COUNT. Topics with a NUMERIC z among the 11 numeric dimensions (the 10 dual topics' "
                       "numeric track + wage). For the combined roll-up's availability see n_topics_combined.",
    "n_topics_gen01": "COUNT. Topics with a gen01 percentile this row.",
    "n_topics_combined": "COUNT. Topics with a combined (magnitude+coverage) score this row.",
    "overall_z": "COMPOSITE HEADLINE SCORE (z). Equal-weight AVAILABLE-CASE mean of the per-topic COMBINED z "
                 "over 13 inputs: the 10 dual topics' {t}_z + wage_median_z + safety & childcare coverage z "
                 "(ai excluded — 99% of CAOs have no AI clause). Averaged over ONLY the topics this document "
                 "is scored on (a doc scored on 9 gets the mean of those 9; see n_topics_combined). The "
                 "PRIMARY overall generosity score. Higher = more worker-generous.",
    "overall_pctile": "COMPOSITE HEADLINE RANK (0-1). Mean of the per-topic COMBINED pctile (numeric + coverage), "
                      "same 13 inputs as overall_z (wage enters as wage_median_pctile; ai excluded) — the PRIMARY "
                      "0-1 generosity rank. 0=least, 1=most.",
    "overall_z_var": "COMPOSITE SPREAD. Variance across the per-topic COMBINED z's = how lopsided the package is "
                     "(high = strong on some topics, weak on others). Pairs with overall_z.",
    "overall_z_without_wage": "COMPOSITE HEADLINE SCORE (z), WAGE EXCLUDED — the AgreementLevel variant of "
                              "overall_z: equal-weight available-case mean of the 12 non-salary inputs (10 dual "
                              "topics' combined z + safety & childcare coverage z). Wage is excluded because at "
                              "document grain it is a CAO-year fact (join the Wage tab instead). NOT directly "
                              "comparable to the panel's wage-inclusive overall_z.",
    "overall_pctile_without_wage": "COMPOSITE HEADLINE RANK (0-1), WAGE EXCLUDED — mean of the same 12 non-salary "
                                   "inputs' combined pctiles. 0=least, 1=most generous (non-salary conditions).",
    "overall_z_var_without_wage": "COMPOSITE SPREAD, WAGE EXCLUDED. Variance across the 12 non-salary combined "
                                  "z's — lopsidedness of the non-salary package. Pairs with overall_z_without_wage.",
    "overall_numeric_z_without_wage": "COMPOSITE SCORE (z), NUMERIC-only + WAGE EXCLUDED: mean of the 10 dual "
                                      "topics' numeric z's.",
    "overall_numeric_z_var_without_wage": "Variance across the 10 numeric z's (wage excluded).",
    "overall_numeric_pctile_without_wage": "COMPOSITE RANK (0-1), NUMERIC-only + WAGE EXCLUDED: mean of the 10 "
                                           "dual topics' numeric pctiles.",
    "n_topics_scored_without_wage": "COUNT. Topics with a numeric z among the 10 non-salary dual topics.",
    "n_topics_combined_without_wage": "COUNT. Topics with a combined score among the 12 non-salary inputs.",
    "n_topics_pctile_without_wage": "COUNT. Topics with a numeric pctile among the 10 non-salary dual topics.",
    "overall_numeric_z": "COMPOSITE SCORE (z), NUMERIC-only companion to overall_z. Mean of the topic NUMERIC "
                         "(magnitude) z's; excludes coverage/breadth.",
    "overall_numeric_pctile": "COMPOSITE RANK (0-1), NUMERIC-only companion to overall_pctile. Mean of the topic "
                              "numeric pctiles; excludes coverage.",
    "overall_numeric_z_var": "COMPOSITE SPREAD, NUMERIC-only companion to overall_z_var. Variance across the topic "
                             "NUMERIC z's (excludes coverage). Pairs with overall_numeric_z.",
    "coverage_overall": "PROVISION BREADTH (0-1). Mean of the topic coverage shares — how MANY provision types exist. Boolean-based.",
    "coverage_overall_z": "z of coverage_overall (breadth standardised vs the term-deduped pool).",
    "coverage_overall_pctile": "RANK (0-1). Percentile of coverage_overall in the pool.",
    "leave_z_fre_total": "Companion: pooled z of the SUMMED FRE weeks (an older leave measure).",
    "leave_z": "Leave COMBINED SCORE (z) = mean of leave_numeric_z and leave_coverage_z — the HEADLINE leave "
               "generosity. Numeric part = mean of the per-type (paternity/adoption/parental) pooled z's "
               "(maternity excluded, near-zero variance).",
    "leave_z_extr": "Pooled z of fre_total_extracted (CAO-stated leave only, no statutory floor).",
    "fre_total_with_statutory": "Full-Rate-Equivalent weeks incl. the era statutory floor (headline leave size). "
                                "FRE = paid_wks×1 + partial_wks×(rate/100) + unpaid×0.",
    "fre_total_extracted": "FRE weeks from CAO-stated values only (no statutory floor).",
    "duration_total_weeks_with_statutory": "Total job-protected weeks incl. statutory floor.",
    "duration_total_weeks_extracted": "Total job-protected weeks, CAO-stated only.",
    "in_force_id": "id of the file in force this month (latest file_date <= month end — round-up).",
    "thin_doc": "FLAG (bool). TRUE = this edition was SOURCE-VERIFIED as not a substantive full CAO "
                "(mis-ingested appendix, mantel/umbrella doc deferring to companion regulations, "
                "deviations-only delta, procedure-only doc, or failed extraction). Its scores are "
                "carried from the last full edition (see scores_carried_from) — scoring its near-empty "
                "extraction would have read as 'all provisions abolished'. Registry: thin_docs_reviewed.csv.",
    "scores_carried_from": "KEY. For thin_doc=TRUE rows: the id of the edition whose scores this row "
                           "carries — normally the same CAO's last full edition (the old agreement stays "
                           "in force); for explicit cross-CAO deferrals (e.g. RPO's deltas on the national "
                           "CAO PO) it is the NAMED base CAO's in-force edition. Empty = the row's scores "
                           "are its own.",
    "n_caos_in_month": "COUNT. CAOs in force this month (panel pool size).",
    "n_caos": "COUNT. CAOs in the month's cross-section.",
    "mw_low": "WAGE VALUE. p10 of the CAO's wage-scale amounts (EUR/month) — the low-skill floor.",
    "mw_q25": "WAGE VALUE. p25 of scale amounts (EUR/month).",
    "mw_median": "WAGE VALUE. p50 (median) of scale amounts (EUR/month).",
    "mw_q75": "WAGE VALUE. p75 of scale amounts (EUR/month).",
    "mw_mean": "WAGE VALUE. Mean of scale amounts (EUR/month) — the level that feeds wage_mean_z.",
    "mw_high": "WAGE VALUE. p90 of scale amounts — the high-skill top.",
    "mw_span_pct": "WAGE SPREAD. (p90-p10)/p10 ×100 — within-CAO wage compression.",
    "mw_entry_median": "WAGE VALUE. Median of entry/aanloop-scale amounts (hiring floor, kept separate).",
    "n_entry": "COUNT. Entry-scale observations.",
    "n_obs": "COUNT. Wage observations in the (cao, year) cell.",
    "flag_thin": "QUALITY FLAG. TRUE = fewer than 5 observations (percentiles unreliable).",
    "wml_month": "STATUTORY WAGE. Statutory minimum wage, year-average EUR/month (national floor).",
    "wml_month_eur": "STATUTORY WAGE. Statutory minimum MONTHLY wage in force (EUR/month, adult rate).",
    "wml_hour_eur": "STATUTORY WAGE. Statutory minimum HOURLY wage in force (EUR/hour, adult rate).",
    "effective_from": "Date this statutory WML value takes effect.",
    "basis": "How the WML figure was derived (monthly statutory, or €/hr = month ÷ 164.5 ≈ 38h wk).",
    "please_verify": "FLAG. YES = a derived value — verify against rijksoverheid.nl.",
    "wml_please_verify": "FLAG. YES = the WML figure for this month is derived — verify against rijksoverheid.nl.",
    "source": "Source URL / reference for the WML figure.",
    "ratio_low_wml": "RELATIVE WAGE. mw_low / WML — the CAO wage floor vs the national floor (1.0 = at the WML).",
    "ratio_mean_wml": "RELATIVE WAGE. mw_mean / WML — the CAO average wage vs the national floor.",
    "ratio_median_wml": "RELATIVE WAGE. mw_median / WML — the CAO median wage vs the national floor.",
    "ratio_high_wml": "RELATIVE WAGE. mw_high / WML — the CAO top wage vs the national floor.",
    "ratio_entry_wml": "RELATIVE WAGE. mw_entry_median / WML — the hiring floor vs the national floor.",
    "wage_median_z": "WAGE HEADLINE SCORE (z). Pooled z of the NOMINAL median wage (mw_median, EUR/month) — "
                     "THE wage dimension in overall_z. Nominal (rises with inflation; use for within-year "
                     "comparison). WML normalisation lives ONLY in the ratio_*_wml value columns.",
    "wage_median_pctile": "WAGE HEADLINE RANK (0-1). ECDF rank of wage_median_z. The wage dimension in the overall pctile roll-ups.",
    "wage_mean_z": "WAGE SCORE (z). Pooled z of the NOMINAL mean wage (mw_mean, EUR/month) — companion; not in the overall.",
    "wage_mean_pctile": "WAGE RANK (0-1). ECDF rank of wage_mean_z (companion).",
    "wage_src_year": "KEY/VALUE. Calendar year of the wage-scale row this document's wage columns are carried "
                     "from (as-of forward-fill: a wage table stays in force until replaced, so a doc filed in a "
                     "gap year carries the last known table). Differs from the doc's own year exactly in gap years.",
    "leave_numeric_z_extr": "Leave NUMERIC z, extracted-only (no statutory floor) — the stability-diagnostic axis.",
    "leave_numeric_z_fre_total": "Leave NUMERIC z of the SUMMED FRE weeks (companion to the per-type mean).",
    "statutory_overall_z": "The STATUTORY pseudo-file's overall z that month (the legal-minimum reference line).",
    "any_imputed": "TRUE = a statutory floor exceeded the extracted leave package somewhere in this row.",
    "variable": "The input variable this row describes (see its own definition in the Dictionary tab).",
    "variable_desc": "Plain-English description of the variable named in the 'variable' column (added for readability).",
    "field": "The input field this row describes (see its definition in the Dictionary tab).",
    "field_desc": "Plain-English description of the field named in the 'field' column (added for readability).",
    "topic": "Topic name — one of the 12 generosity dimensions (leave, term, contract, overtime, …).",
    "communality": "FACTOR STAT (0-1). Share of this variable's variance the retained factors explain "
                   "together. High = the variable is well-described by the common factors; low = it stands alone.",
    "analysis": "Which factor analysis this row belongs to (a topic, or the overall across-topic EFA).",
    "track": "Which score track this row weights: NUMERIC (the topic's numeric fields) or COVERAGE (its yes/no booleans).",
    "section": "Which independent factor analysis this block belongs to: combined / numeric / coverage (blocks are separated by a thick line).",
    "type": "The variable's data type in the mixed (FAMD) analysis: numeric (an amount) or boolean (a yes/no provision).",
    "wage_span_z": "WAGE SPREAD (z). Pooled z of mw_span_pct — within-CAO wage compression (p90 vs p10).",
    "corr_PC1": "STAGE-1 CHECK (−1..1). Correlation of the data-driven 1st principal component (PCA weighting "
                "of the topic's fields) with our EQUAL-WEIGHT topic score. Near ±1 → equal-weighting already "
                "captures the main signal, so weighting the fields by PCA would barely change ranks.",
    "corr_FA1": "STAGE-1 CHECK (−1..1). Same as corr_PC1 but using the 1st factor-analysis factor instead of PCA.",
    "PC1_var%": "STAGE-1 STAT (%). Share of the topic's field variance captured by the TRUE 1st principal component "
                "(largest correlation-matrix eigenvalue ÷ #fields). High → the fields move together (one underlying "
                "dimension); low → they measure different things.",
    "n_factors": "COUNT. Factors retained for this topic (eigenvalue > 1, capped at the field count).",
    "fields": "COUNT. Number of input fields entering this topic's score.",
    "combined_pay_pearson": "CORRELATION (−1..1). This topic's COMBINED score (good+many) vs the CAO's wage — "
                            "are CAOs generous on this topic also higher-paying? (Pearson = linear.)",
    "combined_pay_spearman": "CORRELATION (−1..1). Rank version of combined_pay_pearson.",
    "coverage_pay_pearson": "CORRELATION (−1..1). This topic's COVERAGE (many provisions) vs the CAO's wage.",
    "coverage_pay_spearman": "CORRELATION (−1..1). Rank version of coverage_pay_pearson.",
    "numeric_pay_pearson": "CORRELATION (−1..1). This topic's NUMERIC (large amounts) vs the CAO's wage.",
    "numeric_pay_spearman": "CORRELATION (−1..1). Rank version of numeric_pay_pearson.",
    "overall_factor1": "FACTOR SCORE. This document's position on across-topic varimax factor 1 (from the topic-z EFA).",
    "overall_factor2": "FACTOR SCORE. Document position on across-topic varimax factor 2.",
    "overall_factor3": "FACTOR SCORE. Document position on across-topic varimax factor 3.",
    "worker_type": "Worker-type label from the parser dataset (only ~17% of rows are labelled).",
    "median": "Median EUR/month for this (cao, year, worker_type).",
    "mean": "Mean EUR/month for this (cao, year, worker_type).",
    "n": "COUNT. Observations.",
    "stat_leave_fre_weeks": "STATUTORY VALUE. Statutory leave package in FRE weeks in force that month.",
    # pension — the one topic whose blanks are NEVER filled (fund-deferral, not absence)
    "pension_numeric_z": "PENSION NUMERIC SCORE (z) — AVAILABLE-CASE ONLY, blanks NEVER filled. Most CAOs "
                         "delegate pensions to a mandatory industry fund (ABP/PFZW/PMT/bpfBOUW; verplichtstelling, "
                         "Wet Bpf 2000) which sets contribution, accrual, franchise and retirement age — so a blank "
                         "means 'ruled by the fund', not 'no pension'. This score covers ONLY the minority of CAOs "
                         "stating their own terms (often company schemes) and is not representative of fund sectors. "
                         "Fields: employee contribution % (−), accrual % (+), franchise € (−), early-retirement age (−).",
    "pension_z": "PENSION COMBINED SCORE (z) = mean of pension_numeric_z (available-case only — blanks are "
                 "fund-deferral, never filled; see pension_numeric_z) and pension_coverage_z (booleans: excedent, "
                 "accrual during leave/illness-y2, 50/50 premium split, mandatory participation).",
    "pension_coverage": "PENSION COVERAGE (0-1). Share of 5 pension booleans TRUE (excedent top-up, accrual during "
                        "statutory leave, accrual in illness year 2, 50/50 premium split, mandatory participation). "
                        "NOTE: in mandated-fund sectors participation is mandatory BY LAW even when the CAO is silent, "
                        "so this understates real fund membership.",
    "pension_pension_type": "DESCRIPTIVE. Pension scheme type (DB / DC / hybrid / unspecified) — carried for "
                            "reference, NOT scored (no clean generosity direction in the Wtp transition era).",
    "pension_retire_age_normal": "DESCRIPTIVE. Stated normal retirement age — mostly restates the statutory "
                                 "AOW/pensioenrichtleeftijd, so it is carried for reference, NOT scored.",
}
SUFFIX = [
    ("_coverage_z", "COVERAGE SCORE (z). z of the coverage share (breadth standardised vs the term-deduped pool)."),
    ("_coverage_n", "COUNT. Number of generosity booleans in this topic's coverage set."),
    ("_coverage_pctile", "COVERAGE RANK (0-1). Percentile of the coverage share in the pool."),
    ("_coverage", "COVERAGE (0-1). Share of the topic's generosity yes/no booleans that are TRUE — how MANY "
                  "provisions exist (breadth), not how large. A blank boolean counts as not-present "
                  "(the denominator is fixed), so silence lowers coverage."),
    ("_numeric_z", "NUMERIC SCORE (z). Magnitude track ONLY — how MUCH the provision gives (numeric fields, "
                   "pooled z, available-case mean). Blank-cell rules (Deck / METHODOLOGY §7.2): a stated number is "
                   "scored (below a hard floor → lifted); a blank is filled with the STATUTORY value only where a "
                   "floor/default exists (12 of 39 fields); an EXTRA perk's blank becomes 0 only when its presence "
                   "boolean is False (confirmed absence); boolean True + no amount stays blank (never guessed); "
                   "PENSION blanks are never filled (fund-deferral). Combines with coverage into the headline {t}_z."),
    ("_numeric_pctile", "NUMERIC RANK (0-1). Equal-range percentile of the numeric/magnitude track."),
    ("_numeric_rankpct", "NUMERIC ROBUSTNESS RANK (0-1). Pooled percentile rank of the numeric z in the term-deduped pool."),
    ("_numeric_z_raw", "NUMERIC SCORE (z), RAW variant — no forward-fill / statutory impute / cap."),
    ("_combined_z", "COMBINED SCORE (z). Mean of the topic's numeric z and coverage z (size + breadth)."),
    ("_combined01", "COMBINED RANK (0-1). Mean of the topic's numeric pctile and coverage pctile."),
    ("_gen01", "NUMERIC RANK (0-1). Equal-range percentile of the numeric track. (Legacy name — now _numeric_pctile.)"),
    ("_z_raw", "SCORE (z), RAW variant — no forward-fill / statutory impute / cap applied."),
    ("_pctile", "RANK (0-1). Equal-range percentile, 0=least…1=most generous. A topic HEADLINE {t}_pctile is the "
                "COMBINED (mean of numeric & coverage percentiles); a coverage/numeric one is that single track."),
    ("_prank", "RANK (0-1). Percentile rank in the term-deduped pool."),
    ("_z_mean", "AGGREGATE. Cross-CAO MEAN of this z in the month."),
    ("_z_var", "AGGREGATE. Cross-CAO VARIANCE of this z in the month."),
    ("_pay_z", "SUB-SCORE (z). Overtime PAY sub-score = mean of the 4 pay-field pooled z's."),
    ("_protection_z", "SUB-SCORE (z). Overtime PROTECTION sub-score = mean of the 3 protection-field pooled z's."),
    ("_z", "SCORE (z). Pooled z: winsorise each field 1/99, standardise vs the ALL-YEARS term-deduped pool, "
           "clip to ±3, sign so higher = more worker-generous. Unit = SD from the pool mean (0 = average CAO). "
           "A topic HEADLINE {t}_z is the COMBINED (mean of numeric z & coverage z); single-track topics = that one track."),
    ("_mo", "VALUE. Months (canonical unit, forward-filled)."),
    ("_wk", "VALUE. Per week (canonical unit)."),
    ("_h", "VALUE. Hours (canonical unit)."),
    ("_pct", "VALUE. Percent (canonical unit)."),
    ("_eur_km", "VALUE. EUR per km."),
    ("_eur", "VALUE. EUR (canonical unit)."),
    ("_imputed", "FLAG. TRUE = a statutory floor was used instead of the extracted value."),
    ("_weeks", "VALUE. Weeks (leave component)."),
    ("_rate", "VALUE. Pay rate 0-1 (leave component: fraction of salary paid)."),
]
PREFIX = [
    ("loading_f", "FACTOR LOADING. Varimax loading of this variable on this factor (correlation with the latent factor)."),
    ("overall_p", "RANK (0-1). Cross-CAO percentile of overall_numeric_z in the month."),
    ("stat_", "STATUTORY VALUE (raw). The legal floor/default fed into the STATUTORY pseudo-file — the legal "
              "minimum in force that era; 0 where the law grants no such perk."),
    ("flag_", "QUALITY FLAG (bool). TRUE marks a row the topic driver flags as suspicious — e.g. a stated value "
              "above the legal cap, an anomalous unit, or too few observations. Flagged rows are kept but should "
              "be read with care; the exact rule is in the topic driver's docstring."),
    ("mw_", "WAGE VALUE. Wage-scale statistic (EUR/month)."),
]


def define(col):
    if col in EXACT: return EXACT[col]
    for s, d in SUFFIX:
        if col.endswith(s): return d
    for p, d in PREFIX:
        if col.startswith(p): return d
    if col and col[0] == "f" and col[1:].isdigit():
        return ("FACTOR LOADING (−1..1). How strongly this variable aligns with varimax factor "
                f"{col[1:]} — a latent dimension the analysis extracted. |loading| near 1 = defines the factor.")
    if col.startswith("bundle") and col[6:].isdigit():
        return ("TETRACHORIC LOADING (−1..1). How strongly this yes/no provision loads on co-adoption "
                f"bundle {col[6:]} — a group of provisions CAOs tend to adopt together.")
    if "_factor" in col: return "FACTOR SCORE. Per-document varimax factor score."
    return ("INPUT VALUE. Field from the corrected dataset, converted to its canonical unit and forward-filled "
            "within the CAO. Blank = the file states no number (see the Deck reading rules for when a blank is "
            "scored as the statutory value, as 0, or left out).")


def kind(col):
    """One-word class for the Dictionary 'Kind' column."""
    if col in ("cao_number", "id", "month", "in_force_id", "term_group", "file_name",
               "file_date", "ingangsdatum", "document_type", "variable", "analysis",
               "worker_type", "effective_from"):
        return "key / label"
    if col.startswith("stat_") or col.startswith("mw_") or col.startswith("wml") \
       or col.startswith("ratio_") or col in ("median", "mean", "basis", "source"):
        return "value (raw)"
    if col.startswith("flag_") or col.endswith("_imputed") or "verify" in col or col == "any_imputed":
        return "flag (bool)"
    if col.startswith("loading_f") or "_factor" in col or col == "communality":
        return "factor stat"
    if col.endswith(("_gen01", "_pctile", "_prank", "_combined01")) or col.startswith("overall_p") \
       or col in ("overall_gen01", "overall_combined01"):
        return "rank (0-1)"
    if col.endswith("_coverage"):
        return "coverage (0-1)"
    if col.endswith("_z") or col.endswith("_z_raw"):
        return "score (z)"
    if col.startswith("n_") or col.endswith(("_coverage_n", "_n")) or col == "n":
        return "count"
    return "value / meta"


ARIAL = "Arial"
HDR_FILL = PatternFill("solid", fgColor="1F4E78")          # dark-blue header (main block)
LIGHT_HDR_FILL = PatternFill("solid", fgColor="BDD7EE")    # light-blue header (breakdown block)
DECK_BG = "1F3864"                                          # Deck slide background (fixed, not the tab colour)
THICK = Side(style="medium", color="1F3864")

# dual-track topics (numeric magnitude AND coverage booleans) — the ones that get the
# numeric / coverage / combined split. Single-track topics keep their one score.
DUAL = ["leave", "absence", "term", "contract", "overtime", "training", "bonus", "fringe", "homeoffice", "pension"]
# per-topic breakdown columns pushed to the far right (light-blue headers, behind a thick line)
BREAKDOWN_SUFFIXES = ["_numeric_z", "_coverage_z", "_numeric_pctile", "_coverage_pctile"]

# tab-strip colours — MATCH the manually-saved scheme (do not change without asking)
C_DECK = Color(theme=3, tint=-0.499984740745262)  # Deck        — theme colour 3, darkened 50%
C_HEAD = Color(theme=3, tint=0.0)                 # Composite, PanelMonthly — theme colour 3
C_DICT = "FF2E5496"                               # Dictionary
C_ANALYSIS = "FF4472C4"                           # factor / weighting / structure tabs
C_DATA = "FFB4C7E7"                               # Statutory, Wage, topics, Leave

def _tabcolor(spec):
    """Fresh Color per sheet (theme Colors shouldn't be shared across sheets)."""
    if isinstance(spec, Color):
        return Color(theme=spec.theme, tint=spec.tint)
    return spec

# derived columns dropped from the workbook (recomputable from the dates / doc set)
DROP_COLS = {"doc_is_newest", "year", "months_since_file", "is_stale", "pub_lag_months",
             "score_src_id"}   # score_src_id = internal helper (thin_doc + scores_carried_from are the marks)

# per-topic tab summaries that deviate from the generic one-liner
TOPIC_TAB_SUMMARY = {
    "pension": "pension index per document — AVAILABLE-CASE ONLY: blanks are never filled, because most CAOs "
               "delegate pensions to a mandatory industry fund (ABP/PFZW/PMT…) that sets the terms; a blank = "
               "'ruled by the fund', not 'no pension'. Numeric z covers only CAOs stating their own terms; "
               "coverage (excedent, accrual-in-leave/illness, 50/50 split, mandatory participation) is the "
               "broader signal.",
    "ai": "ai provisions per document — coverage-only and ~99% empty (nascent, 2024+). EXCLUDED from the "
          "overall roll-ups; coverage z/pctile blank by design (no variance to standardise).",
}

# TABS in DISPLAY ORDER: (tab name, csv file, one-line summary, tab-strip colour)
# TABS in DISPLAY ORDER (matches the workbook's saved order; a manual reorder in Excel is preserved
# on the next rebuild — see _existing_sheet_order). WageStructure is merged into FactorLoadings.
TABS = [
    ("PanelMonthly", "all_indices_panel_monthly.csv",
     "cao × month in-force panel (scores constant per in-force file) + per-topic combined z & pctile + numeric/coverage breakdown + STATUTORY floor rows.", C_HEAD),
    ("AgreementLevel", "cao_agreement_level.csv",
     "One row per full-CAO DOCUMENT, organized by agreement term (cao × ingangsdatum) — all NON-SALARY scores (per-topic combined z & pctile, numeric/coverage breakdown) + term bookkeeping (term_edition_seq orders the files within a term by file_date) + ready-made start dates: Date_first_is_Ingangsdatum = first-file-of-term starts at ingangsdatum, later files at their file_date; Date_retro_datum = same, overridden by the explicit retro_start_date where one exists. NO WAGE HERE: at document grain wage is a CAO-year fact (a year's ladder pools scales from several files), so the overall roll-ups EXCLUDE wage and are named *_without_wage — wage lives in the Wage tab (next tab, cao × year): as-of join it on the calendar year when building a panel (carry last known year forward). PanelMonthly keeps the wage-inclusive overall_z. (Replaces the former Composite and PanelMonthlyFirst tabs, 2026-07-15.)", C_HEAD),
    ("Wage", "mw_indices.csv",
     "COMPANION TABLE to AgreementLevel: the wage-scale ladder per cao × year (parser dataset, tier A+B, adult): p10..p90, mean, entry, WML ratios, wage z. The year = the wage table's OWN effective date as printed in the CAO ('per 1 July 2023') — NOT the filing date and NOT the agreement's ingangsdatum, so the wage series is already on its natural retroactive axis. One document usually states the agreed tables for SEVERAL future years — so the panel joins wage by CALENDAR YEAR, not by document: expand AgreementLevel rows by your chosen start-date column, then merge as-of on year (carry the last known year forward) to reproduce PanelMonthly's wage columns exactly.", C_DATA),
    ("FactorOverall", "factor_loadings_overall.csv",
     "FACTOR ANALYSIS = finding hidden dimensions behind correlated scores. Do the TOPIC scores share ONE common 'generosity' factor? THREE independent blocks (see the section column, separated by thick lines): COMBINED, NUMERIC, COVERAGE. Mostly no single factor — topics are fairly independent.", C_ANALYSIS),
    ("FactorLoadings", "factor_loadings_by_topic.csv",
     "FACTOR ANALYSIS within each topic: do a topic's FIELDS move together? Loadings of each field's z + communality (share the factors explain). Wage-ladder rows (topic=wage: LEVEL vs SPREAD) appended at the bottom.", C_ANALYSIS),
    ("FactorScores", "factor_scores.csv",
     "Each CAO document placed on the 1-3 hidden 'generosity dimensions' from FactorOverall (a coordinate per factor). High overall_factor1 = scores high on whatever mix of topics factor 1 captures.", C_ANALYSIS),
    ("FieldWeighting", "stage1_weighting.csv",
     "The index is built in 2 stages: STAGE 1 = combine a topic's FIELDS into its score (stage 2 = combine topics). Does our EQUAL-WEIGHT average match a data-driven (PCA/factor) weighting? Done per TRACK (see the track column): NUMERIC fields, and the COVERAGE booleans. High corr → equal weights are fine. (Combined field-weighting = the FactorMixed/FAMD tab.)", C_ANALYSIS),
    ("FactorMixed", "famd_mixed_loadings.csv",
     "FAMD = factor analysis of MIXED data: numeric amounts + yes/no provisions together, per topic. Numerics standardised, booleans MCA-scaled so neither dominates. Same-factor + same-sign numeric & boolean ⇒ having the provision goes with larger amounts.", C_ANALYSIS),
    ("CoverageVsLevel", "coverage_vs_z_correlations.csv",
     "Are CAOs generous on a topic (GOOD & MANY provisions) also the ones that PAY more? Per topic: correlation of the topic's combined / coverage / numeric score with the CAO's wage (Pearson + Spearman). Mostly weak.", C_ANALYSIS),
    ("Statutory", "statutory_index.csv",
     "The LAW as its own file: era floors/defaults, EXTRAS at 0, coverage 0 — WML folded in. Scores | thick line | values.", C_DATA),
    ("WageWorkerType", "mw_by_worker_type.csv",
     "Secondary wage split by worker_type label (~15% of points labelled — partial by construction).", C_DATA),
    ("Leave", "parental_leave_index.csv",
     "FRE-weeks leave package per document + leave scores + leave coverage (9 parental/holiday booleans; the 3 "
     "sickness/care top-ups live in Absence).", C_DATA),
] + [(t.capitalize(), f"{t}_index.csv",
      TOPIC_TAB_SUMMARY.get(t, f"{t} index per document: numeric z (size), per-field z, and coverage (breadth)."),
      C_DATA)
     for t in TOPICS]

# WML merged into Statutory; WageStructure merged into FactorLoadings; CoverageBundles dropped
# (exploratory-only, not used by the index) — no standalone tabs. Composite + PanelMonthlyFirst
# tabs retired 2026-07-15: AgreementLevel is a strict superset of Composite (same 2,698 rows,
# all its columns, cell-identical), and the first-file axis is the Date_first_is_Ingangsdatum
# column there (composite_index.csv itself stays — the pipeline reads it).
DROP_TABS = {"WML", "WageStructure", "CoverageBundles", "Composite", "PanelMonthlyFirst"}
# renamed tabs (old workbook name -> new) so a saved manual order still maps
TAB_RENAMES = {"Stage1Weighting": "FieldWeighting"}


def _existing_sheet_order(path):
    """Read the tab order already saved in the workbook so a manual reorder in Excel survives a
    rebuild. Old tab names are mapped through TAB_RENAMES. Returns [] if the file can't be read."""
    try:
        from openpyxl import load_workbook as _lw
        wbx = _lw(path, read_only=True)
        names = list(wbx.sheetnames)
        wbx.close()
        return [TAB_RENAMES.get(n, n) for n in names]
    except Exception:
        return []


def numify(df):
    out = df.copy()
    for c in out.columns:
        col = out[c].replace({"": None, "nan": None, "None": None})
        conv = pd.to_numeric(col, errors="coerce")
        nn = col.notna()
        if nn.sum() and bool(conv[nn].notna().all()):
            out[c] = conv
        else:
            out[c] = col
    return out


# ---------------------------------------------------------------------------
# DECK — a blue title slide
# ---------------------------------------------------------------------------
def add_deck(wb, present):
    ws = wb.create_sheet("Deck")
    ws.sheet_view.showGridLines = False
    ws.sheet_properties.tabColor = _tabcolor(C_DECK)
    NCOL = 4
    fill = PatternFill("solid", fgColor=DECK_BG)

    def put(r, c, text, bold=False, size=11, color="FFFFFF", span=1):
        cell = ws.cell(row=r, column=c, value=text)
        cell.font = Font(name=ARIAL, bold=bold, size=size, color=color)
        cell.alignment = Alignment(wrap_text=True, vertical="top")
        if span > 1:
            ws.merge_cells(start_row=r, start_column=c, end_row=r, end_column=c + span - 1)
        return cell

    body = [
        ("Dutch CAO Generosity Indices", 22, True, "FFFFFF"),
        ("v2 workbook — how generous is each collective agreement, and on what?", 12, False, "D6E0F0"),
        ("", 6, False, "FFFFFF"),
        ("WHAT THIS MEASURES", 12, True, "9DC3FF"),
        ("One row = one full-CAO document (AgreementLevel & topic tabs) or one cao×month (PanelMonthly). "
         "Every score is signed so HIGHER = MORE WORKER-GENEROUS, always.", 11, False, "FFFFFF"),
        ("", 6, False, "FFFFFF"),
        ("TWO SCALES for every score  (METHODOLOGY.md §3)", 12, True, "9DC3FF"),
        ("•  z — pooled, winsorised, ±3-clipped standardisation. Reads in standard deviations from the "
         "pool average (0 = a typical CAO). This is the factor-analysis input.", 11, False, "FFFFFF"),
        ("•  pctile — EQUAL-RANGE percentile (0–1). Every field on the same symmetric range, so 0 = least "
         "and 1 = most generous. The ranking scale.", 11, False, "FFFFFF"),
        ("", 6, False, "FFFFFF"),
        ("THREE TRACKS per topic  (each on both scales above)", 12, True, "9DC3FF"),
        ("•  numeric  ({t}_numeric_z / _numeric_pctile) — how MUCH a provision gives (numeric fields).", 11, False, "FFFFFF"),
        ("•  coverage ({t}_coverage_z / _coverage_pctile) — how MANY provisions exist (yes/no booleans → share).", 11, False, "FFFFFF"),
        ("•  combined = the HEADLINE {t}_z / {t}_pctile — the mean of numeric & coverage. Never blank when either "
         "track exists, so use this as the topic score.", 11, False, "FFFFFF"),
        ("   (Single-track topics: wage = numeric only; safety, childcare, ai = coverage only — {t}_z is that one track. All other topics, incl. absence, are dual numeric+coverage.)", 10, False, "D6E0F0"),
        ("", 6, False, "FFFFFF"),
        ("PANEL LAYOUT", 12, True, "9DC3FF"),
        ("•  Headline combined {t}_z / {t}_pctile sit on the LEFT; the numeric & coverage breakdown is pushed to "
         "the FAR RIGHT behind a thick line, with light-blue headers.", 11, False, "FFFFFF"),
        ("", 6, False, "FFFFFF"),
        ("READING RULES", 12, True, "9DC3FF"),
        ("•  STATUTORY rows = the law as its own file: legal minimum where it exists, 0 on every extra, coverage 0.", 11, False, "FFFFFF"),
        ("", 4, False, "FFFFFF"),
        ("BLANK, FILLED, OR ZERO?  Every numeric (magnitude) cell follows this ladder (METHODOLOGY.md §7.2):", 11, True, "9DC3FF"),
        ("•  1. The CAO states a number → that value is scored. (A stated value BELOW a hard legal floor is "
         "lifted TO the floor — e.g. <20 vacation days.)", 11, False, "FFFFFF"),
        ("•  2. Blank + the field has a statutory floor/default at the file's date → filled with the LAW's value. "
         "Only 12 of 39 fields have one (20 vacation days, 8% vakantiegeld, 70% sick pay, keten defaults, ATW rest…).", 11, False, "FFFFFF"),
        ("•  3. Blank + an EXTRA (above-statutory) perk whose presence boolean is FALSE in every edition of the CAO → 0. "
         "The boolean CONFIRMS the perk is absent, so absence ranks as least generous. Applies to: 13th month, "
         "meal / relocation / commuting, WFH stipend, shift allowance, training days/budget/reimbursement.", 11, False, "FFFFFF"),
        ("•  4. Blank + presence boolean TRUE → stays BLANK: the perk EXISTS but no amount is stated. The field is "
         "dropped from the topic mean (available-case) — an amount is never guessed.", 11, False, "FFFFFF"),
        ("•  5. Blank anywhere else → stays BLANK. No number is ever invented.", 11, False, "FFFFFF"),
        ("", 4, False, "FFFFFF"),
        ("PENSION — the one hard exception: blanks are NEVER filled (no statutory value, no zero).", 11, True, "9DC3FF"),
        ("   Most CAOs delegate pensions to a MANDATORY industry fund (ABP, PFZW, PMT, bpfBOUW, … — verplichtstelling, "
         "Wet Bpf 2000). The FUND, not the CAO, sets the contribution split, accrual rate, franchise and retirement age, "
         "so the CAO text states no figures. A blank pension cell therefore means 'ruled by the fund' — NOT 'no pension' "
         "and NOT the legal minimum — and filling it would fabricate a number. Consequence: the numeric pension score "
         "covers only the minority of CAOs that state their own terms (often company schemes); the fund's existence is "
         "visible in pension COVERAGE instead (mandatory participation, excedent top-up booleans).", 10, False, "D6E0F0"),
        ("", 4, False, "FFFFFF"),
        ("•  A blank NUMERIC score never blanks the topic: the COVERAGE booleans still score it, so the combined "
         "{t}_z stays populated.  ai is excluded from the overall (99% of CAOs have no AI clause yet).", 11, False, "FFFFFF"),
        ("•  THIN DOCUMENTS (thin_doc=TRUE): a few editions typed 'full CAO' are really an appendix, an umbrella "
         "(mantel) doc deferring to companion regulations, a deviations-only delta, or a failed extraction — each "
         "SOURCE-VERIFIED as such. Scoring their near-empty text would read as 'all provisions abolished', so their "
         "scores are carried from the last full edition and marked (scores_carried_from). New candidates are never "
         "carried automatically — they are surfaced for source review first.", 11, False, "FFFFFF"),
        ("•  Hover any column header (red corner) to read its definition; the Dictionary tab lists them all.", 11, False, "FFFFFF"),
        ("", 6, False, "FFFFFF"),
        ("TAB COLOUR KEY  (tab strip, dark → light)", 12, True, "9DC3FF"),
        ("■ darkest = this Deck   ■ dark = PanelMonthly & AgreementLevel   ■ mid = factor/analysis tabs   "
         "■ light = Statutory, Wage & the per-topic tabs.", 11, False, "FFFFFF"),
        ("", 6, False, "FFFFFF"),
        ("TAB INDEX", 12, True, "9DC3FF"),
    ]
    r = 1
    for text, size, bold, color in body:
        put(r, 1, text, bold=bold, size=size, color=color, span=NCOL)
        r += 1
    # tab index: name + summary
    for nm, _f, summ, _c in present:
        put(r, 1, nm, bold=True, size=10, color="9DC3FF")
        put(r, 2, summ, size=10, color="FFFFFF", span=NCOL - 1)
        r += 1
    put(r + 1, 1, "Dictionary", bold=True, size=10, color="9DC3FF")
    put(r + 1, 2, "Every column of every tab — what it is, its kind, and how it is built.",
        size=10, color="FFFFFF", span=NCOL - 1)
    last = r + 1
    # paint the slide background
    for rr in range(1, last + 2):
        for cc in range(1, NCOL + 1):
            ws.cell(row=rr, column=cc).fill = fill
        ws.row_dimensions[rr].height = 15
    ws.row_dimensions[1].height = 30
    ws.column_dimensions["A"].width = 22
    ws.column_dimensions["B"].width = 60
    ws.column_dimensions["C"].width = 40
    ws.column_dimensions["D"].width = 40
    print("  tab Deck: blue slide")


# ---------------------------------------------------------------------------
# generic data tab
# ---------------------------------------------------------------------------
def _variable_desc(df):
    """Name the row-label column sensibly and add a plain-English description beside it,
    so tabs whose ROWS are variables (WageStructure's ratio_mean_wml, factor loadings, …)
    explain themselves."""
    df = df.rename(columns={c: "variable" for c in df.columns if str(c).startswith("Unnamed")})
    for label in ("variable", "field"):
        if label in df.columns and f"{label}_desc" not in df.columns:
            pos = df.columns.get_loc(label) + 1
            df.insert(pos, f"{label}_desc", df[label].map(lambda v: define(str(v))))
            break
    return df


def _write_table(ws, df, freeze_col=3, thick_before=None, light_from=None):
    """Row 1 = title (already set by caller), row 2 = header w/ hover notes, then data.
    thick_before / light_from = 1-based column at which the breakdown block starts
    (thick separator line + light-blue header cells)."""
    hrow = 2
    for c, col in enumerate(df.columns, 1):
        cell = ws.cell(row=hrow, column=c, value=col)
        breakdown = light_from is not None and c >= light_from
        cell.font = Font(name=ARIAL, bold=True, color=("1F4E78" if breakdown else "FFFFFF"), size=9)
        cell.fill = LIGHT_HDR_FILL if breakdown else HDR_FILL
        cell.alignment = Alignment(horizontal="center", wrap_text=True, vertical="center")
        cm = Comment(f"{col}\n\n{define(col)}", "indices")
        cm.width, cm.height = 320, 150
        cell.comment = cm
    for row in df.itertuples(index=False):
        ws.append(list(row))
    for c in range(1, len(df.columns) + 1):
        w = max(10, min(24, len(str(df.columns[c - 1])) + 2))
        ws.column_dimensions[get_column_letter(c)].width = w
    ws.row_dimensions[hrow].height = 42
    if thick_before is not None:
        for rr in range(hrow, ws.max_row + 1):
            ws.cell(row=rr, column=thick_before).border = Border(left=THICK)
    ws.freeze_panes = ws.cell(row=hrow + 1, column=freeze_col)
    ws.auto_filter.ref = f"A{hrow}:{get_column_letter(len(df.columns))}{ws.max_row}"


_KEYSET = {"cao_number", "id", "file_date", "ingangsdatum", "year", "file_name",
           "document_type", "term_group", "doc_is_newest", "pub_lag_months", "is_full_cao"}
_SCORE_SUF = ("_numeric_z", "_numeric_z_raw", "_numeric_z_availcase", "_numeric_rankpct",
              "_numeric_pctile", "_coverage", "_coverage_z", "_coverage_pctile", "_coverage_n",
              "_pay_z", "_protection_z", "_pay_prank", "_protection_prank",
              "_numeric_z_extr", "_numeric_z_fre_total")


def _topic_scores_first(df, topic):
    """Topic detail tab: put the topic-level headline scores right after the key columns, before
    the raw input fields and per-field z's (main results first, then granular)."""
    keys = [c for c in df.columns if c in _KEYSET]
    scores = [c for c in df.columns if c.startswith(f"{topic}_") and c.endswith(_SCORE_SUF)]
    rest = [c for c in df.columns if c not in keys and c not in scores]
    return df[keys + scores + rest]


def _add_section_lines(ws, df, col, hrow=2):
    """Draw a horizontal thick line between row-blocks (e.g. FactorOverall's combined/numeric/
    coverage sections) — a top border on the first row of each new block."""
    if col not in df.columns:
        return
    vals = df[col].tolist()
    ncol = len(df.columns)
    for i in range(1, len(vals)):
        if vals[i] != vals[i - 1]:
            r = hrow + 1 + i
            for c in range(1, ncol + 1):
                b = ws.cell(row=r, column=c).border
                ws.cell(row=r, column=c).border = Border(left=b.left, right=b.right,
                                                          bottom=b.bottom, top=THICK)


def _communality_first(df):
    """Coherence: in every factor-loading tab put `communality` right after the label columns
    (before the f1/f2/… loadings), so FactorLoadings / FactorOverall / WageStructure read alike."""
    if "communality" not in df.columns:
        return df
    cols = [c for c in df.columns if c != "communality"]
    labels = [c for c in ("topic", "variable", "variable_desc", "field", "field_desc",
                          "type", "section", "analysis") if c in cols]
    pos = (max(cols.index(c) for c in labels) + 1) if labels else 0
    cols.insert(pos, "communality")
    return df[cols]


def _breakdown_layout(df, breakdown):
    """Move breakdown columns to the far right; return (df, first_breakdown_col_1based|None)."""
    bd = [c for c in breakdown if c in df.columns]
    if not bd:
        return df, None
    main = [c for c in df.columns if c not in bd]
    return df[main + bd], len(main) + 1


def add_tab(wb, name, df, summary, color, breakdown=None, freeze_col=3, breakdown_note=None):
    df = _variable_desc(df)
    ws = wb.create_sheet(name)
    ws.sheet_properties.tabColor = _tabcolor(color)
    tail = "" if not breakdown else (
        breakdown_note or " · numeric/coverage breakdown to the right (light-blue, behind the thick line)")
    ws["A1"] = (f"{name} — {summary}   [{len(df)} rows · hover a header for its definition · "
                f"full list on the Dictionary tab{tail}]")
    ws["A1"].font = Font(name=ARIAL, bold=True, size=10, color="1F4E78")
    start = None
    if breakdown:
        df, start = _breakdown_layout(df, breakdown)
    _write_table(ws, df, freeze_col=freeze_col, thick_before=start, light_from=start)
    print(f"  tab {name}: {len(df)} rows x {len(df.columns)} cols" + (f" (breakdown @ col {start})" if start else ""))


# ---------------------------------------------------------------------------
# NUMERIC / COVERAGE / COMBINED naming scheme
# ---------------------------------------------------------------------------
def _nid(x):  # normalise an id to a bare string ('10002.0'/'10002'/nan -> '10002'/'')
    if pd.isna(x):
        return ""
    s = str(x)
    return s[:-2] if s.endswith(".0") else s


def apply_scheme(df):
    """Delegate to the single canonical implementation (index_lib.apply_scheme). A local copy
    used to live here and DRIFTED — it listed absence in the single-track drop tuple while DUAL
    treats absence as dual, so on raw internal names it would have silently deleted the absence
    combined headline. il.apply_scheme (driven by il.DUAL_TOPICS, absence included) is correct
    and idempotent on already-published frames."""
    return il.apply_scheme(df)


def topic_breakdown_cols():
    """The per-dual-topic numeric/coverage z & pctile columns pushed to the far right."""
    return [f"{t}{suf}" for t in DUAL for suf in BREAKDOWN_SUFFIXES]


def build_panel(panel, comp):
    """PanelMonthly: the panel CSV now already carries the combined headline {t}_z, the
    coverage z's, and the combined overall_z / overall_z_var for BOTH CAO and statutory rows
    (build_panel_monthly.py). Here we only graft the PCTILE breakdown (not stored in the panel
    CSV) from the in-force document and form the combined {t}_pctile. Combined = mean of the
    parts available on the row (statutory rows have no in-force doc, so no pctile)."""
    p = panel.copy()
    p["in_force_id"] = p["in_force_id"].map(_nid)
    # graft numeric_pctile / coverage_pctile from the in-force document (composite). Thin docs
    # need no special key: their composite row ALREADY holds the carried-from edition's pctiles
    # (apply_thin_doc_carry substitutes the full score row). coverage_z and the combined {t}_z
    # are ALREADY in the panel CSV, so we do NOT re-graft/recompute them.
    join = pd.DataFrame({"in_force_id": comp["id"].map(_nid)})
    for t in DUAL:
        for src in (f"{t}_numeric_pctile", f"{t}_coverage_pctile"):
            if src in comp.columns:
                join[src] = pd.to_numeric(comp[src], errors="coerce").values
    for extra in ("wage_median_pctile", "wage_mean_pctile"):
        if extra in comp.columns:                  # wage pctiles (no coverage to combine)
            join[extra] = pd.to_numeric(comp[extra], errors="coerce").values
    p = p.merge(join.drop_duplicates("in_force_id"), on="in_force_id", how="left")
    for t in DUAL:                                               # combined pctile = mean of parts
        pparts = [c for c in (f"{t}_numeric_pctile", f"{t}_coverage_pctile") if c in p.columns]
        if pparts:
            p[f"{t}_pctile"] = p[pparts].apply(pd.to_numeric, errors="coerce").mean(axis=1).round(4)
    return _dashboard_order(p)


def _dashboard_order(df):
    """Aggregate-first layout used by AgreementLevel & PanelMonthly. The COMBINED roll-ups
    (overall_z / overall_pctile = combined numeric + coverage) are the PRIMARY headline at the front;
    the numeric-only roll-ups (overall_numeric_z, overall_numeric_pctile), the per-topic coverage
    shares, counts, and the numeric/coverage breakdown all go to the LIGHT (right) section.
    keys → primary aggregates → per-topic headline z's → headline pctiles → wage/level detail →
    [light] numeric roll-ups + counts + coverage shares + numeric/coverage breakdown.
    Returns (ordered_df, light_cols)."""
    KEYS = ["cao_number", "id", "month", "in_force_id", "thin_doc", "scores_carried_from",
            "score_src_id", "file_date", "ingangsdatum",
            "Date_first_is_Ingangsdatum", "Date_retro_datum",   # AgreementLevel only
            "pub_lag_months",
            "term_group", "document_type", "n_caos_in_month", "doc_is_newest"]
    AGG = ["overall_z", "overall_z_without_wage", "overall_pctile", "overall_pctile_without_wage",
           "coverage_overall", "overall_z_var", "overall_z_var_without_wage"]
    HTOPICS = DUAL + ["wage_median"]                            # topics with a headline z & pctile
    HEAD_Z = [f"{t}_z" for t in HTOPICS]
    HEAD_P = [f"{t}_pctile" for t in HTOPICS]
    WAGE = ["wage_mean_z", "wage_mean_pctile", "mw_low", "mw_median", "mw_mean", "mw_high",
            "ratio_low_wml", "ratio_median_wml", "ratio_mean_wml", "ratio_high_wml", "wml_month"]
    # light section (right, light-blue headers): numeric-only roll-ups, counts, coverage shares, breakdown
    COV_ORDER = ["safety_coverage", "training_coverage", "bonus_coverage", "fringe_coverage",
                 "homeoffice_coverage", "pension_coverage", "childcare_coverage", "leave_coverage",
                 "absence_coverage"]
    covshare = [c for c in COV_ORDER if c in df.columns] + \
               [c for c in df.columns if c.endswith("_coverage") and c not in COV_ORDER]
    counts = [c for c in ("n_topics_scored", "n_topics_scored_without_wage",
                          "n_topics_combined", "n_topics_combined_without_wage",
                          "n_topics_pctile", "n_topics_pctile_without_wage") if c in df.columns]
    num_roll = [c for c in ("overall_numeric_z", "overall_numeric_z_without_wage",
                            "overall_numeric_z_var", "overall_numeric_z_var_without_wage",
                            "overall_numeric_pctile", "overall_numeric_pctile_without_wage",
                            "coverage_overall_z", "coverage_overall_pctile") if c in df.columns]
    light = num_roll + counts + covshare + [c for c in topic_breakdown_cols() if c in df.columns]
    order = []
    for grp in (KEYS, AGG, HEAD_Z, HEAD_P, WAGE):
        order += [c for c in grp if c in df.columns and c not in order]
    order += [c for c in df.columns if c not in order and c not in light]
    order += [c for c in light if c not in order]
    return df[order], light


# ---------------------------------------------------------------------------
# STATUTORY (+ WML) — scores | thick line | values
# ---------------------------------------------------------------------------
def add_statutory(wb, df, wml, summary, color):
    # fold WML in: month-map the statutory-minimum-wage series onto each month
    s = df.copy()
    s["_m"] = pd.to_datetime(s["month"] + "-01", errors="coerce")
    w = wml.copy()
    w["_eff"] = pd.to_datetime(w["effective_from"], errors="coerce")
    w = w.dropna(subset=["_eff"]).sort_values("_eff")
    merged = pd.merge_asof(s.sort_values("_m"), w[["_eff", "wml_month_eur", "wml_hour_eur", "please_verify"]],
                           left_on="_m", right_on="_eff", direction="backward")
    merged = merged.rename(columns={"please_verify": "wml_please_verify"})
    merged = merged.sort_values("month").drop(columns=["_m", "_eff"])

    cols = list(merged.columns)
    keys = [c for c in ["month", "n_topics_scored"] if c in cols]
    mag_z = [c for c in cols if c.endswith("_z") and not c.endswith("_coverage_z")
             and not c.startswith("stat_")]
    cov = [c for c in cols if c.endswith("_coverage_z")] + \
          [c for c in ["coverage_overall", "coverage_overall_pctile"] if c in cols]
    values = [c for c in cols if c.startswith("stat_")] + \
             [c for c in ["wml_month_eur", "wml_hour_eur", "wml_please_verify"] if c in cols]
    # overall_numeric_z leads the scores
    lead = [c for c in ["overall_numeric_z"] if c in cols]
    mag_z = [c for c in mag_z if c not in lead]
    order = keys + lead + sorted(mag_z) + cov + values
    order += [c for c in cols if c not in order]  # safety: keep any stragglers
    merged = merged[order]

    thick_at = len(keys + lead + sorted(mag_z) + cov) + 1  # 1-based col of first value

    ws = wb.create_sheet("Statutory")
    ws.sheet_properties.tabColor = _tabcolor(color)
    ws["A1"] = (f"Statutory — {summary}   [{len(merged)} months · SCORES left | thick line | VALUES right · "
                f"WML merged in · hover a header for its definition]")
    ws["A1"].font = Font(name=ARIAL, bold=True, size=10, color="1F4E78")
    _write_table(ws, merged, freeze_col=2, thick_before=thick_at)
    print(f"  tab Statutory: {len(merged)} rows x {len(merged.columns)} cols (WML merged; thick line @ col {thick_at})")


# ---------------------------------------------------------------------------
# DICTIONARY — every column of every tab
# ---------------------------------------------------------------------------
def add_dictionary(wb, tab_dfs):
    ws = wb.create_sheet("Dictionary")
    ws.sheet_properties.tabColor = _tabcolor(C_DICT)
    ws.sheet_view.showGridLines = False
    ws["A1"] = "Data dictionary — every column of every tab: what it is, its kind, and how it is built."
    ws["A1"].font = Font(name=ARIAL, bold=True, size=12, color="1F4E78")
    hdr = ["Tab", "Column", "Kind", "Definition"]
    for c, h in enumerate(hdr, 1):
        cell = ws.cell(row=2, column=c, value=h)
        cell.font = Font(name=ARIAL, bold=True, color="FFFFFF", size=10)
        cell.fill = HDR_FILL
        cell.alignment = Alignment(horizontal="left", vertical="center")
    r = 3
    seen_vars = {}
    for tab, df in tab_dfs:
        for col in df.columns:
            ws.cell(row=r, column=1, value=tab).font = Font(name=ARIAL, size=9, color="808080")
            ws.cell(row=r, column=2, value=col).font = Font(name=ARIAL, bold=True, size=9)
            ws.cell(row=r, column=3, value=kind(col)).font = Font(name=ARIAL, size=9, italic=True)
            ws.cell(row=r, column=4, value=define(col)).font = Font(name=ARIAL, size=9)
            r += 1
        # document the row-variable tokens too (e.g. ratio_mean_wml appearing in a 'variable' column)
        if "variable" in df.columns:
            for v in df["variable"].dropna().unique():
                v = str(v)
                if v not in seen_vars:
                    seen_vars[v] = True
    if seen_vars:
        ws.cell(row=r, column=1, value="(row variables)").font = Font(name=ARIAL, bold=True, size=9, color="1F4E78")
        r += 1
        for v in sorted(seen_vars):
            ws.cell(row=r, column=2, value=v).font = Font(name=ARIAL, bold=True, size=9)
            ws.cell(row=r, column=3, value=kind(v)).font = Font(name=ARIAL, size=9, italic=True)
            ws.cell(row=r, column=4, value=define(v)).font = Font(name=ARIAL, size=9)
            r += 1
    for c, wdt in zip("ABCD", (16, 30, 14, 110)):
        ws.column_dimensions[c].width = wdt
    ws.freeze_panes = "A3"
    ws.auto_filter.ref = f"A2:D{ws.max_row}"
    print(f"  tab Dictionary: {r - 3} column definitions")


# ---------------------------------------------------------------------------
def main():
    # merged CSV (doc grain): composite spine + every topic's non-duplicate columns + leave
    comp = il.read_csv_safe(il.locate("composite_index.csv"))
    merged = comp.copy()
    for t in TOPICS:
        d = il.read_csv_safe(il.locate(f"{t}_index.csv"))
        add = d[["id"] + [c for c in d.columns if c not in merged.columns and c not in il.ID_COLS]]
        merged = merged.merge(add, on="id", how="left")
    leave = il.read_csv_safe(il.locate("parental_leave_index.csv"))
    add = leave[["id"] + [c for c in leave.columns
                          if c not in merged.columns and c not in il.ID_COLS and c != "is_full_cao"]]
    merged = merged.merge(add, on="id", how="left")
    out_csv = os.path.join(il.OUT, "all_indices_combined.csv")
    apply_scheme(merged).to_csv(out_csv, sep=";", index=False)   # same clear naming as the workbook
    print(f"wrote {out_csv} ({len(merged)} rows, {len(merged.columns)} cols)")

    comp_num = numify(comp)   # composite with numeric dtypes, for grafting onto the panel

    out_x = os.path.join(HERE, "all_indices.xlsx")
    existing_order = _existing_sheet_order(out_x)   # preserve a manual tab reorder across rebuilds

    present = [(n, f, s, c) for n, f, s, c in TABS
               if n not in DROP_TABS and os.path.exists(il.locate(f))]

    wb = Workbook(); wb.remove(wb.active)
    add_deck(wb, present)
    add_dictionary_later = []   # (tab, df) pairs for the Dictionary

    # build each data tab in display order
    wml_df = numify(il.read_csv_safe(il.locate("wml_timeline.csv")))
    for name, f, summary, color in present:
        raw = numify(il.read_csv_safe(il.locate(f)))
        drop_here = {c for c in DROP_COLS if c in raw.columns}
        if "year" in drop_here and "file_date" not in raw.columns:
            drop_here.discard("year")     # cao×year tabs (Wage/WageWorkerType) have no other date key
        raw = raw.drop(columns=list(drop_here))
        if name == "FactorLoadings":     # merge WageStructure in: wage-ladder loadings as bottom rows
            wfp = il.locate("wage_factor_loadings.csv")
            if os.path.exists(wfp):
                wf = numify(il.read_csv_safe(wfp))
                wf = wf.rename(columns={c: "field" for c in wf.columns if str(c).startswith("Unnamed")})
                wf.insert(0, "topic", "wage")
                raw = pd.concat([raw, wf], ignore_index=True)
        if name == "Statutory":
            add_statutory(wb, raw, wml_df, summary, color)
            disp = raw.copy()
            for extra in ("wml_month_eur", "wml_hour_eur", "wml_please_verify"):
                disp[extra] = None
            add_dictionary_later.append((name, disp))
            continue
        if name == "PanelMonthly":
            raw, breakdown = build_panel(raw, comp_num)
            add_tab(wb, name, raw, summary, color, breakdown=breakdown)
            add_dictionary_later.append((name, raw))
            continue
        if name == "AgreementLevel":
            raw, light = _dashboard_order(raw)
            add_tab(wb, name, raw, summary, color, breakdown=light)
            add_dictionary_later.append((name, raw))
            continue
        if name == "Wage":       # panel-mirroring layout: the wage block PanelMonthly carries
            raw = apply_scheme(raw)              # (dark, left) | extra wage detail (light, right)
            KEYS_W = [c for c in ("cao_number", "year") if c in raw.columns]
            PANELW = [c for c in ("wage_median_z", "wage_mean_z", "mw_low", "mw_median",
                                  "mw_mean", "mw_high", "ratio_low_wml", "ratio_median_wml",
                                  "ratio_mean_wml", "ratio_high_wml", "wml_month")
                      if c in raw.columns]
            light = [c for c in raw.columns if c not in KEYS_W + PANELW]
            raw = raw[KEYS_W + PANELW + light]
            add_tab(wb, name, raw, summary, color, breakdown=light,
                    breakdown_note=" · left of the thick line = the columns carried into "
                                   "PanelMonthly (and to join onto AgreementLevel); right "
                                   "(light-blue) = extra wage detail (quartiles, entry scales, "
                                   "spread, diagnostics)")
            add_dictionary_later.append((name, raw))
            continue
        raw = _variable_desc(apply_scheme(raw))   # gen01 -> pctile everywhere
        raw = _communality_first(raw)             # coherent column order across factor tabs
        _topic = "leave" if name == "Leave" else (name.lower() if name.lower() in TOPICS else None)
        if _topic:                                # topic detail tabs: headline scores first
            raw = _topic_scores_first(raw, _topic)
        add_tab(wb, name, raw, summary, color)
        if name == "FactorOverall":               # thick line between the 3 section blocks
            _add_section_lines(wb[name], raw, "section")
        add_dictionary_later.append((name, raw))

    add_dictionary(wb, add_dictionary_later)

    if existing_order:                              # honour the workbook's saved tab order
        want = [n for n in existing_order if n in wb.sheetnames]
        newbies = [n for n in wb.sheetnames if n not in existing_order]   # e.g. a newly-added tab
        tab_order = [n for n, *_ in TABS]           # place each newbie at its TABS-list position:
        for nb in newbies:                          # right after the nearest preceding tab that
            anchor = None                           # already exists in the saved order
            if nb in tab_order:
                for prev in reversed(tab_order[:tab_order.index(nb)]):
                    if prev in want:
                        anchor = prev; break
            if anchor is not None:
                want.insert(want.index(anchor) + 1, nb)
            elif "Dictionary" in want:              # fallback: just before Dictionary
                want.insert(want.index("Dictionary"), nb)
            else:
                want.append(nb)
        wb._sheets = [wb[n] for n in want]

    wb.save(out_x)
    print(f"wrote {out_x} ({len(wb.sheetnames)} tabs)")


if __name__ == "__main__":
    main()
