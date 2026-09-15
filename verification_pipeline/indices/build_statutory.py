"""Build the consolidated statutory reference: one nicely-formatted multi-tab workbook
(statutory_timeline.xlsx) + one flat machine-readable CSV (statutory_all.csv) that the
indices read. Single source of truth = the DATA list below. Timelines run 1999->today;
each row is a change-point with the value, the statutory law, and a source link.

Sources verified 2026-06 via gov't / official portals; law NAMES are authoritative,
topic-hub links point at rijksoverheid.nl/belastingdienst/officielebekendmakingen.
Run: python3.13 qa/indices/build_statutory.py"""
import os, csv, sys
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import index_lib as il

# CONCRETE source links — the statute on wetten.overheid.nl (BWBR) or the Staatsblad
# of the specific amendment. Verified via search 2026-06.
S = {
    "wazo": "https://wetten.overheid.nl/BWBR0013008",        # Wet arbeid en zorg (WAZO)
    "wieg": "https://wetten.overheid.nl/BWBR0041646",        # Wet invoering extra geboorteverlof (WIEG)
    "ouderschap": "https://wetten.overheid.nl/BWBR0013008",  # ouderschapsverlof = WAZO
    "bopl": "https://wetten.overheid.nl/BWBR0046019",        # Wet betaald ouderschapsverlof (Stb 2022,174)
    "bw": "https://wetten.overheid.nl/BWBR0005290",          # Burgerlijk Wetboek Boek 7 (652/634/668a/672/673)
    "wwz": "https://wetten.overheid.nl/BWBR0035254",         # Wet werk en zekerheid (Stb 2014,216)
    "wab": "https://wetten.overheid.nl/BWBR0042307",         # Wet arbeidsmarkt in balans (Stb 2019,219)
    "ontslag": "https://www.rijksoverheid.nl/onderwerpen/ontslag/vraag-en-antwoord/hoe-hoog-is-de-transitievergoeding-als-ik-word-ontslagen",  # transitievergoeding max bedrag
    "atw": "https://wetten.overheid.nl/BWBR0007671",         # Arbeidstijdenwet
    "wml": "https://wetten.overheid.nl/BWBR0002638",         # Wet minimumloon en minimumvakantiebijslag
    "uurloon": "https://zoek.officielebekendmakingen.nl/stb-2023-168.html",  # Wet invoering minimumuurloon (2024)
    "witteveen": "https://www.eerstekamer.nl/wetsvoorstel/33610_wet_verlaging_maximumopbouw",  # Witteveen 1,875% (2015)
    "wtp": "https://wetten.overheid.nl/BWBR0048328",         # Wet toekomst pensioenen (2023)
    "aow": "https://www.eerstekamer.nl/wetsvoorstel/33290_wet_verhoging_aow_en",  # Wet verhoging AOW-leeftijd (Stb 2012,328)
    "aow33290": "https://www.eerstekamer.nl/wetsvoorstel/33290_wet_verhoging_aow_en",
    "pensioen": "https://www.eerstekamer.nl/wetsvoorstel/33610_wet_verlaging_maximumopbouw",  # Witteveen
    "reis": "https://www.rijksfinancien.nl/belastingplan-memorie-van-toelichting/2024/d17e1932",  # Belastingplan 2024 (reiskosten 0,23)
    "scholing": "https://zoek.officielebekendmakingen.nl/stb-2022-277.html",  # Wet transp. en voorspelbare arbeidsvoorwaarden
    "wfw": "https://wetten.overheid.nl/BWBR0011173",         # Wet flexibel werken
    "arbo": "https://wetten.overheid.nl/BWBR0010346",        # Arbeidsomstandighedenwet (Arbowet)
    "kinderopvang": "https://wetten.overheid.nl/BWBR0017017",  # Wet kinderopvang
}

# topic, variable, machine_field, role, from, to, value, unit, law, source, notes
# role drives the index ONLY for cap/floor/default; informational/none = documentation.
DATA = [
 # ---------- LEAVE (WAZO / WIEG / Wet betaald ouderschapsverlof) ----------
 ("leave","Maternity leave (bevallingsverlof)","","floor","1999-01-01","(current)","16","weeks @100%","WAZO art. 3:1",S["wazo"],"Pregnancy + maternity; UWV pays 100% up to max dagloon. Stable across the window."),
 ("leave","Paternity / birth leave (geboorteverlof)","","floor","1999-01-01","2018-12-31","2","days @100%","kraamverlof (pre-WIEG)",S["wieg"],"2 paid days at birth."),
 ("leave","Paternity / birth leave (geboorteverlof)","","floor","2019-01-01","2020-06-30","1","week @100%","WIEG (Wet invoering extra geboorteverlof)",S["wieg"],"1 week employer-paid (1x weekly working hours), from 1 Jan 2019."),
 ("leave","Paternity supplementary (aanvullend geboorteverlof)","","floor","2020-07-01","(current)","5","weeks @70%","WIEG",S["wieg"],"Up to 5 extra weeks @70% via UWV, from 1 Jul 2020 (on top of the 1 paid week)."),
 ("leave","Adoption / foster leave (adoptieverlof)","","floor","1999-01-01","2018-12-31","4","weeks @100%","WAZO",S["wazo"],"4 weeks."),
 ("leave","Adoption / foster leave (adoptieverlof)","","floor","2019-01-01","(current)","6","weeks @100%","WIEG",S["wieg"],"Raised 4 -> 6 weeks on 1 Jan 2019 (VERIFY exact date)."),
 ("leave","Parental leave (ouderschapsverlof)","","floor","1999-01-01","2022-08-01","26","weeks @0% (unpaid)","WAZO ouderschapsverlof",S["ouderschap"],"26x weekly working hours per parent, unpaid."),
 ("leave","Parental leave - paid portion","","floor","2022-08-02","(current)","9","weeks @70%","Wet betaald ouderschapsverlof",S["bopl"],"9 of the 26 weeks paid @70% via UWV (if taken in first year), from 2 Aug 2022."),
 ("leave","Statutory paid holiday (vakantiedagen)","","floor","1999-01-01","(current)","4x weekly days","days/yr","BW 7:634",S["bw"],"Minimum = 4x the agreed weekly working days (~20 days for full-time)."),
 ("leave","Holiday allowance (vakantietoeslag)","","floor","1999-01-01","(current)","8","% of gross salary","WML art. 15",S["wml"],"Statutory minimum 8% holiday pay (vakantiegeld)."),
 # ---------- ABSENCE (vacation / sick pay / care leave — v2 Tier-1 fields, added 2026-07-05) ----------
 ("absence","Vacation days (vakantiedagen)","leave_vacation_time_value","floor_lift","1999-01-01","(current)","20","days/yr","BW 7:634",S["bw"],"Statutory minimum = 4x weekly working DAYS; encoded as 20 days at the 5-day full-time norm. Stable across the window."),
 ("absence","Holiday allowance % (vakantiebijslag)","leave_vacation_bonus_value","floor_lift","1999-01-01","(current)","8","percent of gross salary","WML art. 15",S["wml"],"8% statutory minimum holiday pay. (Art. 16: may be lower above 3x WML by written agreement — rare, ignored as floor.)"),
 ("absence","Sick pay continuation % (loondoorbetaling bij ziekte)","leave_sickpay_continuation_value","floor_lift","1999-01-01","(current)","70","percent of wage","BW 7:629",S["bw"],"70% of wage (year-1 minimum = WML; cap max dagloon). CAOs typically top up to 90-100% in year 1 (C3: first-tier value is stored)."),
 ("absence","Sick pay duration (loondoorbetaling)","leave_sickpay_duration_value","floor_lift","1999-01-01","2003-12-31","52","weeks","BW 7:629 (pre-VLZ)",S["bw"],"52 weeks employer-paid sick pay before 2004."),
 ("absence","Sick pay duration (loondoorbetaling)","leave_sickpay_duration_value","floor_lift","2004-01-01","(current)","104","weeks","Wet verlenging loondoorbetalingsverplichting bij ziekte (Stb. 2003, 555)","https://zoek.officielebekendmakingen.nl/stb-2003-555.html","Extended 52 -> 104 weeks from 1 Jan 2004 (VERIFY exact commencement)."),
 ("absence","Short-term care leave (kortdurend zorgverlof)","leave_short_term_care_value","floor_lift","2001-12-01","(current)","10","days/yr (=2x weekly hours)","WAZO art. 5:1",S["wazo"],"Max 2x the weekly working hours per 12 months (~10 working days full-time). Introduced with WAZO 1 Dec 2001 (VERIFY date). No statutory entitlement before."),
 ("absence","Short-term care leave pay","leave_short_term_care_pay_value","floor_lift","2001-12-01","(current)","70","percent of wage","WAZO art. 5:6",S["wazo"],"70% of wage (min WML) during kortdurend zorgverlof."),
 ("absence","Long-term care leave (langdurend zorgverlof)","leave_long_term_care_value","floor_lift","2005-06-01","(current)","6","weeks (=6x weekly hours per 12mo)","WAZO art. 5:9",S["wazo"],"6x weekly working hours per 12 months; introduced 1 Jun 2005 (VERIFY date); eligibility circle widened 1 Jul 2015 (Wet modernisering verlofregelingen)."),
 ("absence","Long-term care leave pay","leave_long_term_care_pay_value","floor_lift","2005-06-01","(current)","0","percent (unpaid)","WAZO art. 5:9",S["wazo"],"Statutorily UNPAID — any CAO pay is pure generosity."),
 # ---------- TERM/PENSION/CONTRACT/OVERTIME/HOMEOFFICE informational anchors for the
 # ---------- Tier-2/3 coverage booleans (documentation; NOT imputed) ------------------
 ("term","Dismissal ban during illness (opzegverbod bij ziekte)","","informational","1999-01-01","(current)","104","weeks","BW 7:670",S["bw"],"Statutory 2-year dismissal ban during illness -> the coverage boolean term_sick_dismissal_prot partly RESTATES law (like the Arbowet caveat in safety)."),
 ("pension","Mandatory sector-fund participation (verplichtstelling)","","informational","2001-01-01","(current)","mandatory","(non-numeric)","Wet verplichte deelneming in een bedrijfstakpensioenfonds 2000","https://wetten.overheid.nl/BWBR0012092","In verplichtgesteld-bpf sectors participation is mandatory BY LAW even if the CAO is silent -> pension_mandatory_participation coverage understates the legal reality."),
 ("contract","Right to request working-hours adjustment (WAA/Wfw)","","informational","2000-07-01","(current)","request right","(non-numeric)","Wet aanpassing arbeidsduur (2000) -> Wet flexibel werken (2016)",S["wfw"],"Statutory right to REQUEST more/fewer hours (employer refuses only on serious business grounds) -> contract_part_time_allowed partly restates law post-2000."),
 ("overtime","Overtime counts for WML; TVT only via CAO","","informational","2018-01-01","(current)","WML incl. overtime","(non-numeric)","WML amendment (Stb. 2017, 24)",S["wml"],"From 2018 overtime pay counts toward the WML (incl. 8% vakantiebijslag from 2019); time-for-time compensation for overtime under WML requires a CAO basis from 1 Jan 2019 (VERIFY detail)."),
 ("homeoffice","Untaxed home-working allowance (thuiswerkvergoeding)","","informational","2022-01-01","(current)","2.00 -> 2.15 -> 2.35 -> ~2.40","EUR/day","gerichte vrijstelling thuiswerkkosten (Belastingplan 2022)",S["reis"],"Fiscal norm, not an entitlement: EUR2.00/day (2022), 2.15 (2023), 2.35 (2024), ~2.40 (2025/26) — VERIFY the 2025/2026 values."),
 # ---------- TERM (BW / WWZ / WAB) ----------
 ("term","Probation max - fixed-term (proeftijd)","term_probation_fixedterm_value","cap","1999-01-01","(current)","2","months","BW 7:652",S["bw"],"Absolute max 2 months; bracket-specific lower limits by contract length. Flag values >2."),
 ("term","Probation max - permanent (proeftijd)","term_probation_indef_value","cap","1999-01-01","(current)","2","months","BW 7:652",S["bw"],"Max 2 months for a permanent contract. Flag values >2."),
 ("term","Probation banned in short contracts","","informational","2015-01-01","(current)","0","months (contracts <=6mo)","WWZ",S["wwz"],"From 1 Jan 2015 no probation allowed in contracts of <=6 months."),
 ("term","Employer notice period (opzegtermijn)","term_employer_notice_value","informational","1999-01-01","(current)","1|2|3|4","months","BW 7:672",S["bw"],"Tenure-graded: <5/5-10/10-15/>=15 yr = 1/2/3/4 months. Not a single scalar -> not imputed."),
 ("term","Employee notice period","term_employee_notice_value","floor","1999-01-01","(current)","1","months","BW 7:672",S["bw"],"1 month statutory. Dropped from the index (low signal)."),
 ("term","Severance (transitievergoeding) - intro","","informational","2015-07-01","2019-12-31","1/6 .. 1/4","month per half-year","WWZ",S["wwz"],"WWZ introduced the transitievergoeding (eligible after 24 months service)."),
 ("term","Severance (transitievergoeding) - formula","term_severance_extra_value","informational_formula","2020-01-01","(current)","0.333","months_salary per yr","BW 7:673 (WAB)",S["wab"],"WAB: 1/3 month per year of service from day one. Dataset field = CAO severance ABOVE this base."),
 ("term","Transitievergoeding annual cap","term_severance_extra_value","informational","2015-01-01","2015-12-31","75000","EUR","BW 7:673",S["ontslag"],"Annual maximum (or 1 gross annual salary if higher)."),
 ("term","Transitievergoeding annual cap","term_severance_extra_value","informational","2020-01-01","2020-12-31","83000","EUR","BW 7:673",S["ontslag"],"Indexed annually."),
 ("term","Transitievergoeding annual cap","term_severance_extra_value","informational","2024-01-01","2024-12-31","94000","EUR","BW 7:673",S["ontslag"],"2024 figure."),
 ("term","Transitievergoeding annual cap","term_severance_extra_value","informational","2025-01-01","2025-12-31","98000","EUR","BW 7:673",S["ontslag"],"2025 figure."),
 ("term","Transitievergoeding annual cap","term_severance_extra_value","informational","2026-01-01","(current)","102000","EUR","BW 7:673",S["ontslag"],"VERIFY 2026 figure (indexed annually)."),
 # ---------- CONTRACT (ketenregeling: BW 7:668a / WWZ / WAB) ----------
 ("contract","Chain rule max contracts (ketenregeling)","contract_ketenregeling_max_contracts_value","default","2015-07-01","(current)","3","contracts","WWZ / WAB",S["wab"],"Max 3 temp contracts before a permanent one. Stable at 3. CAO may vary."),
 ("contract","Chain rule max duration","contract_ketenregeling_max_duration_value","default","1999-01-01","2015-06-30","36","months","BW 7:668a (pre-WWZ)",S["bw"],"Pre-WWZ chain = 36 months. VERIFY pre-2015 handling."),
 ("contract","Chain rule max duration","contract_ketenregeling_max_duration_value","default","2015-07-01","2019-12-31","24","months","WWZ",S["wwz"],"WWZ shortened the chain to 24 months (1 Jul 2015)."),
 ("contract","Chain rule max duration","contract_ketenregeling_max_duration_value","default","2020-01-01","(current)","36","months","WAB",S["wab"],"WAB restored 36 months (1 Jan 2020). NON-MONOTONIC: 36 -> 24 -> 36."),
 ("contract","Chain-reset gap","contract_ketenregeling_gap_value","informational","2015-07-01","(current)","6","months","WWZ / WAB",S["wab"],"Interval that resets the chain (CAO may reduce to 3mo for recurring/seasonal work)."),
 ("contract","Full-time hours","contract_full_time_hours_value","none","","","","","(no statute)",S["atw"],"Sector norm 36-40 h/wk; not a statutory figure. Used as a normaliser, not scored."),
 # ---------- OVERTIME (Arbeidstijdenwet) ----------
 ("overtime","Max hours per week","overtime_max_hours_per_week_value","cap","2007-04-01","(current)","60","hours","ATW (Arbeidstijdenwet)",S["atw"],"60h absolute single-week max (48h/16wk avg, 55h/4wk avg). Flag values >60."),
 ("overtime","Max hours per shift/day","overtime_max_hours_per_day_value","cap","2007-04-01","(current)","12","hours","ATW",S["atw"],"Max 12h per shift. Flag values >12."),
 ("overtime","Minimum daily rest","overtime_min_rest_between_shifts_value","floor","2007-04-01","(current)","11","hours","ATW",S["atw"],"11h daily rest (reducible to 8h once per 7 days). Flag values <8."),
 ("overtime","Overtime surcharge","overtime_allowance_value","none","","","","","(no statute)",S["atw"],"No statutory overtime premium - purely contractual. Empty = not provided, never imputed."),
 # ---------- WAGE (Wet minimumloon, WML) ----------
 ("wage","Statutory minimum wage (WML)","","floor","1999-01-01","2023-12-31","varies","EUR/month (per age)","Wet minimumloon en minimumvakantiebijslag",S["wml"],"Monthly/weekly/daily statutory minimum; revised 1 Jan and 1 Jul each year."),
 ("wage","Statutory minimum HOURLY wage","","floor","2024-01-01","(current)","minimumuurloon","EUR/hour (36h base)","WML (Wet invoering minimumuurloon)",S["uurloon"],"From 1 Jan 2024 a single statutory minimum HOURLY wage replaced the monthly one. ~EUR14.06/hr (2025), EUR14.71 (2026)."),
 ("wage","Holiday allowance (vakantietoeslag)","","floor","1999-01-01","(current)","8","% of gross salary","WML art. 15",S["wml"],"Statutory minimum 8%."),
 # ---------- PENSION (Witteveen / AOW / Wtp) — documented, index uses clamps not imputation ----------
 ("pension","Max annual accrual - middelloon (Witteveen)","pension_accrual_rate_value","informational","1999-01-01","2013-12-31","2.25","% per year","Witteveen-kader",S["pensioen"],"Fiscal max DB accrual (middelloon) pre-2014. Above = impossible. Index uses a clamp (0-2.5%)."),
 ("pension","Max annual accrual - middelloon (Witteveen)","pension_accrual_rate_value","informational","2014-01-01","2014-12-31","2.15","% per year","Witteveen 2014",S["pensioen"],"2014 step."),
 ("pension","Max annual accrual - middelloon (Witteveen)","pension_accrual_rate_value","informational","2015-01-01","(current)","1.875","% per year","Wet verlaging maximumopbouw pensioen",S["pensioen"],"From 2015. Wet toekomst pensioenen (1 Jul 2023) phases DB out to flat-premium DC by 2028."),
 ("pension","AOW state pension age","pension_retire_age_normal_value","informational","1999-01-01","2012-12-31","65","years","AOW (Algemene Ouderdomswet)",S["aow"],"65 until 2012."),
 ("pension","AOW state pension age","pension_retire_age_normal_value","informational","2013-01-01","2017-12-31","65y1m .. 65y9m","years","Wet verhoging AOW- en pensioenrichtleeftijd",S["aow33290"],"Gradual rise: 2013 65+1m, 2014 65+2m, 2015 65+3m, 2016 65+6m, 2017 65+9m."),
 ("pension","AOW state pension age","pension_retire_age_normal_value","informational","2018-01-01","2023-12-31","66 .. 66y10m","years","Wet verhoging / temporisering AOW",S["aow"],"2018 66, 2019-21 66+4m, 2022 66+7m, 2023 66+10m."),
 ("pension","AOW state pension age","pension_retire_age_normal_value","informational","2024-01-01","2027-12-31","67","years","AOW (life-expectancy linked)",S["aow"],"67 from 2024 through 2027; 67+3m in 2028."),
 ("pension","Pension franchise (AOW-franchise)","pension_franchise_value","informational","2015-01-01","(current)","~10000-16000","EUR/year","fiscal minimum",S["pensioen"],"Year-specific minimum franchise. Index uses a loose clamp (EUR5k-50k), monthly annualised."),
 # ---------- FRINGE (untaxed travel allowance — fiscal norm) ----------
 ("fringe","Untaxed commuting allowance","fringe_commuting_allowance_value","informational","1999-01-01","2022-12-31","0.19","EUR/km","gerichte vrijstelling reiskosten (Belastingdienst)",S["reis"],"EUR0.19/km untaxed maximum for many years (not an entitlement; employer may pay less/more)."),
 ("fringe","Untaxed commuting allowance","fringe_commuting_allowance_value","informational","2023-01-01","2023-12-31","0.21","EUR/km","Belastingplan 2023",S["reis"],"Raised to EUR0.21/km on 1 Jan 2023."),
 ("fringe","Untaxed commuting allowance","fringe_commuting_allowance_value","informational","2024-01-01","(current)","0.23","EUR/km","Belastingplan 2024",S["reis"],"Raised to EUR0.23/km on 1 Jan 2024; unchanged 2025/2026."),
 # ---------- TRAINING (transparency directive 2022) ----------
 ("training","Mandatory training free + study-cost clause void","","informational","2022-08-01","(current)","free / void","(non-numeric)","Wet transparante en voorspelbare arbeidsvoorwaarden",S["scholing"],"From 1 Aug 2022 legally/CAO-mandated training must be free + counts as working time; studiekostenbeding for it is void. Affects training_reclaim_clause."),
 # ---------- HOMEOFFICE (Wet flexibel werken) ----------
 ("homeoffice","Right to request remote/flex work","","informational","2016-01-01","(current)","request right","(non-numeric)","Wet flexibel werken",S["wfw"],"Employee may request changes to hours/place; employer must consider. No statutory days/stipend. 'Wet werken waar je wilt' rejected by Senate (26 Sep 2023)."),
 # ---------- SAFETY (Arbowet) ----------
 ("safety","RI&E, arbodienst, preventiemedewerker, PSA policy, BHV","","informational","1999-01-01","(current)","mandatory","(non-numeric)","Arbowet (Arbeidsomstandighedenwet)",S["arbo"],"Several safety provisions are legally REQUIRED (RI&E, arbodienst/basiscontract since 1 Jul 2017, PSA policy, BHV) -> coverage 'absent in text' != 'absent in fact'."),
 # ---------- CHILDCARE (Wet kinderopvang — universal mandatory employer levy) ----------
 ("childcare","Employer childcare contribution (pre-2007)","","informational","1999-01-01","2006-12-31","voluntary","(none)","(no statutory obligation pre-2007)",S["kinderopvang"],"Before 1 Jan 2007 the employer contribution to childcare was voluntary."),
 ("childcare","Mandatory employer childcare levy (werkgeversbijdrage)","","informational","2007-01-01","(current)","~0.5","% of payroll","Wet kinderopvang art. 8-9 (Bijdrage Wko, via Aof-premie)",S["kinderopvang"],"Universal mandatory employer contribution since 1 Jan 2007 (2026: 0.5% of total payroll), collected via the differentiated Aof-premie regardless of childcare use; funds the national kinderopvangtoeslag. NOT CAO-specific -> the childcare_* CAO fields capture employer support ON TOP of this universal levy."),
]

# ---------------- WML — statutory minimum wage series (adult rate) ----------------
# One row per statutory revision (1 Jan / 1 Jul). Monthly gross EUR for the full adult
# rate (23+ until 30-06-2017, 22+ until 30-06-2019, 21+ since). From 1 Jan 2024 the
# statute switched to a single HOURLY minimum (Wet invoering minimumuurloon); the
# monthly-equivalent column is then DERIVED at the statutory 36h/week reference
# (hourly x 36 x 52/12 = x156) and marked basis="derived@36h".
# Sources: 2007+ = salaris-informatie.nl WML history table (cross-checked vs
# rijksoverheid.nl anchors); 2002-2006 = derived from the zerotax.nl hourly table
# (x173.33 @40h) + the 2004-2006 statutory freeze -> please_verify=YES on those rows.
# (effective_from, wml_month_eur, wml_hour_eur, please_verify)
WML = [
 ("2002-01-01", 1206.60, None, "YES"), ("2002-07-01", 1232.40, None, "YES"),
 ("2003-01-01", 1249.20, None, "YES"), ("2003-07-01", 1264.80, None, "YES"),
 ("2004-01-01", 1264.80, None, "YES"), ("2004-07-01", 1264.80, None, "YES"),
 ("2005-01-01", 1264.80, None, "YES"), ("2005-07-01", 1264.80, None, "YES"),
 ("2006-01-01", 1272.60, None, "YES"), ("2006-07-01", 1284.60, None, "YES"),
 ("2007-01-01", 1300.80, None, ""), ("2007-07-01", 1317.00, None, ""),
 ("2008-01-01", 1335.00, None, ""), ("2008-07-01", 1356.60, None, ""),
 ("2009-01-01", 1381.20, None, ""), ("2009-07-01", 1398.60, None, ""),
 ("2010-01-01", 1407.60, None, ""), ("2010-07-01", 1416.00, None, ""),
 ("2011-01-01", 1424.40, None, ""), ("2011-07-01", 1435.20, None, ""),
 ("2012-01-01", 1446.60, None, ""), ("2012-07-01", 1456.20, None, ""),
 ("2013-01-01", 1469.40, None, ""), ("2013-07-01", 1477.80, None, ""),
 ("2014-01-01", 1485.60, None, ""), ("2014-07-01", 1495.20, None, ""),
 ("2015-01-01", 1501.80, None, ""), ("2015-07-01", 1507.80, None, ""),
 ("2016-01-01", 1524.60, None, ""), ("2016-07-01", 1537.20, None, ""),
 ("2017-01-01", 1551.60, None, ""), ("2017-07-01", 1565.40, None, ""),
 ("2018-01-01", 1578.00, None, ""), ("2018-07-01", 1594.20, None, ""),
 ("2019-01-01", 1615.80, None, ""), ("2019-07-01", 1635.60, None, ""),
 ("2020-01-01", 1653.60, None, ""), ("2020-07-01", 1680.00, None, ""),
 ("2021-01-01", 1684.80, None, ""), ("2021-07-01", 1701.00, None, ""),
 ("2022-01-01", 1725.00, None, ""), ("2022-07-01", 1756.20, None, ""),
 ("2023-01-01", 1934.40, None, ""), ("2023-07-01", 1995.00, None, ""),
 ("2024-01-01", None, 13.27, ""), ("2024-07-01", None, 13.68, ""),
 ("2025-01-01", None, 14.06, ""), ("2025-07-01", None, 14.40, ""),
 ("2026-01-01", None, 14.71, ""), ("2026-07-01", None, 14.99, ""),
]
WML_SRC = "https://www.salaris-informatie.nl/wettelijk-minimumloon/historie-wettelijk-minimumloon"

def wml_rows():
    """Resolved WML series: (effective_from, month_eur, hour_eur, basis, please_verify)."""
    out = []
    for frm, m, h, ver in WML:
        if m is None and h is not None:
            out.append((frm, round(h * 156.0, 2), h, "derived@36h", ver))
        else:
            out.append((frm, m, h, "statutory_monthly", ver))
    return out

def write_wml_csv():
    cols = ["effective_from", "wml_month_eur", "wml_hour_eur", "basis", "please_verify", "source"]
    with open(os.path.join(il.OUT, "wml_timeline.csv"), "w", newline="") as f:
        w = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writerow(cols)
        for frm, m, h, basis, ver in wml_rows():
            w.writerow([frm, m, "" if h is None else h, basis, ver, WML_SRC])

OVERVIEW = [
 ("leave","Yes","floor (numeric)","Yes - imputed (era-aware)","WAZO, WIEG, Wet betaald ouderschapsverlof"),
 ("absence","Yes","floor (numeric)","Yes - imputed (era-aware)","BW 7:634 (vacation), WML art.15 (8%), BW 7:629 (sick pay), WAZO 5 (care leave)"),
 ("wage","Yes","floor (numeric)","Reference (mw uses percentiles)","WML (Wet minimumloon en minimumvakantiebijslag)"),
 ("term","Yes","cap + floor (numeric)","Yes","BW 7:652/672/673, WWZ, WAB"),
 ("contract","Yes","default (numeric)","Yes","WWZ, WAB (ketenregeling)"),
 ("overtime","Yes","cap + floor (numeric)","Yes","ATW (Arbeidstijdenwet)"),
 ("pension","Partial","cap + informational","Clamp only (Witteveen); fund-deferred","Witteveen-kader, AOW, Wet toekomst pensioenen"),
 ("fringe","Partial","informational (fiscal norm)","No (documented)","Untaxed travel allowance (Belastingdienst)"),
 ("training","Partial","informational (non-numeric)","Affects reclaim_clause read","Wet transparante en voorspelbare arbeidsvoorwaarden (2022)"),
 ("homeoffice","Yes (right to request)","informational (non-numeric)","No","Wet flexibel werken"),
 ("safety","Yes (mandated provisions)","informational (non-numeric)","Coverage caveat","Arbowet (Arbeidsomstandighedenwet)"),
 ("childcare","Partial","informational (universal levy)","No - universal, not CAO-specific","Wet kinderopvang: verplichte werkgeversbijdrage (since 2007)"),
 ("bonus","No (own fields)","none","No","13th month / bonuses not statutory; statutory 8% vakantiegeld -> see Wage/Leave tabs"),
 ("ai","No","none","No","None yet (EU AI Act phasing in 2024-2026)"),
]

# ---------------- machine-readable CSV (indices read this) ----------------
def write_csv():
    cols = ["topic","field","statutory_role","effective_from","effective_to","value","unit","legal_basis","source","notes"]
    with open(os.path.join(il.OUT,"statutory_all.csv"),"w",newline="") as f:
        w = csv.writer(f,delimiter=";",quoting=csv.QUOTE_MINIMAL)
        w.writerow(cols)
        for t,var,field,role,frm,to,val,unit,law,src,note in DATA:
            w.writerow([t,field,role,frm,to,val,unit,law,src,note])

# ---------------- nice workbook ----------------
ARIAL = "Arial"
HDR_FILL = PatternFill("solid", fgColor="1F4E78")
TITLE_FILL = PatternFill("solid", fgColor="DDEBF7")
DIR_FILL = {"floor": "E2EFDA", "cap": "FCE4D6", "default": "FFF2CC",
            "informational": "F2F2F2", "informational_formula": "F2F2F2", "none": "F2F2F2"}
THIN = Side(style="thin", color="BFBFBF")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)

def style_header(ws, ncol, row=1):
    for c in range(1, ncol + 1):
        cell = ws.cell(row=row, column=c)
        cell.font = Font(name=ARIAL, bold=True, color="FFFFFF", size=10)
        cell.fill = HDR_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = BORDER

def build_xlsx():
    wb = Workbook()
    # Overview tab
    ws = wb.active; ws.title = "Overview"
    ws["A1"] = "CAO Statutory Reference - which topics have a statutory anchor"
    ws["A1"].font = Font(name=ARIAL, bold=True, size=13)
    ws.merge_cells("A1:E1")
    ws["A2"] = "Each topic tab below gives a 1999->today timeline per statutory variable, with the law and a source link. Rows are change-points."
    ws["A2"].font = Font(name=ARIAL, italic=True, size=9); ws.merge_cells("A2:E2")
    hdr = ["Topic","Statutory anchor?","Type","Used in the index?","Key law(s)"]
    ws.append([]); ws.append(hdr)
    hrow = ws.max_row
    style_header(ws, len(hdr), hrow)
    for r in OVERVIEW:
        ws.append(list(r))
        for c in range(1, len(hdr) + 1):
            cell = ws.cell(row=ws.max_row, column=c)
            cell.font = Font(name=ARIAL, size=10); cell.border = BORDER
            cell.alignment = Alignment(vertical="top", wrap_text=True)
        anc = ws.cell(row=ws.max_row, column=2)
        anc.fill = PatternFill("solid", fgColor="E2EFDA" if r[1].startswith("Yes")
                               else "FFF2CC" if r[1] == "Partial" else "FCE4D6")
    for col, w in zip("ABCDE", [12, 18, 26, 30, 46]):
        ws.column_dimensions[col].width = w
    ws.freeze_panes = f"A{hrow+1}"; ws.auto_filter.ref = f"A{hrow}:E{ws.max_row}"

    # per-topic tabs
    topics = []
    for rec in DATA:
        if rec[0] not in topics: topics.append(rec[0])
    cols = ["Variable","From","To","Value","Unit","Direction","Statutory law","Source","Notes"]
    widths = [34, 12, 12, 14, 18, 16, 40, 16, 60]
    for topic in topics:
        ws = wb.create_sheet(topic.capitalize())
        ws["A1"] = f"{topic.upper()} - statutory timeline (1999 -> today)"
        ws["A1"].font = Font(name=ARIAL, bold=True, size=12)
        ws.merge_cells("A1:I1"); ws["A1"].fill = TITLE_FILL
        ws.append([]); ws.append(cols); hrow = ws.max_row
        style_header(ws, len(cols), hrow)
        prev_var = None; band = False
        for t,var,field,role,frm,to,val,unit,law,src,note in DATA:
            if t != topic: continue
            if var != prev_var: band = not band; prev_var = var
            ws.append([var, frm, to, val, unit, role, law, "source", note])
            r = ws.max_row
            for c in range(1, len(cols) + 1):
                cell = ws.cell(row=r, column=c)
                cell.font = Font(name=ARIAL, size=10)
                cell.alignment = Alignment(vertical="top", wrap_text=(c in (1,7,9)))
                cell.border = BORDER
                if band: cell.fill = PatternFill("solid", fgColor="F8F8F8")
            ws.cell(row=r, column=6).fill = PatternFill("solid", fgColor=DIR_FILL.get(role, "F2F2F2"))
            ws.cell(row=r, column=6).alignment = Alignment(horizontal="center", vertical="top")
            link = ws.cell(row=r, column=8)
            link.value = "link"; link.hyperlink = src
            link.font = Font(name=ARIAL, size=10, color="0563C1", underline="single")
            link.alignment = Alignment(horizontal="center", vertical="top")
        for i, w in enumerate(widths):
            ws.column_dimensions[get_column_letter(i+1)].width = w
        ws.freeze_panes = f"A{hrow+1}"; ws.auto_filter.ref = f"A{hrow}:I{ws.max_row}"

    # WML tab — the dense minimum-wage series (feeds wml_timeline.csv / the wage indices)
    ws = wb.create_sheet("WML")
    ws["A1"] = "WML - statutory gross minimum wage, adult rate (per revision date)"
    ws["A1"].font = Font(name=ARIAL, bold=True, size=12)
    ws.merge_cells("A1:F1"); ws["A1"].fill = TITLE_FILL
    ws["A2"] = ("Monthly gross EUR (23+/22+/21+ adult rate as the age threshold moved). From 2024 the statute is "
                "HOURLY; the monthly column is derived at the 36h/week statutory reference (x156). "
                "please_verify=YES rows are derived (pre-2007 hourly table + 2004-2006 freeze) - verify vs rijksoverheid.nl.")
    ws["A2"].font = Font(name=ARIAL, italic=True, size=9); ws.merge_cells("A2:F2")
    hdr = ["Effective from", "EUR/month", "EUR/hour", "Basis", "please_verify", "Source"]
    ws.append([]); ws.append(hdr); hrow = ws.max_row
    style_header(ws, len(hdr), hrow)
    for frm, m, h, basis, ver in wml_rows():
        ws.append([frm, m, h, basis, ver, "link"])
        r = ws.max_row
        for c in range(1, len(hdr) + 1):
            cell = ws.cell(row=r, column=c)
            cell.font = Font(name=ARIAL, size=10); cell.border = BORDER
        if ver:
            ws.cell(row=r, column=5).fill = PatternFill("solid", fgColor="FCE4D6")
        link = ws.cell(row=r, column=6); link.hyperlink = WML_SRC
        link.font = Font(name=ARIAL, size=10, color="0563C1", underline="single")
    for col, w in zip("ABCDEF", [14, 12, 12, 18, 14, 10]):
        ws.column_dimensions[col].width = w
    ws.freeze_panes = f"A{hrow+1}"; ws.auto_filter.ref = f"A{hrow}:F{ws.max_row}"

    wb.save(os.path.join(il.REV, "statutory_timeline.xlsx"))

if __name__ == "__main__":
    import sys
    # RETIRED FROM THE PIPELINE (2026-07-06): statutory_timeline.xlsx is Hanna's
    # hand-maintained GROUND TRUTH — regenerating it here destroyed her research once
    # (recovered from _recovered/statutory_timeline.BACKUP8.xlsx). The machine files
    # are now derived FROM the workbook by statutory_sync.py. This bootstrap only
    # runs with --force-bootstrap AND writes to statutory_timeline.BOOTSTRAP.xlsx.
    if "--force-bootstrap" not in sys.argv:
        sys.exit("REFUSING to run: statutory_timeline.xlsx is hand-maintained ground truth.\n"
                 "Edit the workbook, then run:  python3 statutory_sync.py\n"
                 "(--force-bootstrap writes statutory_timeline.BOOTSTRAP.xlsx only)")
    import builtins
    _orig_join = os.path.join
    def _join(*a):
        p = _orig_join(*a)
        return p.replace("statutory_timeline.xlsx", "statutory_timeline.BOOTSTRAP.xlsx") \
                .replace("statutory_all.csv", "statutory_all.BOOTSTRAP.csv") \
                .replace("wml_timeline.csv", "wml_timeline.BOOTSTRAP.csv")
    os.path.join = _join
    write_csv(); write_wml_csv(); build_xlsx()
    print("bootstrap files written (*.BOOTSTRAP.*) — the live files are managed by statutory_sync.py")
