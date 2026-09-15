# Advanced multivariate analysis — the indices under the microscope

_Single consolidated analysis (replaces the old factor_analysis.py). Every method below earns its place; redundant ones are listed in §8 with the reason they were dropped. Honest headline up front: the topics are close to independent, so the equal-weight signed z stays the primary index and everything else is robustness or description._

## 0 · Is a factor model even warranted here? (KMO + Bartlett)

**Plain-language.** *Factor analysis* asks: do these many variables really move together as a few hidden 'factors' (e.g. one latent 'generosity'), or are they largely independent? Before trusting any factor, two standard checks say whether the data is even factor-able:
- **KMO** (Kaiser-Meyer-Olkin, 0-1): how much the variables share common variance. >0.8 great, 0.6-0.8 mediocre, **<0.6 = a factor model is barely justified** — the variables mostly stand alone.
- **Bartlett's test**: rejects the null that the correlation matrix is the identity (all variables perfectly independent). A tiny p just says 'not *perfectly* independent' — it does not say the relationships are strong.
The table below counts how many topics each document actually scores (the matrix the tests run on). The **score basis** for this and §2 is the **11 topic magnitude z's** (`overall` = their mean); §1 runs on each topic's **per-field z's**; §4 on the **coverage booleans**; §5 relates magnitude vs coverage; §9 re-runs §0/§2 on the **percentile (`gen01`)** scale. Magnitude and coverage are always separate tracks.

The 11 topic z's are computed **available-case** — every document keeps the topics it actually states a number for. The factorability tests need a fairly complete matrix, so they run on the **2670 of 2698** documents that score ≥8 topics (the other 28 genuinely state too few numbers — they defer pension to a fund, carry no bonus/fringe/homeoffice/wage figure — and we never invent values). Only **804** documents score all 11, because homeoffice *magnitude* (stipend/WFH-days) is filled in just 10% of CAOs.

- **KMO = 0.64** (>0.8 good · 0.6–0.8 mediocre · **<0.6 = factor analysis barely supported**); Bartlett χ² ≈ 2201 (df 55, p≈0 — the topics are not perfectly independent, but only weakly related).
- **Reading:** the topics share little common variance. Any 'latent generosity' factor will be weak *by construction*, so throughout this file we read factors as **mild tendencies, not real underlying constructs.** This is itself the headline result: generosity is topic-specific, not one trait.

| # topics scored | docs |
|---|---|
| 6 | 2 |
| 7 | 26 |
| 8 | 108 |
| 9 | 498 |
| 10 | 1260 |
| 11 | 804 |

## 1 · Within-topic factor structure & the weighting question

**What we test.** Each `<topic>_z` we publish is an *equal-weight mean of signed field z's*. Two questions: (a) **sub-dimensions** — does a topic's fields split into more than one latent dimension (e.g. overtime pay vs protection)? (b) **weighting** — would data-driven weights (the first principal component / first factor) rank documents differently from equal weights? For each topic we run a full exploratory FA (varimax) AND a PCA on the per-field signed z's (available-case, mean-imputed, standardised), and report eigenvalues (scree), variance explained, the complete loading matrix with communalities, and the correlation of the data-driven score with the equal-weight z.

**How to read a loading.** A loading is the correlation of a field with a factor (−1…+1); |loading|>0.4 = the field defines that factor. *Communality* = share of a field's variance the factors capture (near 0 = the field stands alone). Two fields loading on the SAME factor move together; on DIFFERENT factors = a real sub-dimension.

### 1.1 leave  (2698 docs, 3 fields, 0% cells imputed)

- eigenvalues (scree): 2.39, 0.48, 0.13 → **1 factor(s)** (Kaiser: eigenvalue>1)
- variance explained: f1 72.2% (total 72.2%)
**leave — varimax loadings**

```
                     f1  communality
leave_paternity_z -1.00         1.00
leave_adoption_z  -0.85         0.73
leave_parental_z  -0.66         0.43
```

- corr(PC1, equal-weight) = **+1.00**, corr(FA1, equal-weight) = +0.94

### 1.2 term  (2655 docs, 6 fields, 17% cells imputed)

- eigenvalues (scree): 1.47, 1.24, 1.06, 0.85, 0.72, 0.67 → **3 factor(s)** (Kaiser: eigenvalue>1)
- variance explained: f1 17.9%, f2 11.9%, f3 6.5% (total 36.2%)
**term — varimax loadings**

```
                            f1    f2    f3  communality
employer_notice_mo_z     -0.01  0.20  0.41         0.21
employer_notice_max_mo_z -0.01  0.36 -0.03         0.13
probation_fixedterm_mo_z  1.00  0.06  0.02         1.00
probation_indef_mo_z      0.26 -0.06 -0.08         0.08
notice_floor_mo_z        -0.07  0.73  0.26         0.60
severance_mo_z           -0.09 -0.05  0.38         0.15
```

- corr(PC1, equal-weight) = **+0.55**, corr(FA1, equal-weight) = +0.40

### 1.3 contract  (2698 docs, 4 fields, 21% cells imputed)

- eigenvalues (scree): 1.52, 1.01, 0.99, 0.48 → **2 factor(s)** (Kaiser: eigenvalue>1)
- variance explained: f1 26.6%, f2 23.4% (total 50.0%)
**contract — varimax loadings**

```
                           f1    f2  communality
keten_max_contracts_z    0.98  0.20         1.00
keten_max_duration_mo_z  0.32  0.94         1.00
fulltime_hours_wk_z      0.07 -0.01         0.00
wh_adjust_tenure_mo_z    0.03 -0.08         0.01
```

- corr(PC1, equal-weight) = **+0.90**, corr(FA1, equal-weight) = +0.70

### 1.4 overtime  (2629 docs, 9 fields, 22% cells imputed)

- eigenvalues (scree): 1.71, 1.36, 1.21, 1.13, 0.97, 0.82 → **4 factor(s)** (Kaiser: eigenvalue>1)
- variance explained: f1 11.9%, f2 11.7%, f3 7.6%, f4 3.6% (total 34.9%)
**overtime — varimax loadings**

```
                          f1    f2    f3    f4  communality
ot_allowance_pct_z     -0.08 -0.17  0.48  0.18         0.30
ot_allowance_max_pct_z  0.13 -0.06  0.55 -0.01         0.32
shift_allowance_pct_z  -0.03  0.08  0.01  0.29         0.09
unfav_allowance_pct_z  -0.10  0.12  0.39 -0.23         0.22
trigger_daily_h_z       0.98  0.09  0.03 -0.18         1.00
trigger_weekly_h_z      0.26  0.04 -0.01  0.09         0.07
min_rest_h_z            0.05  0.02 -0.01  0.29         0.09
max_hours_wk_z          0.05  0.75 -0.05  0.18         0.59
max_hours_day_z         0.12  0.66 -0.06  0.05         0.45
```

- corr(PC1, equal-weight) = **+0.43**, corr(FA1, equal-weight) = +0.31

### 1.5 absence  (2698 docs, 7 fields, 12% cells imputed)

- eigenvalues (scree): 1.39, 1.10, 1.08, 0.97, 0.95, 0.81 → **3 factor(s)** (Kaiser: eigenvalue>1)
- variance explained: f1 14.6%, f2 10.2%, f3 2.7% (total 27.5%)
**absence — varimax loadings**

```
                        f1    f2    f3  communality
vacation_days_yr_z    0.11  0.01  0.00         0.01
vacation_bonus_pct_z  0.03 -0.14  0.31         0.11
sickpay_pct_z         0.02  0.08  0.21         0.05
stcare_days_yr_z      0.05 -0.00 -0.20         0.04
stcare_pay_pct_z      0.99  0.11 -0.06         1.00
ltcare_wks_z         -0.03  0.74  0.04         0.55
ltcare_pay_pct_z      0.17  0.35 -0.02         0.15
```

- corr(PC1, equal-weight) = **+0.76**, corr(FA1, equal-weight) = +0.53

### 1.6 pension  (827 docs, 4 fields, 32% cells imputed)

- eigenvalues (scree): 1.19, 1.04, 1.02, 0.75 → **3 factor(s)** (Kaiser: eigenvalue>1)
- variance explained: f1 11.1%, f2 6.3%, f3 5.4% (total 22.8%)
**pension — varimax loadings**

```
                          f1    f2    f3  communality
employee_contrib_pct_z  0.45  0.26 -0.21         0.31
accrual_rate_pct_z      0.49 -0.09  0.12         0.26
franchise_eur_z         0.01  0.03  0.40         0.16
early_retire_age_z     -0.00  0.42  0.03         0.18
```

- corr(PC1, equal-weight) = **+0.79**, corr(FA1, equal-weight) = +0.69

### 1.7 training  (1613 docs, 2 fields, 0% cells imputed)

- eigenvalues (scree): 1.09, 0.91 → **1 factor(s)** (Kaiser: eigenvalue>1)
- variance explained: f1 9.2% (total 9.2%)
**training — varimax loadings**

```
                     f1  communality
training_days_yr_z -0.3         0.09
cost_reimb_pct_z   -0.3         0.09
```

- corr(PC1, equal-weight) = **+0.92**, corr(FA1, equal-weight) = +0.92

### 1.8 fringe  (1670 docs, 3 fields, 20% cells imputed)

- eigenvalues (scree): 1.27, 0.99, 0.74 → **1 factor(s)** (Kaiser: eigenvalue>1)
- variance explained: f1 35.5% (total 35.5%)
**fringe — varimax loadings**

```
                      f1  communality
commuting_eur_km_z -1.00         1.00
meal_eur_z         -0.24         0.06
relocation_eur_z   -0.10         0.01
```

- corr(PC1, equal-weight) = **+0.92**, corr(FA1, equal-weight) = +0.59

### 1.9 bonus  (1019 docs, 2 fields, 0% cells imputed)

- eigenvalues (scree): 1.35, 0.65 → **1 factor(s)** (Kaiser: eigenvalue>1)
- variance explained: f1 34.6% (total 34.6%)
**bonus — varimax loadings**

```
                           f1  communality
thirteenth_pct_annual_z  0.59         0.35
fixed_lump_eur_z         0.59         0.35
```

- corr(PC1, equal-weight) = **+1.00**, corr(FA1, equal-weight) = +1.00

**Summary — equal-weight vs data-driven weighting, all topics**

```
     topic    track    n  fields  corr_PC1  corr_FA1  PC1_var%  n_factors
     leave  numeric 2698       3     1.000     0.938      79.8          1
      term  numeric 2655       6     0.546     0.405      24.5          3
  contract  numeric 2698       4     0.901     0.702      37.9          2
  overtime  numeric 2629       9     0.433     0.310      19.0          4
   absence  numeric 2698       7     0.760     0.527      19.9          3
   pension  numeric  827       4     0.788     0.688      29.7          3
  training  numeric 1613       2     0.923     0.923      54.6          1
    fringe  numeric 1670       3     0.918     0.590      42.4          1
     bonus  numeric 1019       2     0.995     0.995      67.3          1
     leave coverage 2698       8     0.537     0.208      25.4          3
   absence coverage 2698       3     0.633     0.720      37.7          2
      term coverage 2698       3     0.997     0.974      44.5          1
  contract coverage 2698       3     0.711     0.733      41.4          2
  training coverage 2698       3     0.848     0.637      41.2          2
     bonus coverage 2698       8     0.725     0.454      20.8          3
    fringe coverage 2698       8     0.989     0.401      24.4          3
homeoffice coverage 2698       3     0.993     0.960      81.8          1
   pension coverage 2698       5     0.980     0.466      27.8          2
    safety coverage 2698      12     0.983     0.472      24.8          4
 childcare coverage 2698       2     0.498     0.498      50.5          1
```

**Verdict — sub-dimensions:** most topics show ≥2 factors with PC1 explaining only ~19–39% of variance, i.e. the fields genuinely carry more than one dimension. The clearest and most interpretable: **overtime** splits pay-rate fields (allowance, unfavourable-hours) from hours-protection fields (rest, max-hours) — the same split the index already ships as `overtime_pay_z` / `overtime_protection_z`; **term** splits probation (employer flexibility) from notice+severance (worker security); **absence** splits leave-duration from replacement-rate. So the topic scores are defensible *averages* of real sub-dimensions, and the sub-scores are there when you want to separate them.

**Verdict — weighting ('PCA as first step?'):** where corr(PC1, equal-weight) is high (contract ~0.90, absence, training, bonus) the two rank documents identically — data-driven weights add nothing. Where it is LOW (pension ~0.18, fringe ~0.22, overtime ~0.48) that disagreement is a point **FOR** equal weights: PCA is *sign-blind* and variance-driven, so for pension — whose fields have opposing signs (employee-contribution −, accrual +) — PC1 chases the highest-variance field, not 'generosity'. Our equal-weight score applies the theory-driven +/− signs PCA cannot see. **Keep the signed equal-weight z primary; do NOT make PCA the first step** — it would discard the normative signs that define 'more generous'. The PCA/FA scores are kept in `stage1_weighting.csv` as a robustness companion.

## 2 · Across-topic structure — is there ONE 'generosity' dimension?

**What we test.** Do the topic scores reflect one underlying 'generosity' we could summarise with a single factor, or are they distinct? We run THREE independent exploratory FAs (varimax, 3 factors) — one on the COMBINED topic scores, one on the NUMERIC-only scores, one on the COVERAGE-only scores — and read the loadings and communalities.

**How to interpret.** If ONE factor loaded strongly and positively on (almost) all topics, that factor *is* 'generosity'. If topics scatter across factors with low communalities, generosity is not one thing — the composite is a useful summary, not a construct. Each of the three blocks is a SEPARATE analysis (independent factors).

- **combined**: variance explained f1 15.9%, f2 11.7%, f3 8.6% (total 36.2%; 2698 docs).
- **numeric**: variance explained f1 9.9%, f2 8.2%, f3 3.8% (total 21.9%; 2670 docs).
- **coverage**: variance explained f1 13.2%, f2 11.3%, f3 7.7% (total 32.2%; 2698 docs).
**EFA of the topic scores — 3 independent blocks (combined / numeric / coverage)**

```
                        section    f1    f2    f3  communality
leave_z                combined  0.16  0.28  0.77         0.69
absence_z              combined  0.61  0.13  0.14         0.41
term_z                 combined  0.52  0.26 -0.03         0.35
contract_z             combined  0.47  0.04  0.15         0.24
overtime_z             combined  0.35 -0.11  0.34         0.25
training_z             combined  0.48 -0.00  0.07         0.24
bonus_z                combined  0.38  0.46  0.14         0.37
fringe_z               combined  0.40  0.46  0.04         0.38
homeoffice_z           combined  0.02  0.41  0.40         0.33
pension_z              combined  0.44  0.14  0.06         0.21
wage_median_z          combined  0.00  0.70  0.12         0.51
leave_numeric_z         numeric  0.74 -0.02 -0.05         0.55
absence_numeric_z       numeric  0.11  0.39 -0.02         0.17
term_numeric_z          numeric  0.05  0.27  0.34         0.19
contract_numeric_z      numeric -0.09  0.16  0.03         0.03
overtime_numeric_z      numeric  0.02  0.03 -0.14         0.02
training_numeric_z      numeric  0.06  0.22 -0.03         0.05
bonus_numeric_z         numeric  0.16  0.61  0.08         0.41
fringe_numeric_z        numeric  0.15  0.25  0.18         0.12
homeoffice_numeric_z    numeric  0.42  0.14  0.09         0.20
pension_numeric_z       numeric -0.08 -0.03 -0.47         0.23
wage_median_z           numeric  0.53  0.38  0.06         0.43
leave_coverage_z       coverage  0.51  0.14  0.39         0.43
absence_coverage_z     coverage  0.45  0.35  0.00         0.32
term_coverage_z        coverage  0.29  0.39  0.01         0.23
contract_coverage_z    coverage  0.57  0.16  0.18         0.38
overtime_coverage_z    coverage  0.43  0.05  0.17         0.22
training_coverage_z    coverage  0.32  0.38 -0.14         0.26
bonus_coverage_z       coverage  0.20  0.37  0.30         0.27
fringe_coverage_z      coverage  0.05  0.75  0.37         0.71
homeoffice_coverage_z  coverage  0.06  0.08  0.61         0.38
pension_coverage_z     coverage  0.40  0.24  0.15         0.24
safety_coverage_z      coverage  0.46  0.34  0.09         0.34
childcare_coverage_z   coverage  0.11  0.03  0.26         0.08
```

**Reading:** no factor loads strongly and positively on most topics — **there is no single generosity axis.** The mild groupings: a *pay/perks* tendency (wage, bonus, absence), a *time/flexibility* tendency (homeoffice, training, overtime), and pension largely alone. Communalities are low (most <0.2), so each topic is mostly its own signal. **Consequence:** the equal-weight composite is a legitimate transparent *summary*, but you should ALSO report the topic z's individually — collapsing to one number hides real, independent variation. Per-document factor scores are in `factor_scores.csv`; loadings in `factor_loadings_overall.csv`.

## 3 · Do provision BREADTH and pay LEVEL separate? (magnitude + coverage jointly)

**Joint EFA: 11 magnitude z's + 12 coverage z's (varimax, 2670 docs)**

```
                         f1    f2    f3  communality
leave_numeric_z        0.40  0.07 -0.09         0.18
absence_numeric_z      0.08  0.36  0.33         0.24
term_numeric_z         0.21  0.15  0.11         0.08
contract_numeric_z    -0.01  0.12 -0.02         0.01
overtime_numeric_z     0.04 -0.07  0.27         0.08
training_numeric_z     0.00  0.25  0.17         0.09
bonus_numeric_z        0.27  0.43  0.10         0.26
fringe_numeric_z       0.13  0.50  0.03         0.27
homeoffice_numeric_z   0.77 -0.00  0.11         0.61
pension_numeric_z     -0.09 -0.12 -0.08         0.03
wage_median_z          0.43  0.44 -0.15         0.40
leave_coverage_z       0.20  0.13  0.57         0.38
absence_coverage_z    -0.04  0.34  0.42         0.30
term_coverage_z       -0.03  0.40  0.26         0.23
contract_coverage_z    0.05  0.17  0.56         0.34
overtime_coverage_z    0.04 -0.02  0.49         0.24
training_coverage_z   -0.12  0.25  0.37         0.21
bonus_coverage_z       0.17  0.44  0.25         0.29
fringe_coverage_z      0.22  0.65  0.20         0.51
homeoffice_coverage_z  0.94  0.02  0.16         0.90
pension_coverage_z     0.10  0.31  0.38         0.25
safety_coverage_z      0.07  0.31  0.47         0.32
childcare_coverage_z   0.20  0.09  0.15         0.07
```

**Reading:** magnitude items load mainly on **f2**, coverage items on **f3** — SEPARATE factors. Provision BREADTH (how many things are written down) and pay/duration LEVEL (how much) are **distinct constructs**, so the two roll-ups (`overall_numeric_z` and `coverage_overall`) are kept separate by design — collapsing them would blur two different questions.

## 4 · Coverage booleans, done right — TETRACHORIC factor analysis

Coverage items are yes/no. Pearson correlation between 0/1 variables is attenuated (it caps well below ±1 even for strongly linked provisions), so a Pearson factor analysis understates structure — we therefore use **tetrachoric** correlations, which recover the latent-continuous association a yes/no implies, and factor that matrix. (This replaces the earlier Pearson coverage FA.)

- 55 booleans (True-rate 3–97%), 2698 documents. Tetrachoric-matrix **KMO = 0.47** (Kaiser suggested 17 factors; capped at 6 for readability). The very low KMO means the booleans barely share structure — provisions are largely INDEPENDENT adoption decisions.

Each boolean's strongest **co-adoption bundle** (loading >0.3):

- **bundle1**: leave:parental_topup_prese (+0.78), leave:liberation_day_annua (+0.75), bonus:thirteenth_month (+0.69), leave:liberation_day_lustr (-0.62), fringe:relocation_allowance (+0.57), fringe:bike_scheme_present (+0.53), bonus:seniority_loyalty_bo (+0.53)
- **bundle2**: homeoffice:stipend_present (+0.93), homeoffice:costs_reimbursed (+0.89), homeoffice:has_rights (+0.85), fringe:internet_or_phone_re (+0.45), childcare:support_present (+0.41), contract:part_time_allowed (+0.40), leave:has_above_statutory_ (+0.38)
- **bundle3**: safety:psa_prevention_measu (+0.83), safety:workload_monitoring_ (+0.76), safety:wellbeing_program_pr (+0.59), safety:rie_psa_required (+0.49), contract:workhours_adjustment (+0.39), leave:has_enhancements (+0.34), pension:excedent_present (+0.34)
- **bundle4**: safety:reporting_channel_ex (+0.86), safety:confidential_counsel (+0.83), safety:harassment_protocol_ (+0.75), safety:integrity_protocol_p (+0.69), bonus:seniority_loyalty_bo (+0.45)
- **bundle5**: leave:extra_seniority_pres (+0.59), absence:leave_sickpay_extra_ (+0.58), leave:has_enhancements (+0.55), contract:workhours_adjustment (+0.51), term:notice_tenure_presen (+0.50), pension:mandatory_participat (+0.47), absence:leave_sick_topup_pre (+0.47)
- **bundle6**: bonus:qual_present (+0.60), fringe:mandatory_certificat (+0.58), absence:leave_sick_topup_pre (+0.48), bonus:job_allowances_prese (+0.46), fringe:commuting_allowance_ (+0.46), safety:preventive_medical_c (+0.44), fringe:health_insurance_sup (+0.43)

**Reading:** breadth resolves into a handful of *co-adoption bundles* — a home-working perks bundle, a safety-protocol bundle, a PSA/well-being bundle, a fringe-benefits bundle, a flexibility-rights bundle — **not one 'comprehensive CAO' dimension.** A CAO that adopts one bundle need not adopt another. Full matrix in `coverage_tetrachoric_matrix.csv`.

## 4b · Mixed factor analysis (FAMD) — amounts AND provisions together

**What it is & how it works.** §1 (FactorLoadings) factors only the NUMERIC amounts; §4 factors only the yes/no provisions. **FAMD (Factor Analysis of Mixed Data)** puts BOTH in one analysis, per topic. Numeric fields are standardised (mean 0, sd 1). Each yes/no provision becomes a 0/1 indicator that is centred and divided by √(its True-rate) — the MCA metric, so a rare provision cannot dominate and booleans sit on the same scale as the standardised numerics. ONE PCA/SVD then runs on the combined matrix, so numerics and booleans compete on equal terms. Each variable's loading = its correlation with the resulting factor (signed −1..1; numeric = Pearson, boolean = point-biserial); communality = how much of the variable the factors capture.

**FAMD mixed loadings — numeric fields + coverage booleans, per topic**

```
   topic                                        variable    type     f1     f2     f3  communality
   leave                               leave_paternity_z numeric  0.947 -0.062  0.010        0.901
   leave                                leave_adoption_z numeric  0.900 -0.106 -0.042        0.823
   leave                                leave_parental_z numeric  0.813 -0.002 -0.081        0.668
   leave                    leave_has_leave_enhancements boolean -0.012  0.061  0.111        0.016
   leave             leave_has_above_statutory_maternity boolean  0.100  0.609  0.571        0.707
   leave      leave_paternity_explicitly_above_statutory boolean  0.158  0.301  0.644        0.530
   leave              leave_parental_eligibility_present boolean -0.173  0.509 -0.125        0.305
   leave                    leave_parental_topup_present boolean  0.124  0.739 -0.170        0.590
   leave                     leave_liberation_day_annual boolean  0.087  0.738 -0.362        0.683
   leave                    leave_liberation_day_lustrum boolean -0.009 -0.404  0.316        0.263
   leave                   leave_extra_seniority_present boolean -0.125 -0.039  0.049        0.020
    term                            employer_notice_mo_z numeric  0.650 -0.238  0.379        0.623
    term                        employer_notice_max_mo_z numeric  0.412 -0.395 -0.530        0.607
    term                        probation_fixedterm_mo_z numeric -0.285 -0.697  0.244        0.627
    term                            probation_indef_mo_z numeric -0.388 -0.615  0.297        0.617
    term                               notice_floor_mo_z numeric  0.663 -0.332 -0.267        0.621
    term                                  severance_mo_z numeric  0.421  0.215  0.624        0.613
    term                      term_notice_tenure_present boolean -0.185  0.136 -0.168        0.081
    term                        term_sick_dismissal_prot boolean  0.273  0.103  0.258        0.152
    term                    term_severance_ww_supplement boolean  0.180  0.042  0.209        0.078
contract                           keten_max_contracts_z numeric -0.873 -0.098 -0.048        0.774
contract                         keten_max_duration_mo_z numeric -0.852 -0.005  0.134        0.744
contract                             fulltime_hours_wk_z numeric -0.113  0.509 -0.852        0.998
contract                           wh_adjust_tenure_mo_z numeric  0.065 -0.858 -0.494        0.984
contract contract_conversion_rights_temp_to_perm_present boolean  0.290  0.147  0.056        0.109
contract     contract_workhours_adjustment_right_present boolean -0.084 -0.093 -0.040        0.017
contract                      contract_part_time_allowed boolean -0.124 -0.077  0.078        0.027
 absence                              vacation_days_yr_z numeric -0.159  0.204 -0.540        0.358
 absence                            vacation_bonus_pct_z numeric  0.236  0.503 -0.305        0.402
 absence                                   sickpay_pct_z numeric -0.090  0.629  0.079        0.410
 absence                                stcare_days_yr_z numeric -0.206 -0.593 -0.243        0.453
 absence                                stcare_pay_pct_z numeric -0.643  0.059 -0.515        0.682
 absence                                    ltcare_wks_z numeric -0.520  0.159  0.568        0.618
 absence                                ltcare_pay_pct_z numeric -0.715  0.112  0.189        0.559
 absence                        leave_sick_topup_present boolean -0.032  0.198  0.051        0.043
 absence           leave_sickpay_extra_insurance_present boolean  0.116  0.320 -0.101        0.126
 absence                        leave_care_topup_present boolean -0.724 -0.077 -0.124        0.545
 pension                          employee_contrib_pct_z numeric  0.659 -0.425 -0.183        0.648
 pension                              accrual_rate_pct_z numeric  0.330  0.055 -0.752        0.677
 pension                                 franchise_eur_z numeric  0.331  0.832  0.067        0.806
 pension                              early_retire_age_z numeric  0.295 -0.035  0.590        0.436
 pension                        pension_excedent_present boolean -0.530 -0.322  0.041        0.386
 pension                     pension_accrual_stat_leaves boolean -0.339 -0.087 -0.238        0.179
 pension                      pension_accrual_illness_y2 boolean -0.407  0.017 -0.217        0.213
 pension                        pension_premium_eq_split boolean -0.430  0.405 -0.185        0.383
 pension                 pension_mandatory_participation boolean -0.133  0.035 -0.072        0.024
training                              training_days_yr_z numeric  0.721  0.693 -0.015        1.000
training                                cost_reimb_pct_z numeric  0.741 -0.669  0.018        0.997
training                           training_fund_present boolean  0.073 -0.089 -0.959        0.933
training                training_mandatory_training_paid boolean  0.226 -0.020  0.328        0.159
  fringe                              commuting_eur_km_z numeric  0.610 -0.152  0.638        0.802
  fringe                                      meal_eur_z numeric  0.484 -0.754 -0.297        0.891
  fringe                                relocation_eur_z numeric  0.561  0.548 -0.396        0.772
  fringe                      fringe_bike_scheme_present boolean  0.464  0.269  0.203        0.329
  fringe  fringe_internet_or_phone_reimbursement_present boolean  0.329  0.206 -0.340        0.266
  fringe         fringe_health_insurance_support_present boolean  0.403  0.143  0.302        0.274
  fringe     fringe_insurance_or_savings_benefit_present boolean  0.267  0.021  0.306        0.165
  fringe            fringe_mandatory_certifications_paid boolean  0.359 -0.169  0.024        0.158
  fringe                     fringe_meal_benefit_present boolean  0.508 -0.575 -0.328        0.696
  fringe             fringe_relocation_allowance_present boolean  0.611  0.412 -0.120        0.557
  fringe              fringe_commuting_allowance_present boolean  0.428 -0.065  0.200        0.227
   bonus                         thirteenth_pct_annual_z numeric  0.892 -0.063  0.005        0.800
   bonus                                fixed_lump_eur_z numeric  0.416  0.733  0.393        0.865
   bonus                    bonus_profit_sharing_present boolean -0.092 -0.572  0.727        0.864
   bonus                 bonus_performance_bonus_present boolean  0.139 -0.109 -0.061        0.035
   bonus                        bonus_qual_bonus_present boolean -0.276  0.078 -0.452        0.287
   bonus                   bonus_retire_gratuity_present boolean  0.544 -0.283 -0.328        0.484
   bonus                   bonus_seniority_loyalty_bonus boolean  0.510 -0.189 -0.267        0.367
   bonus                    bonus_job_allowances_present boolean  0.077 -0.069 -0.179        0.043
   bonus                          bonus_thirteenth_month boolean  0.800 -0.131  0.064        0.661
```

_Skipped (need ≥2 numeric AND ≥2 booleans): overtime (9 numeric / 0 bool)._

**Reading.** When a numeric field and a boolean load on the SAME factor with the SAME sign, having that provision goes together with larger amounts (presence and generosity move together); OPPOSITE signs hint at a trade-off. Factors are mostly weak and topic-specific — consistent with §0/§4 (provisions are fairly independent adoption choices). FAMD is the honest way to combine the two data types; a plain Pearson FA on 0/1 + continuous would understate the boolean structure.

## 5 · Do generous CAOs also PAY more? (topic generosity vs wage)

**What we test.** For every topic, do the CAOs that are generous ON THAT TOPIC also pay higher WAGES? We correlate each topic's score with the CAO's wage score (`wage_median_z`), in THREE flavours of 'generous': the COMBINED score `<topic>_z` (good + many), COVERAGE only `<topic>_coverage_z` (many provisions), and NUMERIC only `<topic>_numeric_z` (large amounts). Pearson (linear) + Spearman (rank).

**How to interpret.** ≈0 ⇒ topic generosity and pay are independent — a CAO that is generous on leave/pension/etc. is NOT necessarily a high payer, so pay carries its own signal. Strongly positive ⇒ 'good employers are good on everything'. Negative would hint at a trade-off (pay vs perks).

**Correlation of each topic's generosity (combined / coverage / numeric) with wage**

```
     topic    n  combined_pay_pearson  combined_pay_spearman  coverage_pay_pearson  coverage_pay_spearman  numeric_pay_pearson  numeric_pay_spearman
     leave 2444                 0.332                  0.387                 0.082                  0.110                0.390                 0.447
      term 2444                 0.188                  0.200                 0.148                  0.172                0.138                 0.125
  contract 2444                 0.034                  0.049                 0.064                  0.130               -0.030                -0.088
  overtime 2444                -0.074                  0.023                -0.089                 -0.031                0.010                 0.036
  training 2444                -0.015                  0.058                -0.071                 -0.001                0.085                 0.150
     bonus 2444                 0.362                  0.344                 0.255                  0.262                0.398                 0.437
    fringe 2444                 0.343                  0.401                 0.350                  0.404                0.251                 0.272
homeoffice 2444                 0.320                  0.283                 0.336                  0.338                0.299                 0.322
   pension 2444                 0.124                  0.168                 0.148                  0.207               -0.112                -0.102
   absence 2444                 0.160                  0.246                 0.119                  0.183                0.184                 0.252
```

**Reading:** the correlations are mostly weak — topic generosity and wage are largely INDEPENDENT signals (a CAO generous on leave/pension is not automatically a high payer), which is why wage is kept as its own dimension. Where a column is blank the track doesn't exist (absence has no coverage). combined ≈ average of the coverage and numeric columns.

## 7 · Wage structure (salary-specific factor analysis)

**EFA of the WML-relative wage ladder (1970 cao×year cells; % var 49.7/27.3)**

```
                    f1    f2  communality
ratio_low_wml     0.98 -0.19         1.00
ratio_median_wml  0.91  0.37         0.95
ratio_mean_wml    0.88  0.47         1.00
ratio_high_wml    0.64  0.74         0.96
mw_span_pct      -0.06  0.81         0.66
ratio_entry_wml   0.06  0.18         0.04
```

**Reading:** two clean dimensions. **f1 = wage LEVEL** (low/median/mean/high ratios all load together — a CAO that pays above WML pays above it across the whole scale). **f2 = DISPERSION** (span and the p90/high ratio) — how stretched the scale is, independent of its level. Entry-scale ratio is nearly its own thing. So the salary data reduces to *how high* and *how spread* — both already shipped (`wage_median_z` = level, `wage_span_z` = dispersion).

## 8 · Methods considered but NOT run (with reasons)

| method | what it would add | verdict here |
|---|---|---|
| plain **PCA** | components = directions of max variance, no latent claim | redundant — with these low communalities PCA ≈ the EFA (§2); the per-topic PCA in §1 already covers the weighting question |
| **PCA with varimax rotation** | rotated components, FA-like reading | same as above — would duplicate §2/§1 without new information |
| **k-means clustering** ('types') | groups CAOs into types, not dimensions | ran it earlier: best k=2 but silhouette only 0.12 (soft, overlapping types) — not robust or actionable, consistent with the weak factor structure. Dropped. |
| **Pearson coverage FA** | factor the 0/1 coverage items | superseded by the **tetrachoric** FA (§4); Pearson attenuates binary associations and understates the bundles |
| **FAMD** (mixed numerics+booleans, `prince`) | one joint structure over levels and yes/no items | not installed; its question — do breadth and level share structure — is already answered by §3 (they separate) and §5 (~zero correlation). Available on request. |
| **full SEM** (paths between topics) | causal/mediation structure | no theory of inter-topic causation to test; CFA (§6) already shows no common factor to build on |

### One-line map: which method answers which question

| Your question | Use | Result |
|---|---|---|
| a transparent, normative generosity index | **signed equal-weight z** (shipped) | PRIMARY |
| are the equal weights distorting anything? | per-topic PCA/FA (§1) | no — equal ≈ PC1, and equal is more valid where they differ |
| is generosity one underlying trait? | EFA (§2) + factorability (§0) | no — ~independent topics |
| how do yes/no provisions cluster? | **tetrachoric FA** (§4) | a few co-adoption bundles |
| does breadth track pay level? | §3, §5 | no — separate; keep both roll-ups |
| what does the salary scale reduce to? | wage FA (§7) | level + dispersion |
| are there 'types' of CAO? | k-means (§8) | only soft types — not used |
## 9 · Robustness — the equal-range percentile scale (`gen01`)

**Why.** The published z averages fields whose z-ranges are asymmetric (a left-skewed field like reimbursement can only reach z≈+0.33 at its max but −3 at its min), so being best in a bunched field cannot offset being worst in a spread field. The percentile track (`<topic>_gen01`, `overall_gen01`) replaces each field's z with its ECDF rank (no winsor, no ±3 clip — rank is outlier-proof) and averages those, giving every field the identical [0,1] range. This section re-runs the same tests on that scale to confirm the *structure* is unchanged (so gen01 is a safe alternative scale, not a different model).

- **KMO = 0.55**, Bartlett χ² ≈ 1884 (df 55) on the 2670 docs scoring ≥8 topics — essentially identical factorability to the z matrix (§0), as expected: a monotone per-field transform barely moves the correlation matrix.

**Across-topic EFA (varimax, 3 factors, n=2670).** Variance explained: f1 8.4%, f2 6.9%, f3 5.3%.

**gen01 topic loadings**

```
                             f1    f2    f3  communality
leave_numeric_pctile       0.57  0.19 -0.11         0.37
absence_numeric_pctile     0.02  0.23  0.02         0.06
term_numeric_pctile        0.10 -0.13  0.36         0.16
contract_numeric_pctile    0.03 -0.08  0.24         0.06
overtime_numeric_pctile    0.00  0.05  0.24         0.06
training_numeric_pctile   -0.02  0.39 -0.15         0.17
bonus_numeric_pctile      -0.03  0.52  0.52         0.54
fringe_numeric_pctile      0.21 -0.05  0.03         0.05
homeoffice_numeric_pctile  0.45  0.02  0.10         0.21
pension_numeric_pctile    -0.05 -0.11  0.06         0.02
wage_median_pctile         0.58  0.46  0.13         0.56
```


**Does the scale change the ranking?** corr(overall_numeric_z, overall_numeric_pctile) = **0.810** (Pearson), **0.801** (Spearman rank). High — the two scales agree on who is generous — but **< 1**, and the gap is exactly the asymmetric-range fields the z under/over-weighted. Use **z** for factor analysis and σ-distances; use **gen01 / `*_pctile`** when you want a bounded, symmetric, communicable 0–1 generosity score where every field counts equally.

