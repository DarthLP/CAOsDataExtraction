"""Per-field semantic recognition sets for L2 presence triggering and
field-anchored slicing.

============================================================================
CRITICAL — READ BEFORE EDITING
============================================================================
The per-topic source files at `inputs/by_topic/*_information.md` are
ENGLISH-TRANSLATED content. The upstream extractor translated the original
Dutch CAOs into English. As a result:

  1. Keyword lists MUST be ENGLISH-PRIMARY. The bulk of each entry is
     natural-English phrases the translator would actually produce.

  2. Retain Dutch terms ONLY when they survive translation, i.e. when they
     actually appear verbatim in the English-translated source:

       * Statutory acronyms: AOW, WAB, WWZ, BW, UWV, ATW, WAZO, WIEG, BHV,
         EHBO, PSA, FTE, RI&E, ORT, TOIL, AVG, ZW, WIA, WGA, RVU, VPL,
         EVC, OR
       * Pension fund names: ABP, BPF, PME, PMT, PFZW, PNO, bpfBOUW,
         "Stichting Bedrijfstakpensioenfonds"
       * Legal acts/concepts: "ketenregeling", "transitievergoeding",
         "kantonrechter", "Scheidsgerecht", "Wet flexibel werken", "Wfa",
         "Burgerlijk Wetboek", "Artikel X:Y BW"
       * Untranslatable concepts: "mantelzorg", "kraamverlof",
         "ouderschapsverlof", "zwangerschapsverlof", "eindejaarsuitkering",
         "13e maand", "dertiende maand", "vakantiegeld", "vakantietoeslag",
         "tijd voor tijd", "TVT", "compensatieuren", "thuiswerkvergoeding",
         "instaptrede", "trede", "ploegendienst", "ploegentoeslag",
         "onregelmatigheidstoeslag", "consignatie"
       * Formal scheme names: "O&O-fonds", "scholingsfonds",
         "studiekostenbeding", "terugbetalingsbeding",
         "Regeling Vervroegde Uittreding", "Witteveen", "Witteveenkader"

  3. DO NOT include Dutch multi-word sentence-fragments like "door
     werkgever in acht te nemen", "in geld uitbetaald", "naar keuze van
     de werknemer". These were translated to English and will NEVER
     match in source. They only inflate the keyword set without firing.

Each entry aims for 15-30 natural English phrases plus 3-8 retained Dutch
terms. Verify the language profile of `inputs/by_topic/<topic>_*.md`
before seeding a new topic — read 5 random JSON-array passages and confirm
they are mostly English. If a topic file is substantially more Dutch,
flag the upstream extraction.

Pattern (canonical example):
    ("overtime", "compensation_mode"): [
        # Monetary (English-primary)
        "paid out", "monetary compensation", "compensated in money",
        "paid in money", "hourly wage is paid",
        # TOIL (English-primary + retained acronym TOIL)
        "time off in lieu", "compensated by time off", "compensated in time",
        "time off", "TOIL",
        # Both / choice
        "by mutual agreement", "in consultation between",
        "at the choice of",
        # Retained Dutch (these specific phrases DO survive translation)
        "tijd voor tijd", "TVT", "compensatieuren",
    ],
============================================================================

Used by:
  - presence_scan.scan_topic_presence — L2 only fires when a field-specific
    phrase appears
  - source_text_loader.slice_for_item — third anchor pass with 1.5x rank boost

Layered matching:
  - Topic-level (`TOPIC_KEYWORDS`) gates the record (skip if topic absent)
  - Field-level (this file) filters which fields to flag within that record

HARD RULE: do not auto-edit this file. Human-merged convention.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Optional

from qa.shared import schema_lookup


_MANUAL_FIELD_KEYWORDS: dict[tuple[str, str], list[str]] = {
    # =========================================================================
    # OVERTIME (17 base fields)
    # =========================================================================
    ("overtime", "has_overtime_rules"): [
        "overtime is defined", "overtime is compensated", "overtime occurs",
        "overtime applies when", "overtime is performed", "overtime is considered",
        "overtime is regulated", "overtime arrangement", "overtime rules",
        "overtime provisions", "rules on overtime", "regulation of overtime",
        "extra work performed", "additional work", "work beyond",
        "performed at the employer's request", "ordered by the employer",
        "work outside agreed hours", "exceeds the agreed working hours",
        # retained Dutch
        "overwerk", "overuren", "overwerkregeling", "ATW",
    ],
    ("overtime", "trigger_daily"): [
        "daily threshold", "more than X hours per day", "exceeds the daily",
        "exceeded by", "daily working hours", "daily working time",
        "per day", "more than 8 hours per day", "more than 9 hours",
        "beyond the agreed daily", "above the normal daily working hours",
        "exceeds the normal daily", "daily working hours of",
        "outside the regular work schedule", "exceeds the schedule",
        "work performed beyond the normal daily",
        "incidentally exceeding", "exceeds the work schedule by",
    ],
    ("overtime", "trigger_weekly"): [
        "weekly threshold", "more than X hours per week", "per week",
        "exceeds the weekly", "exceeding the weekly", "weekly working hours",
        "more than 36 hours", "more than 38 hours", "more than 40 hours",
        "more than 42 hours", "above the normal weekly working hours",
        "exceeds the agreed weekly working hours", "weekly working time",
        "work schedule per week", "normal weekly working hours",
        "exceeds the agreed weekly",
    ],
    ("overtime", "compensation_mode"): [
        # Monetary
        "paid out", "monetary compensation", "compensated by money",
        "compensated in money", "paid in money", "paid as overtime",
        "wage will be paid", "hourly wage is paid",
        # TOIL / time off in lieu
        "time off in lieu", "compensated by time off", "compensated in time",
        "compensated by free time", "compensated with time off",
        "time-in-lieu", "compensated with an equal number of hours of free time",
        "saved up for half or full days off", "time off",
        # Both / choice
        "by mutual agreement", "in consultation between", "at the choice of",
        "in consultation with the employee",
        "can be compensated entirely or partially with time off in lieu",
        "in money or time", "can also be taken as time off",
        # retained Dutch / borrowed
        "TOIL", "tijd voor tijd", "tijd-voor-tijd", "TVT", "compensatieuren",
    ],
    ("overtime", "stacking_rule"): [
        "interact", "stack", "stacking of allowances", "cumulative",
        "highest applies", "only the highest", "highest only",
        "no stacking", "not cumulative", "combination of allowances",
        "are not cumulative", "stack with shift allowance",
        "no surcharge on top of", "no other surcharge is paid",
        "in addition to", "is paid in addition to",
        # retained
        "cumulatief", "samenloop",
    ],
    ("overtime", "selection_rule"): [
        "applies to the majority", "majority of employees",
        "applies to employees in", "applies to", "for the majority",
        "for full-time employees", "for permanent staff",
        "for all employees", "rule applies to",
        "selection of the typical group", "for the largest group",
        "base tier",
    ],
    ("overtime", "allowance"): [
        "surcharge", "premium", "allowance", "percentage of hourly",
        "% of the hourly rate", "% of the hourly wage",
        "% of the hourly salary", "of the hourly wage", "of hourly salary",
        "surcharge of X%", "X% surcharge", "X% on the basic hourly wage",
        "X% above the hourly wage", "increased by X%",
        "additional pay", "extra pay", "additional X%",
        "125%", "150%", "200%", "100%", "175%", "and a half",
        "time-and-a-half", "double time", "premium pay",
    ],
    ("overtime", "allowance_range"): [
        "ranges from", "between X% and Y%", "minimum X%", "maximum X%",
        "varies between", "varies from", "different rates",
        "tiered surcharge", "tier schedule", "multi-tier",
    ],
    ("overtime", "hetero_present"): [
        "different rates", "different surcharges", "varies by group",
        "differs for", "for drivers", "for technical personnel",
        "for non-driving personnel", "for full-time", "for part-time",
        "for shift workers", "for office staff",
        "depending on the group", "applies differently to",
        "different for different groups", "different overtime rates",
    ],
    ("overtime", "shift_allowance"): [
        "shift allowance", "shift premium", "rotating shift",
        "2-shift", "3-shift", "4-shift", "5-shift",
        "shift schedule", "shift work", "two-shift work", "three-shift work",
        "shift bonus", "shift differential",
        "morning shift", "afternoon shift", "evening shift", "night shift",
        # retained Dutch (shift work uses preserved terminology)
        "ploegendienst", "ploegentoeslag", "wisseldienst",
    ],
    ("overtime", "unfavourable_hours_allowance"): [
        "unfavourable hours", "irregular hours", "irregular working hours",
        "unfavourable working hours", "anti-social hours",
        "night allowance", "night-shift allowance", "weekend allowance",
        "Saturday allowance", "Sunday allowance", "holiday allowance",
        "evening allowance", "early-shift allowance", "late-shift allowance",
        "on-call allowance", "standby allowance", "callout allowance",
        "irregularity allowance", "atypical hours",
        "surcharge for night work", "surcharge for weekend",
        "surcharge on Saturday", "surcharge on Sunday",
        "surcharge on public holidays",
        # retained Dutch (frequently kept in translation)
        "ORT", "onregelmatigheidstoeslag", "consignatie",
    ],
    ("overtime", "min_rest_between_shifts"): [
        "minimum rest", "minimum rest period", "uninterrupted rest",
        "rest between shifts", "daily rest period", "minimum daily rest",
        "rest period of X hours", "11 hours of rest",
        "uninterrupted rest period", "rest after a night shift",
        "minimum continuous rest", "rest between two shifts",
        "minimum rest between",
    ],
    ("overtime", "max_hours_per_day"): [
        "maximum daily", "max hours per day", "daily maximum",
        "maximum daily working hours", "maximum daily working time",
        "daily working time limit", "daily ceiling",
        "no more than X hours per day", "at most X hours per day",
        "maximum length of a working day", "maximum length of a shift",
    ],
    ("overtime", "max_hours_per_week"): [
        "maximum weekly", "max hours per week", "weekly maximum",
        "maximum weekly working hours", "maximum weekly working time",
        "weekly working time limit", "weekly ceiling",
        "no more than X hours per week", "at most X hours per week",
        "average of X hours per week", "weekly working hours not exceeding",
    ],
    ("overtime", "compulsory_annual"): [
        "compulsory annual", "annual maximum", "annual cap",
        "compulsory overtime", "required overtime per year",
        "maximum overtime per year", "annual overtime limit",
        "X hours of overtime per year", "X hours per calendar year",
        "obliged to perform overtime", "must perform overtime",
        "obligation to perform overtime", "compulsory hours",
        "limit on compulsory overtime", "maximum compulsory",
    ],
    ("overtime", "guaranteed_weekends_off_rule_text"): [
        "weekends off", "guaranteed weekends", "free weekend",
        "X weekends off per year", "Sundays off",
        "free Sundays per year", "minimum number of weekends off",
        # age-exemption phrasings
        "aged 55 and older", "55 years and older", "aged 50 and older",
        "50 years or older", "55 or older", "50 or older",
        "exempt from compulsory", "not obliged to perform overtime",
        "cannot be compelled", "cannot be required to",
        "no obligation to perform overtime", "voluntary for employees aged",
        # retained
        "AOW-leeftijd", "AOW age",
    ],

    # =========================================================================
    # HOMEOFFICE (10 base fields)
    # =========================================================================
    ("homeoffice", "has_homeoffice_rights"): [
        "home-working", "home office", "remote work", "remote working",
        "telework", "teleworking", "work from home", "WFH",
        "hybrid working", "hybrid work", "flexible workplace",
        "off-site work", "working from a remote location",
        "right to work from home", "working remotely",
        "work outside the office", "work outside the employer's premises",
        # retained Dutch (occasionally preserved)
        "thuiswerken", "telewerken",
    ],
    ("homeoffice", "entitlement"): [
        "right to home-working", "right to telework", "entitled to WFH",
        "entitled to work from home", "may request to work from home",
        "request to work remotely", "right to request remote work",
        "employees can work from home", "employees may work from home",
        "eligible for home-working",
    ],
    ("homeoffice", "discretion"): [
        "employer discretion", "subject to employer approval",
        "subject to approval", "with employer's consent",
        "at the discretion of", "in consultation with the employer",
        "approval of the employer required", "employer may grant",
        "employer can permit",
    ],
    ("homeoffice", "agreement_required"): [
        "agreement required", "written agreement", "written arrangement",
        "home-working agreement", "remote work agreement",
        "must be agreed in writing", "subject to a written agreement",
        "must be formalised in writing",
    ],
    ("homeoffice", "costs_reimbursed"): [
        "costs reimbursed", "expense reimbursement", "expenses reimbursed",
        "reimbursement of costs", "cost compensation",
        "internet costs reimbursed", "electricity costs reimbursed",
        "office furniture reimbursed", "energy cost allowance",
        "compensation for costs",
    ],
    ("homeoffice", "stipend"): [
        "home-working allowance", "WFH allowance", "stipend",
        "telework allowance", "remote work allowance",
        "fixed allowance for home-working", "daily allowance for WFH",
        "EUR per day of home-working", "tax-free allowance",
        "untaxed allowance", "fixed daily stipend",
        # retained Dutch (acronym/agency)
        "Belastingdienst", "thuiswerkvergoeding",
    ],
    ("homeoffice", "stipend_present"): [
        "home-working allowance", "WFH allowance", "stipend",
        "telework allowance", "remote work allowance",
        "fixed allowance for home-working", "daily allowance",
        "fixed daily stipend", "tax-free home-working allowance",
        "thuiswerkvergoeding",
    ],
    ("homeoffice", "health_safety_guarantee"): [
        "ergonomic workplace", "occupational health guarantee",
        "safe workplace at home", "healthy workplace",
        "workplace inspection", "RI&E for home-working",
        "ergonomic chair", "ergonomic setup",
        "compliant home workplace", "arbo-compliant home workplace",
        # retained
        "RI&E", "arbo",
    ],
    ("homeoffice", "travel_time_compensation"): [
        "travel time compensation", "commute time paid",
        "travel time counts as working time", "travel hours paid",
        "compensation for travel time", "travel time is regarded as work time",
    ],

    # =========================================================================
    # PENSION (17 base fields)
    # =========================================================================
    ("pension", "has_pension_scheme"): [
        "pension scheme", "pension fund", "pension plan",
        "occupational pension", "pension provider",
        "participation in the pension scheme",
        "mandatory pension scheme", "industry pension fund",
        "company pension fund", "sector pension fund",
        "pension agreement", "pension regulations",
        # retained Dutch — fund names and acronyms appear verbatim
        "ABP", "BPF", "PME", "PMT", "PFZW", "PNO",
        "Stichting Bedrijfstakpensioenfonds",
        "Stichting Pensioenfonds", "bpfBOUW", "pensioenfonds",
    ],
    ("pension", "pension_type"): [
        "defined benefit", "defined contribution", "average wage scheme",
        "final wage scheme", "average-salary scheme",
        "DB scheme", "DC scheme",
        "based on average career earnings", "average-pay",
        "based on final salary",
        # retained
        "middelloon", "eindloon", "premieovereenkomst",
        "uitkeringsovereenkomst",
    ],
    ("pension", "mandatory_participation"): [
        "mandatory participation", "compulsory participation",
        "obligatory participation", "must participate",
        "all employees participate", "opt-out", "may opt out",
        "voluntary participation",
    ],
    ("pension", "selection_rule_pension"): [
        "applies to majority", "for all employees",
        "applies to the majority", "for the majority",
        "applies to most employees", "applies to all participating",
    ],
    ("pension", "retire_age_normal"): [
        "AOW age", "state pension age", "retirement age",
        "AOW-eligible age", "AOW-eligible date", "AOW date",
        "normal retirement age", "target pension age",
        "pension age of 68", "pension age of 67",
        "statutory pension age", "AOW-leeftijd",
    ],
    ("pension", "retirement_age_early"): [
        "early retirement", "early retirement scheme",
        "early retirement plan", "early exit",
        "retire before AOW", "retirement before",
        # retained Dutch (RVU/VPL are statutory schemes)
        "RVU", "Regeling Vervroegde Uittreding", "VPL",
        "vervroegd pensioen", "vroegpensioen",
    ],
    ("pension", "retire_age_deferred"): [
        "deferred retirement", "postponed retirement",
        "continue working after AOW", "work beyond AOW age",
        "delayed retirement",
    ],
    ("pension", "end_at_AOW_auto"): [
        "auto-terminates at AOW", "automatically ends at AOW",
        "employment ends automatically at AOW",
        "contract ends automatically when the employee reaches AOW",
        "ends by operation of law at AOW",
    ],
    ("pension", "accrual_rate"): [
        "accrual rate", "annual accrual", "accrual percentage",
        "pension accrual", "build-up rate",
        "accrual of X% per year", "accrues at X%",
        "annual pension build-up", "pension build-up",
        # retained
        "Witteveen", "Witteveenkader", "opbouwpercentage",
    ],
    ("pension", "accrual_illness_y2"): [
        "accrual during illness", "pension accrual during sick leave",
        "second year of illness", "premium-free continuation",
        "continued accrual when sick", "accrual continues during illness",
        # retained — Dutch illness/disability acronyms
        "WIA", "WGA", "ZW", "premievrije voortzetting",
    ],
    ("pension", "accrual_stat_leaves"): [
        "accrual during leave", "pension accrual during maternity leave",
        "pension accrual during parental leave",
        "accrual continues during leave",
        # retained
        "WAZO", "ouderschapsverlof", "zwangerschapsverlof",
    ],
    ("pension", "franchise"): [
        "franchise", "pension threshold",
        "salary above the franchise",
        "pensionable salary minus the franchise",
        "deductible amount", "exempt portion",
    ],
    ("pension", "premium_total_range"): [
        "total premium", "pension contribution", "total contribution rate",
        "premium amounts to", "premium of X%", "contribution of X%",
        "X% of pensionable salary",
        "X% of pension-bearing salary",
        # retained
        "premie",
    ],
    ("pension", "premium_eq_split"): [
        "equal split", "50/50 split", "split equally between",
        "equally divided between employer and employee",
        "50-50 contribution split",
    ],
    ("pension", "employee_contrib"): [
        "employee contribution", "employee share", "employee premium",
        "deducted from salary", "withheld from the employee",
        "X% paid by the employee", "employee pays X%",
        "employee portion of the premium",
        # retained
        "werknemerspremie", "werknemersbijdrage",
    ],
    ("pension", "employee_contrib_range"): [
        "employee contribution range", "between X% and Y% paid by employee",
        "varying employee contribution",
    ],
    ("pension", "excedent_present"): [
        "excess pension scheme", "supplementary pension",
        "above the maximum pensionable salary",
        "excess scheme",
        # retained
        "excedentregeling",
    ],
    ("pension", "hetero_pension"): [
        "different schemes for different groups",
        "different pension scheme for", "varies by employee group",
        "separate scheme for", "different rates apply",
    ],

    # =========================================================================
    # TERM (21 base fields)
    # =========================================================================
    ("term", "has_termination_rules"): [
        "termination rules", "termination of employment", "notice rules",
        "end of employment", "termination of the employment contract",
        "dismissal", "ending the employment", "end of the contract",
        "termination provisions",
    ],
    ("term", "hetero_present"): [
        "different rules for groups", "different notice periods",
        "differs by group", "varies by group", "different rules apply for",
    ],
    ("term", "selection_rule_notice"): [
        "applies to majority", "for the majority of employees",
        "applies to all employees",
    ],
    ("term", "employer_notice"): [
        "employer notice period", "notice period for the employer",
        "notice period required from the employer",
        "employer must give X months notice",
        "termination by the employer",
        "notice when terminated by the employer",
        "must be given by the employer",
        "notice the employer must observe",
        # retained
        "opzegtermijn werkgever",
    ],
    ("term", "employer_notice_range"): [
        "employer notice varies", "depending on tenure",
        "based on years of service", "tiered notice period",
        "increases with tenure", "longer notice for longer-serving",
    ],
    ("term", "employee_notice"): [
        "employee notice period", "notice period for the employee",
        "notice the employee must observe",
        "employee must give X months notice",
        "termination by the employee",
        "if the employee terminates", "notice required from the employee",
    ],
    ("term", "employee_notice_range"): [
        "employee notice varies",
    ],
    ("term", "notice_min_floor"): [
        "statutory minimum", "minimum notice", "at least X months",
        "no less than", "in any case",
        "statutory notice period",
        # retained
        "BW", "Burgerlijk Wetboek",
    ],
    ("term", "notice_tenure_present"): [
        "tenure-based notice", "based on years of service",
        "depending on years of employment",
        "longer notice for longer-serving employees",
        "notice period increases with tenure",
    ],
    ("term", "notice_tenure_rule"): [
        "per year of service", "X weeks per year of service",
        "for every year of employment",
        "notice formula", "scaled by tenure",
    ],
    ("term", "shorten_notice_uwv"): [
        "UWV dismissal permit", "deducted UWV processing time",
        "shortened by UWV",
        # retained
        "UWV", "ontslagvergunning",
    ],
    ("term", "probation_allowed"): [
        "probation", "probation period", "probationary period",
        "trial period", "during probation",
        # retained
        "proeftijd", "proeftijdbeding",
    ],
    ("term", "probation_indef"): [
        "probation for indefinite contract",
        "probation in permanent contract",
        "2 months probation",
    ],
    ("term", "probation_fixedterm"): [
        "probation for fixed-term contract",
        "probation in temporary contract",
        "1 month probation",
    ],
    ("term", "dismissal_approval"): [
        "dismissal approval", "grounds for termination",
        "dismissal permit", "dismissal route",
        "termination by mutual consent",
        "dissolved by the court", "court-ordered dissolution",
        "urgent reason", "dismissal for urgent reason",
        "summary dismissal",
        # retained
        "UWV", "kantonrechter", "Scheidsgerecht",
        "Artikel 7:677 BW", "Artikel 7:669 BW",
    ],
    ("term", "sick_dismissal_prot"): [
        "sick dismissal protection",
        "may not be terminated during sickness",
        "no dismissal during illness", "protected during sickness",
        "dismissal ban during illness",
    ],
    ("term", "severance_extra"): [
        "severance", "transition allowance", "extra severance",
        "severance payment", "termination payment",
        "additional severance", "supplementary severance",
        "compensation upon dismissal",
        # retained
        "transitievergoeding", "ontslagvergoeding",
    ],
    ("term", "severance_extra_formula"): [
        "severance formula", "1/3 month salary per year of service",
        "calculation of severance",
        "transition allowance calculation",
        "severance amounts to X month salary per year",
        # retained
        "transitievergoeding formule",
    ],
    ("term", "severance_tenure_note"): [
        "based on tenure", "depending on years of service",
        "tiered severance",
    ],
    ("term", "severance_ww_supplement"): [
        "WW supplement", "WW top-up", "supplement to unemployment benefit",
        "supplementary unemployment benefit",
        # retained — unemployment insurance acronym
        "WW", "WW-aanvulling", "WW-suppletie",
    ],
    ("term", "end_at_AOW_auto"): [
        "automatically ends at AOW age",
        "employment ends automatically when the employee reaches AOW age",
        "auto-terminates at AOW", "AOW-eligible age",
    ],

    # =========================================================================
    # BONUS (16 base fields)
    # =========================================================================
    ("bonus", "has_bonus_schemes"): [
        "bonus scheme", "bonus arrangements", "bonus regulation",
        "structural bonus", "recurring bonus", "incentive scheme",
        "bonus or incentive", "any bonus", "bonus provisions",
    ],
    ("bonus", "thirteenth_month"): [
        "thirteenth month", "13th month", "year-end bonus",
        "end-of-year bonus", "annual bonus",
        "X% of annual salary paid at year-end",
        "extra month salary",
        # retained — common Dutch terms preserved
        "13e maand", "dertiende maand", "eindejaarsuitkering",
    ],
    ("bonus", "thirteenth_month_amt"): [
        "8.33%", "8,33%", "one twelfth of annual salary",
        "one month salary as bonus", "X% of monthly salary",
    ],
    ("bonus", "sign_on_bonus_present"): [
        "sign-on bonus", "signing bonus", "hiring bonus",
        "one-off payment upon hire", "welcome bonus",
        "bonus on commencement", "one-off lump sum upon hiring",
    ],
    ("bonus", "sign_on_bonus"): [
        "sign-on amount", "signing bonus of EUR X",
        "EUR X paid upon commencement",
    ],
    ("bonus", "fixed_annual_lump"): [
        "fixed annual lump sum", "annual lump sum",
        "fixed annual bonus", "annual one-off payment",
        "EUR X per year as bonus",
    ],
    ("bonus", "profit_sharing_present"): [
        "profit sharing", "profit-sharing scheme",
        "share in profits", "profit distribution",
        "based on company profit", "linked to company results",
        "share of the profit",
    ],
    ("bonus", "profit_sharing_note"): [
        "% of company profit", "% of profit", "profit-sharing formula",
        "calculation of profit share",
    ],
    ("bonus", "performance_bonus_present"): [
        "performance bonus", "target-based bonus",
        "performance-related pay", "result-dependent bonus",
        "bonus based on performance", "individual performance bonus",
        "KPI-based bonus",
    ],
    ("bonus", "job_allowances_present"): [
        "job allowance", "role allowance", "function allowance",
        "cashier allowance", "driver's licence allowance",
        "role-linked allowance",
    ],
    ("bonus", "job_allowances_note"): [
        "allowance for specific functions",
        "allowance for cashiers", "allowance for drivers",
    ],
    ("bonus", "qual_bonus_present"): [
        "qualification bonus", "certification bonus", "degree bonus",
        "diploma bonus", "bonus for obtaining a certificate",
        "monetary bonus for diplomas", "bonus upon completing training",
    ],
    ("bonus", "qualification_bonus_note"): [
        "for diploma", "for completing certification",
        "monthly bonus for certificate", "one-off bonus for certificate",
    ],
    ("bonus", "seniority_loyalty_bonus"): [
        "seniority bonus", "loyalty bonus", "service anniversary bonus",
        "long-service bonus", "service award",
        "25 years of service", "12.5 years of service",
        "anniversary bonus", "jubilee bonus",
        # retained
        "jubileumuitkering", "ancienniteitstoeslag",
    ],
    ("bonus", "retire_gratuity_present"): [
        "retirement gratuity", "leaving bonus", "exit bonus",
        "gratuity at retirement", "lump sum at retirement",
        "long-service exit payment",
    ],
    ("bonus", "retirement_gratuity_note"): [
        "1 month salary after 25 years", "X month salary at retirement",
        "EUR X gratuity at retirement",
    ],

    # =========================================================================
    # WAGE (6 base fields)
    # =========================================================================
    ("wage", "entry_step_exp_present"): [
        "entry step", "starting step", "starting salary scale",
        "starting trede", "higher starting trede",
        "experience-based entry step", "prior experience",
        "relevant experience", "previous experience",
        "starts at a higher step based on experience",
        "trede van aanvang",
        # retained
        "trede", "instaptrede",
    ],
    ("wage", "entry_step_exp_rule"): [
        "X years of experience", "with relevant experience starts at",
        "≥3 years relevant experience", "with prior experience",
        "manager discretion", "based on years of experience",
    ],
    ("wage", "pers_allow_max_scale"): [
        "personal allowance", "personal supplement",
        "personal pay supplement", "above the maximum of the scale",
        "above schaalmaximum", "personal salary supplement",
        "retained above scale maximum",
        # retained — common in CAOs verbatim
        "persoonlijke toeslag",
    ],
    ("wage", "pers_allow_rule"): [
        "% above maximum", "duration of allowance", "phase-out",
        "pensionability", "indexation of allowance",
        "phased out over X years",
    ],
    ("wage", "perf_step_var_present"): [
        "performance step", "merit step", "based on performance",
        "extra step for performance", "withholding a step",
        "extra trede for performance", "performance-based step",
    ],
    ("wage", "perf_step_var_rule"): [
        "max +2 steps", "withholding requires PIP",
        "withholding requires OR notification",
        "improvement plan required", "after excellent rating",
        # retained
        "OR", "Ondernemingsraad", "PIP",
    ],

    # =========================================================================
    # FRINGE (16 base fields)
    # =========================================================================
    ("fringe", "has_fringe_benefits"): [
        "fringe benefits", "secondary benefits", "additional benefits",
        "extralegal benefits", "non-wage benefits",
    ],
    ("fringe", "commuting_allowance_present"): [
        "commuting allowance", "travel allowance",
        "commuting reimbursement", "travel cost compensation",
        "home-work travel reimbursement",
    ],
    ("fringe", "commuting_allowance"): [
        "commuting allowance", "mileage", "EUR per km",
        "EUR per kilometer", "kilometre allowance",
        "public transport reimbursement", "OV-card",
        "company car", "lease car", "fuel allowance",
        "X cents per km", "X EUR per month for commuting",
    ],
    ("fringe", "bike_scheme_present"): [
        "bike scheme", "company bike", "bicycle scheme",
        "cycling scheme", "bike lease",
        "lease bicycle", "bicycle reimbursement",
    ],
    ("fringe", "meal_benefit_present"): [
        "meal benefit", "meal allowance", "lunch subsidy",
        "canteen", "company restaurant", "warm meal provided",
        "subsidised lunch",
    ],
    ("fringe", "meal_benefit_amt"): [
        "EUR per meal", "EUR per lunch", "X EUR meal allowance",
    ],
    ("fringe", "meal_benefit_type"): [
        "warm meal", "lunch", "breakfast", "hot meal",
    ],
    ("fringe", "relocation_allowance_present"): [
        "relocation allowance", "moving allowance",
        "moving expenses", "moving costs reimbursed",
        "relocation reimbursement",
    ],
    ("fringe", "relocation_allowance"): [
        "relocation costs", "EUR for relocation",
        "moving cost compensation",
    ],
    ("fringe", "internet_or_phone_reimbursement_present"): [
        "internet allowance", "phone allowance",
        "mobile phone reimbursement",
        "home internet reimbursement", "phone bill reimbursement",
    ],
    ("fringe", "insurance_or_savings_benefit_present"): [
        "savings scheme", "insurance benefit",
        "group insurance", "supplementary insurance",
        "savings plan", "employer-sponsored savings",
    ],
    ("fringe", "insurance_or_savings_benefit_note"): [
        "savings scheme with employer contribution",
        "% employer contribution to savings",
    ],
    ("fringe", "health_insurance_support_present"): [
        "health insurance support", "collective health insurance",
        "health insurance discount", "health insurance contribution",
        "company health insurance plan",
    ],
    ("fringe", "health_insurance_support_note"): [
        "X% discount on health insurance",
        "EUR X contribution to health insurance",
    ],
    ("fringe", "mandatory_certifications_paid"): [
        "certification paid", "exam costs reimbursed",
        "certification reimbursement",
        "mandatory training certification",
    ],
    ("fringe", "other_fringe_benefits_note"): [
        "other benefits", "company gym", "employee association",
        "staff discount", "discount scheme",
        "fitness benefit",
    ],

    # =========================================================================
    # SAFETY (14 base fields)
    # =========================================================================
    ("safety", "harassment_protocol_present"): [
        "harassment protocol", "anti-harassment", "anti-harassment policy",
        "bullying policy", "anti-bullying", "policy against unwanted behaviour",
        "policy against discrimination", "code of conduct",
        "complaints procedure for harassment",
        # retained
        "PSA",
    ],
    ("safety", "confidential_counsellor_present"): [
        "confidential counsellor", "confidential contact person",
        "trust counsellor", "ombuds officer",
        "internal confidential adviser",
    ],
    ("safety", "integrity_protocol_present"): [
        "integrity protocol", "integrity policy", "integrity code",
        "code of conduct", "ethics policy",
    ],
    ("safety", "reporting_channel_external"): [
        "whistleblower channel", "external reporting channel",
        "whistleblowing policy", "external whistleblower hotline",
        "report of wrongdoing",
        # retained
        "klokkenluider",
    ],
    ("safety", "preventive_medical_checkup_present"): [
        "preventive medical check-up", "periodic medical examination",
        "occupational health check", "periodic health check",
        # retained
        "PMO", "PAGO",
    ],
    ("safety", "psa_prevention_measures_present"): [
        "psychosocial workload prevention", "work stress prevention",
        "burnout prevention", "stress management measures",
        "PSA prevention", "mental health programme",
        "workload measures",
        # retained
        "PSA",
    ],
    ("safety", "psa_measures_note"): [
        "measures against work stress", "burnout programme",
        "workload reduction measures",
    ],
    ("safety", "rie_psa_required"): [
        "risk inventory", "risk inventory and evaluation",
        "risk assessment", "occupational risk assessment",
        # retained — RI&E is the formal name
        "RI&E", "preventiemedewerker", "Arbowet",
    ],
    ("safety", "safety_committee_present"): [
        "safety committee", "occupational safety committee",
        "health and safety committee", "VGW-committee",
    ],
    ("safety", "safety_training_present"): [
        "safety training", "first-aid training", "emergency response training",
        "fire safety training", "occupational safety training",
        # retained — first-aid and emergency-response acronyms
        "BHV", "EHBO",
    ],
    ("safety", "wellbeing_program_present"): [
        "wellbeing program", "wellbeing programme", "vitality programme",
        "employee wellbeing", "health and wellbeing initiative",
    ],
    ("safety", "workload_monitoring_present"): [
        "workload monitoring", "workload measurement",
        "monitoring of work pressure", "regular workload assessment",
    ],
    ("safety", "arbodienst_access_provided"): [
        "occupational health service access", "company doctor",
        "occupational physician", "occupational health service",
        # retained — Dutch occupational health service name
        "arbodienst", "bedrijfsarts",
    ],

    # =========================================================================
    # CHILDCARE (14 base fields)
    # =========================================================================
    ("childcare", "childcare_support_present"): [
        "childcare support", "child care support",
        "support for childcare", "childcare contribution",
        "daycare support", "after-school care support",
    ],
    ("childcare", "support"): [
        "childcare allowance", "childcare subsidy",
        "EUR per child per month", "subsidy per child",
        "EUR contribution per child",
        "parental contribution reduction",
    ],
    ("childcare", "support_cap"): [
        "maximum EUR per child", "up to a maximum of",
        "support cap", "monthly cap", "annual maximum support",
    ],
    ("childcare", "discount_present"): [
        "childcare discount", "discount on childcare costs",
        "X% discount", "discount for employees",
    ],
    ("childcare", "age_min"): [
        "minimum age", "from X years old", "from 0 years",
        "eligible from age",
    ],
    ("childcare", "age_max"): [
        "maximum age", "up to X years old", "until age X",
    ],
    ("childcare", "min_fte"): [
        "minimum FTE", "minimum employment fraction",
        "minimum number of hours", "minimum part-time factor",
    ],
    ("childcare", "min_tenure_months"): [
        "minimum tenure", "at least X months of service",
        "minimum employment of X months",
    ],
    ("childcare", "eligibility_note"): [
        "eligibility criteria", "qualifying conditions",
        "eligible employees",
    ],
    ("childcare", "priority_access"): [
        "priority access", "preferential access",
        "priority placement", "priority arrangement",
    ],
    ("childcare", "provider_scope"): [
        "registered childcare", "certified childcare provider",
        "approved providers", "providers registered in LRK",
        # retained
        "LRK",
    ],
    ("childcare", "public_coord"): [
        "municipal cooperation", "public childcare coordination",
        "with the municipality", "coordination with local government",
    ],
    ("childcare", "inhouse_present"): [
        "in-house childcare", "company daycare", "on-site childcare",
        "workplace childcare",
    ],
    ("childcare", "funding_sector_fund"): [
        "sector fund", "industry fund", "childcare fund",
    ],

    # =========================================================================
    # AI (5 base fields)
    # =========================================================================
    ("ai", "ai_policy_exists"): [
        "AI policy", "AI governance", "artificial intelligence policy",
        "policy on AI", "rules on AI use", "AI strategy",
        "algorithm policy", "policy on algorithms",
        "AI implementation policy",
    ],
    ("ai", "ai_policy_note"): [
        "purpose of AI policy", "framework for AI",
        "AI principles", "AI ethics",
    ],
    ("ai", "ai_governance_body_present"): [
        "AI committee", "AI governance body", "AI steering group",
        "AI oversight body", "AI ethics board",
        # retained — works council involvement in AI is common
        "OR", "works council", "Ondernemingsraad",
    ],
    ("ai", "ai_automated_decisions"): [
        "automated decision-making", "automated decisions",
        "algorithm", "algorithmic decision",
        "Article 22 GDPR", "Article 22 AVG",
        "decisions made by AI", "AI-based decisions",
        # retained
        "AVG",
    ],
    ("ai", "ai_training_rights_present"): [
        "AI training rights", "AI skills training",
        "training in artificial intelligence",
        "right to training on AI", "AI upskilling",
    ],

    # =========================================================================
    # CONTRACT (16 base fields)
    # =========================================================================
    ("contract", "has_contract_type_rules"): [
        "contract type", "employment contract types",
        "types of employment contracts", "contract provisions",
        "kinds of contracts",
    ],
    ("contract", "full_time_hours"): [
        "full-time hours", "full-time working week",
        "36 hours", "38 hours", "40 hours per week",
        "standard working week", "full-time working time",
        "full-time arbeidsduur",
    ],
    ("contract", "part_time_allowed"): [
        "part-time allowed", "part-time work permitted",
        "part-time possible", "part-time employment",
    ],
    ("contract", "part_time_range"): [
        "part-time factor", "minimum part-time hours",
        "between X and Y hours per week", "part-time range",
    ],
    ("contract", "minmax_hours_contract_allowed"): [
        "min-max contract", "minimum-maximum contract",
        "min-max hour contract", "min/max contract",
        # retained
        "minmax", "minmaxcontract",
    ],
    ("contract", "minmax_hours_range"): [
        "between X and Y hours", "min-max range",
        "minimum X and maximum Y hours",
    ],
    ("contract", "zero_hour_oncall_allowed"): [
        "zero-hour contract", "zero hours contract",
        "on-call contract", "on-call worker",
        "no fixed hours", "as-needed work",
        # retained — common Dutch labour terms
        "nulurencontract", "oproepcontract",
    ],
    ("contract", "ketenregeling_deviation_present"): [
        "deviation from the chain rule", "exemption from chain rule",
        "expanded chain rule", "extension of chain rule",
        # retained — formal Dutch legal name
        "ketenregeling", "ketenbepaling",
    ],
    ("contract", "ketenregeling_max_contracts"): [
        "maximum number of successive contracts",
        "X successive fixed-term contracts",
        "maximum chain contracts",
    ],
    ("contract", "ketenregeling_max_duration"): [
        "maximum chain duration", "within X months",
        "total chain period", "maximum X years of fixed-term",
    ],
    ("contract", "workhours_adjustment_right_present"): [
        "right to adjust working hours", "right to reduce hours",
        "request to change working hours", "request to modify hours",
        # retained — statutory act name
        "Wfa", "Wet flexibel werken", "Wet aanpassing arbeidsduur",
    ],
    ("contract", "workhours_adjustment_note"): [
        "conditions for adjusting hours",
        "rules on hours adjustment",
    ],
    ("contract", "workhours_adjustment_min_firm_size"): [
        "minimum firm size", "from X employees",
        "applies to companies with at least",
    ],
    ("contract", "workhours_adjustment_tenure_requirement"): [
        "minimum tenure for adjustment",
        "at least X year of service",
        "after one year of employment",
    ],
    ("contract", "conversion_rights_temp_to_perm_present"): [
        "conversion to permanent", "right to conversion",
        "right to permanent contract",
        "after X temporary contracts becomes permanent",
    ],
    ("contract", "conversion_rights_rule_text"): [
        "after X contracts converted to permanent",
        "automatic conversion to indefinite",
        "right to a permanent contract after",
    ],

    # =========================================================================
    # TRAINING (9 base fields)
    # =========================================================================
    ("training", "has_training_rights"): [
        "training rights", "right to training", "right to education",
        "training entitlement", "individual training rights",
        "study rights", "professional development entitlement",
    ],
    ("training", "budget"): [
        "training budget", "study budget", "development budget",
        "individual training budget",
        "EUR per year for training", "annual training budget",
        "personal development budget",
    ],
    ("training", "time_yearly"): [
        "yearly training time", "training hours per year",
        "study leave per year", "annual training hours",
        "X hours per year for training",
    ],
    ("training", "mandatory_training_paid"): [
        "mandatory training paid", "mandatory training in working time",
        "compulsory training paid by the employer",
        "mandatory courses paid", "required training paid",
    ],
    ("training", "cost_reimbursement"): [
        "training cost reimbursement", "study cost reimbursement",
        "study cost arrangement", "100% reimbursement",
        "training expenses reimbursed", "% reimbursed",
    ],
    ("training", "reclaim_clause_present"): [
        "reclaim clause", "repayment clause for training costs",
        "obligation to repay training costs",
        "study cost recovery", "training cost recovery",
        # retained — formal contract clause name
        "terugbetalingsbeding", "studiekostenbeding",
    ],
    ("training", "career_scan_freq"): [
        "career scan", "career coaching", "career guidance",
        "career advice", "career check",
        "career scan every X years",
        # retained — Dutch career-related acronyms
        "EVC", "loopbaanscan",
    ],
    ("training", "fund_present"): [
        "training fund", "sector training fund",
        "industry training fund",
        # retained — these fund acronyms appear verbatim
        "O&O-fonds", "scholingsfonds", "opleidingsfonds",
    ],
}


# Common stopwords to drop when auto-extracting from descriptions
_STOPWORDS = {
    "the", "a", "an", "is", "are", "of", "in", "on", "for", "to", "with",
    "or", "and", "if", "set", "true", "false", "only", "when", "where",
    "value", "unit", "amount", "str", "bool", "list", "amountrange",
    "e", "g", "eg", "i", "ii", "iii", "iv", "v",
    "de", "het", "een", "en", "of", "van", "in", "op", "is", "zijn",
    "moet", "moeten", "kan", "kunnen", "etc", "described",
    "has", "use", "may", "via", "any", "all", "not", "yes", "no",
    "between", "after", "before", "during", "above", "below",
    "typical", "explicit", "explicitly",
}

# Acronym blocklist: too-generic to be useful as a content anchor
_ACRONYM_BLOCKLIST = {
    "CAO", "PDF", "NUM", "MIN", "MAX", "URL", "API", "CSV", "JSON",
    "MD", "TXT",
}


@lru_cache(maxsize=200)
def get_field_keywords(topic: str, csv_field: str) -> list[str]:
    """Return the combined keyword set for a specific CSV field.

    Combines:
      1. Manual augmentation from _MANUAL_FIELD_KEYWORDS (PRIMARY — English-first
         natural phrases plus retained Dutch acronyms/legal terms)
      2. Capitalized acronyms auto-extracted from the schema description
      3. Field-name stem (e.g. 'allowance' from 'overtime_allowance_value')

    Returns deduplicated list, case-insensitive comparison expected.
    """
    base = schema_lookup._csv_field_to_base(topic, csv_field)
    if base is None:
        return []

    keywords: list[str] = []

    # 1. Manual augmentation (primary source)
    manual = _MANUAL_FIELD_KEYWORDS.get((topic, base), [])
    keywords.extend(manual)
    base_stripped = re.sub(r"_(?:value|unit|range_min|range_max|amt|present)$", "", base)
    if base_stripped != base:
        keywords.extend(_MANUAL_FIELD_KEYWORDS.get((topic, base_stripped), []))

    # 2. Auto-extract additional anchors from the schema description.
    # Conservative: code identifiers (`monetary_pay`) and generic English
    # words (`other`, `both`) never appear in source — skip. Capitalized
    # acronyms (TOIL, AOW, WAB, ORT) DO appear and are useful anchors.
    desc = schema_lookup.describe_field(topic, csv_field)
    if desc:
        for m in re.finditer(r"\b([A-Z]{3,})\b", desc):
            ac = m.group(1)
            if ac not in _ACRONYM_BLOCKLIST:
                keywords.append(ac)
        # Field-name stem tokens (only meaningful nouns, ≥4 chars, not stop)
        for tok in re.split(r"[_\W]+", base):
            tok = tok.strip()
            if tok and tok.lower() not in _STOPWORDS and len(tok) >= 4:
                keywords.append(tok)

    # Dedupe (case-insensitive, preserve order)
    seen = set()
    out = []
    for k in keywords:
        kl = k.lower()
        if kl and kl not in seen:
            seen.add(kl)
            out.append(k)
    return out


def has_field_mention(topic: str, csv_field: str, source_text: str) -> tuple[bool, list[str]]:
    """Returns (any_field_keyword_present, matched_terms).
    Case-insensitive substring match. Used by presence_scan for the L2 trigger.
    """
    kws = get_field_keywords(topic, csv_field)
    if not kws or not source_text:
        return (False, [])
    haystack = source_text.lower()
    matched = [k for k in kws if k.lower() in haystack]
    return (len(matched) > 0, matched)
