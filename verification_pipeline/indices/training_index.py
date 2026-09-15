"""TRAINING — magnitude (paid training days/yr, STRATIFIED budget, % cost-reimbursement) + coverage.
Budget lives in 3 incompatible unit-bases (€ / %-of-salary / %-of-wage-sum) standardised WITHIN
each base -> ONE comparable budget_z / budget_prank (2026-07-08). cost_reimbursement is %-only;
its 183 € rows (misfiled budgets) go to the descriptive companion training_cost_reimbursement_eur.
career_scan_freq dropped (reciprocal 'every 5 yr' vs '1x/yr'). reclaim_clause excluded (anti-perk).
Run: python3.13 qa/indices/training_index.py"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import index_lib as il

FIELDS = [("training_time_yearly_value", "days_per_year", +1),
          ("training_cost_reimbursement_value", "percent", +1)]
# BUDGET is STRATIFIED (2026-07-08): its value lives in 3 incompatible unit-bases — € (703),
# %-of-salary individual budget (175), %-of-wage-sum collective sector-fund levy (297). We
# standardise WITHIN each base (own μ/σ + ECDF) so the ONE output budget_z / budget_prank is
# comparable across bases (a +1σ € doc == a +1σ %-salary doc). First-match order: €, then
# wage-sum levy, then any remaining % (individual). Folded into the training magnitude mean.
BUDGET_BASES = [("eur", r"eur|euro|€"),
                ("pct_wagesum", r"wage\s*sum|total\s*wage|loonsom|wage\s*bill"),
                ("pct_salary", r"%|percent|salary|income|wage")]
BOOLEANS = ["training_fund_present",
            # Tier-2 2026-07-05. CAVEAT: mandatory training free + as working time is LAW
            # since 1 Aug 2022 (Wet transparante en voorspelbare arbeidsvoorwaarden) —
            # post-2022 docs partly restate it (see statutory_all informational row).
            "training_mandatory_training_paid",
            # ADDED 2026-07-07 (fix a): the training RIGHT itself is now a counted provision.
            # Previously excluded as "near-constant" (98.6% True), which created a backwards
            # ranking: a rights=True-but-unquantified doc (magnitude NaN, no fund/mandatory)
            # scored BELOW a rights=False no-training doc, because the model literally could
            # not see the right anywhere. As a near-constant it adds ~zero variance (a fixed +1
            # to almost everyone, washed out by standardisation) so it barely dilutes coverage;
            # its entire real effect is to separate the 1.4% rights=False docs -> they now fall
            # to true coverage-bottom and rank below any doc that actually grants training.
            "training_has_training_rights"]
SHORT = {"training_time_yearly_value": "training_days_yr",
         "training_cost_reimbursement_value": "cost_reimb_pct"}
CLAMP = {"training_time_yearly_value": (0, 60), "training_cost_reimbursement_value": (0, 100)}
# cost_reimbursement is scored %-ONLY (to_percent drops €); the 183 € rows are misfiled budget
# amounts -> captured in a descriptive companion (not scored — € reimbursement isn't comparable
# to % reimbursement without the total cost).
DESC = [("training_cost_reimbursement_eur", "training_cost_reimbursement_value",
         lambda v, u: v if (v is not None and il._EUR.search((u or "").lower())) else None)]

if __name__ == "__main__":
    # presence-gated zero-fill (2026-07-07): training_has_training_rights is the genuine-
    # absence gate. It is 98.4% True (2656/2698) and only 1.6% False (42/2698); those 42
    # rights=False docs state ZERO training numbers (verified, no contradiction), so a blank
    # amount there = no training at all = 0. The 237 rights=True-but-unquantified docs keep
    # their blank (present-but-unquantified, stay available-case). The near-constant right is
    # useless in coverage (barely varies) but is a clean, unambiguous gate on the FALSE side —
    # gating on rights=False is safe regardless of skew. Applies to all three +1 amount fields
    # because the right is generic (covers time, budget AND reimbursement). Corrects the earlier
    # "no zero-fill" call, which wrongly conflated a naive fill-all-blanks (would have hit the
    # 237 unquantified) with this presence-gated fill (only the 42 genuine absences).
    il.build_simple_index("training", FIELDS, BOOLEANS, SHORT, CLAMP, DESC,
                          os.path.join(il.OUT, "training_index.csv"),
                          os.path.join(il.OUT, "training_index_diagnostics.csv"),
                          zerofill=[("training_time_yearly_value", "training_has_training_rights"),
                                    ("training_cost_reimbursement_value", "training_has_training_rights")],
                          # ONE unified budget score across 3 unit-bases, gated for zero-fill like
                          # the other +1 training amounts (rights=False & never stated -> floor)
                          stratified=[("training_budget_value", +1, BUDGET_BASES, "budget",
                                       "training_has_training_rights")],
                          passthrough=["training_budget_value", "training_budget_unit"])
