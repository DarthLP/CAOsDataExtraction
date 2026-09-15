"""Cross-check: did Layer 2 (judge) catch the Layer 1 (rules) issues on the same pilot records?"""
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

viols = pd.read_csv(ROOT / "outputs" / "leave_rule_violations.csv", sep=";", dtype=str)
verdicts = pd.read_csv(ROOT / "outputs" / "pilot" / "pilot_verdicts.csv", sep=";", dtype=str)

# Pilot records
pilot_ids = sorted(verdicts["record_id"].unique().tolist())
print(f"Pilot record_ids: {pilot_ids}\n")

# Layer 1 violations on pilot records only
viols_pilot = viols[viols["record_id"].isin(pilot_ids)].copy()
print(f"Layer 1 violations on pilot records: {len(viols_pilot)}\n")

if not viols_pilot.empty:
    print("=== Per-record Layer 1 violations ===")
    print(viols_pilot.groupby(["record_id", "rule_id", "severity"])
          .size().reset_index(name="count").to_string(index=False))
    print()

# For each Layer 1 violation, check whether Layer 2 flagged the same record + same topic group
# Map rule_id -> the topic_group(s) Layer 2 uses
RULE_TO_TOPIC = {
    "PAR_01_eligibility_required_for_tenure": "parental",
    "PAR_02_eligibility_required_for_contract_length": "parental",
    "PAR_03_statutory_ref_no_exceptions_eligibility_must_be_empty": "parental",
    "PAR_04_topup_pay_requires_topup_present": "parental",
    "CARE_01_statutory_ref_no_exceptions_details_must_be_empty": "care",
    "CARE_02_pay_requires_value": "care",
    "LIB_01_annual_and_lustrum_mutually_exclusive": "vacation_holidays",
    "MAT_01_partially_paid_pay_requires_partially_paid_value": "maternity",  # also paternity
    "MAT_02_paid_maternity_plausible_range": "maternity",
    "VAC_01_vacation_time_plausible": "vacation_holidays",
    "VAC_02_vacation_bonus_plausible": "vacation_holidays",
    "SICK_01_sickpay_duration_plausible": "sick",
    "SICK_02_continuation_pct_plausible": "sick",
    "SICK_03_topup_present_consistent_with_pay": "sick",
    "STAT_01_above_statutory_maternity_requires_value": "maternity",
    "STAT_02_paternity_above_statutory_requires_value": "paternity",
    "SEN_01_schedule_requires_present": "seniority_special",
    "UNIT_01_vacation_time_unit_in_allowlist": "vacation_holidays",
}

# For each Layer 1 violation, find the Layer 2 verdict for that record/topic
print("=== Did Layer 2 flag the same record/topic where Layer 1 fired? ===")
print()
matches = []
for _, v in viols_pilot.iterrows():
    rule = v["rule_id"]
    topic = RULE_TO_TOPIC.get(rule, v["field_group"])
    # Map field_group to verdict topic_group (most names align)
    topic_lookup = {"vacation_holidays": "vacation_holidays", "care": "care",
                    "parental": "parental", "sick": "sick",
                    "maternity": "maternity", "paternity": "paternity",
                    "seniority": "seniority_special"}
    topic = topic_lookup.get(topic, topic)
    verdict = verdicts[
        (verdicts["record_id"] == v["record_id"])
        & (verdicts["topic_group"] == topic)
    ]
    if verdict.empty:
        layer2_status = "NO_VERDICT_FOR_TOPIC"
    else:
        layer2_status = verdict.iloc[0]["status"]

    matches.append({
        "record_id": v["record_id"],
        "layer1_rule": rule,
        "layer1_severity": v["severity"],
        "layer1_msg": v["message"][:90],
        "topic_group": topic,
        "layer2_status": layer2_status,
        "concordant": layer2_status in ("contradicted", "partial"),
    })

mdf = pd.DataFrame(matches)
print(mdf.to_string(index=False, max_colwidth=90))
print()

# Summary
print("=== Concordance summary ===")
n_total = len(mdf)
n_concordant = mdf["concordant"].sum()
print(f"Layer 1 flags on pilot records:                  {n_total}")
print(f"  Layer 2 also flagged that topic (partial/contra): {n_concordant}")
print(f"  Layer 2 said 'supported' or 'not_applicable':     {n_total - n_concordant}")
print()

# Show divergent ones
print("=== Divergent (Layer 1 flagged, Layer 2 said supported/NA) ===")
div = mdf[~mdf["concordant"]]
if div.empty:
    print("  (none)")
else:
    print(div.to_string(index=False, max_colwidth=90))
