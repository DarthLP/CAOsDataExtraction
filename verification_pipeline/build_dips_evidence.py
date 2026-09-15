#!/usr/bin/env python3
import pandas as pd
import sys
import os

# Setup paths
sys.path.insert(0, '/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/indices')
sys.path.insert(0, '/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements')

import index_lib as il

# Topic-to-booleans mapping
TOPIC_BOOLEANS = {
    'childcare': [
        "childcare_childcare_support_present",
        "childcare_inhouse_present",
        "childcare_discount_present",
        "childcare_priority_access"
    ],
    'absence': [
        "leave_sick_topup_present",
        "leave_sickpay_extra_insurance_present",
        "leave_care_topup_present"
    ],
    'safety': [
        "safety_harassment_protocol_present",
        "safety_integrity_protocol_present",
        "safety_confidential_counsellor_present",
        "safety_reporting_channel_external",
        "safety_safety_training_present",
        "safety_safety_committee_present",
        "safety_rie_psa_required",
        "safety_psa_prevention_measures_present",
        "safety_arbodienst_access_provided",
        "safety_preventive_medical_checkup_present",
        "safety_workload_monitoring_present",
        "safety_wellbeing_program_present"
    ],
    'pension': [
        "pension_excedent_present",
        "pension_accrual_stat_leaves",
        "pension_accrual_illness_y2",
        "pension_premium_eq_split",
        "pension_mandatory_participation"
    ],
    'overtime': [],  # No booleans
    'bonus': [
        "bonus_profit_sharing_present",
        "bonus_performance_bonus_present",
        "bonus_qual_bonus_present",
        "bonus_retire_gratuity_present",
        "bonus_seniority_loyalty_bonus",
        "bonus_job_allowances_present",
        "bonus_sign_on_bonus_present",
        "bonus_thirteenth_month"
    ],
    'homeoffice': [
        "homeoffice_has_homeoffice_rights",
        "homeoffice_costs_reimbursed",
        "homeoffice_stipend_present"
    ]
}

# Driver-to-topic mapping
DRIVER_TO_TOPIC = {
    'childcare_coverage_z': 'childcare',
    'absence_z': 'absence',
    'safety_coverage_z': 'safety',
    'pension_z': 'pension',
    'overtime_z': 'overtime',
    'bonus_z': 'bonus',
    'homeoffice_z': 'homeoffice'
}

# Composite column mapping: topic -> list of columns to extract (checked for existence)
COMPOSITE_COLS_BASE = {
    'childcare': ['childcare_coverage', 'childcare_coverage_z', 'childcare_z'],
    'absence': ['absence_coverage', 'absence_numeric_z', 'absence_coverage_z', 'absence_z'],
    'safety': ['safety_coverage', 'safety_coverage_z', 'safety_z'],
    'pension': ['pension_coverage', 'pension_numeric_z', 'pension_coverage_z', 'pension_z'],
    'overtime': ['overtime_coverage', 'overtime_numeric_z', 'overtime_coverage_z', 'overtime_z'],
    'bonus': ['bonus_coverage', 'bonus_numeric_z', 'bonus_coverage_z', 'bonus_z'],
    'homeoffice': ['homeoffice_coverage', 'homeoffice_numeric_z', 'homeoffice_coverage_z', 'homeoffice_z']
}

# Load data
print("Loading corrected dataset...")
df_cao = il.load_full_cao().set_index('id')

print("Loading dips...")
df_dips = pd.read_csv(
    il.locate('v_dips_overall.csv'),
    sep=';'
)

print("Loading composite index...")
df_composite = pd.read_csv(
    il.locate('composite_index.csv'),
    sep=';',
    index_col='id'
)

# Build evidence table
evidence_rows = []
dip_summaries = {}

for idx, dip_row in df_dips.iterrows():
    cao_num = dip_row['cao']
    id_prev = str(dip_row['id_prev'])
    id_dip = str(dip_row['id_dip'])
    id_next = str(dip_row['id_next'])
    driver = dip_row['driver']

    # Map driver to topic
    topic = DRIVER_TO_TOPIC.get(driver)
    if not topic:
        print(f"Warning: Unknown driver {driver} for CAO {cao_num}")
        continue

    # Get booleans for this topic
    booleans = TOPIC_BOOLEANS.get(topic, [])

    # Track which booleans flipped down and recovered
    flipped_bools = []

    # Extract boolean values
    for bool_name in booleans:
        val_prev = df_cao.loc[id_prev, bool_name] if id_prev in df_cao.index else None
        val_dip = df_cao.loc[id_dip, bool_name] if id_dip in df_cao.index else None
        val_next = df_cao.loc[id_next, bool_name] if id_next in df_cao.index else None

        # Convert to string if not None
        val_prev_str = str(val_prev) if val_prev is not None else ''
        val_dip_str = str(val_dip) if val_dip is not None else ''
        val_next_str = str(val_next) if val_next is not None else ''

        # Compute flags
        flipped_down = (val_prev_str == 'True' and val_dip_str != 'True')
        recovered = (flipped_down and val_next_str == 'True')

        # Track if this boolean flipped down
        if flipped_down:
            flipped_bools.append(bool_name)

        # Emit row
        evidence_rows.append({
            'cao': cao_num,
            'id_prev': id_prev,
            'id_dip': id_dip,
            'id_next': id_next,
            'driver': driver,
            'boolean_name': bool_name,
            'value_prev': val_prev_str,
            'value_dip': val_dip_str,
            'value_next': val_next_str,
            'flipped_down': flipped_down,
            'recovered': recovered
        })

    # Add composite-level scores (one row per composite column that exists)
    for col_name in COMPOSITE_COLS_BASE[topic]:
        if col_name not in df_composite.columns:
            continue  # Skip if column doesn't exist

        val_prev = df_composite.loc[id_prev, col_name] if id_prev in df_composite.index else None
        val_dip = df_composite.loc[id_dip, col_name] if id_dip in df_composite.index else None
        val_next = df_composite.loc[id_next, col_name] if id_next in df_composite.index else None

        # Convert to string if not None
        val_prev_str = str(val_prev) if val_prev is not None and pd.notna(val_prev) else ''
        val_dip_str = str(val_dip) if val_dip is not None and pd.notna(val_dip) else ''
        val_next_str = str(val_next) if val_next is not None and pd.notna(val_next) else ''

        evidence_rows.append({
            'cao': cao_num,
            'id_prev': id_prev,
            'id_dip': id_dip,
            'id_next': id_next,
            'driver': driver,
            'boolean_name': col_name,
            'value_prev': val_prev_str,
            'value_dip': val_dip_str,
            'value_next': val_next_str,
            'flipped_down': False,  # Composite scores don't flip
            'recovered': False
        })

    # Track summary
    if flipped_bools:
        bool_list = ', '.join([b.replace('_present', '').replace('_', ' ') for b in flipped_bools])
        dip_summaries[cao_num] = f"cao {cao_num}: {bool_list}"
    else:
        dip_summaries[cao_num] = f"cao {cao_num}: no boolean flip found (driver moved for another reason)"

# Create dataframe
df_evidence = pd.DataFrame(evidence_rows)

# Sort by cao and boolean_name
df_evidence = df_evidence.sort_values(['cao', 'boolean_name']).reset_index(drop=True)

# Write output
output_path = os.path.join(il.REV, 'v_dips_evidence.csv')
df_evidence.to_csv(output_path, sep=';', index=False)

print(f"\n✓ Written {len(df_evidence)} rows to {output_path}")
print(f"\nDip Summaries:")
for cao_num in sorted(set(df_dips['cao'])):
    print(dip_summaries[cao_num])
