"""Statutory-fingerprint restatement screen (time-aware).

Flags cells whose VALUE equals a Dutch statutory constant valid at the document's
date WHILE a companion boolean claims a CAO enhancement — the signature of the
campaign's largest phantom class (statutory restatement coded as an entitlement).
Purely diagnostic: writes indices/out/statutory_fingerprint_suspects.csv, never edits data.

Time-awareness: statutory values change. Each fingerprint carries a validity window
matched against the record's ingangsdatum (fallback datum_kennisgeving):
  - WIEG partner leave: 1 wk 100% from 2019-01-01; +5 wks at 70% from 2020-07-01
    (before 2019 the statutory 'kraamverlof' was 2 days).
  - Ketenregeling: 3 contracts / 36 months, EXCEPT 3/24 in the WWZ window
    [2015-07-01, 2020-01-01) (WAB restored 36).
  - Statutory paid parental leave 9 wks at 70% (UWV) only from 2022-08-02.
  - WAZO short-term care 70%, maternity 16 wks 100%, sick-pay floor 70% — stable.
Run: python3 statutory_fingerprints.py   (also called by check_battery.py, check 5)
"""
import os
import sys
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import index_lib as il

def _num(s):
    return pd.to_numeric(s, errors="coerce")

def _dates(df):
    d = pd.to_datetime(df["ingangsdatum"], errors="coerce", dayfirst=True)
    fb = pd.to_datetime(df["datum_kennisgeving"], errors="coerce", dayfirst=True)
    return d.fillna(fb)

def run(df=None):
    if df is None:
        df = pd.read_csv(os.path.join(BASE, "qa", "corrected_dataset.csv"),
                         sep=";", dtype=str, low_memory=False)
    dt = _dates(df)
    T = lambda col: df[col].astype(str).str.strip().str.lower().eq("true")
    always = pd.Series(True, index=df.index)
    wieg1 = dt >= "2019-01-01"
    wieg5 = dt >= "2020-07-01"
    uwv_parental = dt >= "2022-08-02"
    wwz_24 = (dt >= "2015-07-01") & (dt < "2020-01-01")
    keten_36 = ~wwz_24  # pre-WWZ and WAB-era

    # (label, value-match mask, companion enhancement-claim mask, window mask)
    checks = [
        ("care 70% restated as top-up",
         _num(df["leave_short_term_care_pay_value"]).eq(70),
         T("leave_care_topup_present"), always),
        ("sick-pay 70% floor restated as top-up",
         _num(df["leave_sickpay_continuation_value"]).eq(70),
         T("leave_sick_topup_present"), always),
        ("maternity 16 wks restated as above-statutory",
         _num(df["leave_paid_maternity_value"]).eq(16),
         T("leave_has_above_statutory_maternity"), always),
        ("WIEG 1-wk partner leave restated as above-statutory",
         _num(df["leave_paid_paternity_value"]).eq(1),
         T("leave_paternity_explicitly_above_statutory"), wieg1),
        ("WIEG 5-wk/70% partner leave restated as above-statutory",
         _num(df["leave_partially_paid_paternity_value"]).eq(5)
         & _num(df["leave_partially_paid_paternity_pay_value"]).eq(70),
         T("leave_paternity_explicitly_above_statutory"), wieg5),
        ("UWV 9-wk/70% parental leave restated as CAO top-up",
         _num(df["leave_parental_topup_pay_value"]).eq(70),
         T("leave_parental_topup_present"), uwv_parental),
        ("ketenregeling 3/36 restated as deviation",
         _num(df["contract_ketenregeling_max_contracts_value"]).eq(3)
         & _num(df["contract_ketenregeling_max_duration_value"]).eq(36),
         T("contract_ketenregeling_deviation_present"), keten_36),
        ("ketenregeling 3/24 (WWZ era) restated as deviation",
         _num(df["contract_ketenregeling_max_contracts_value"]).eq(3)
         & _num(df["contract_ketenregeling_max_duration_value"]).eq(24),
         T("contract_ketenregeling_deviation_present"), wwz_24),
        ("statutory chain rule coded as conversion right",
         _num(df["contract_ketenregeling_max_contracts_value"]).eq(3),
         T("contract_conversion_rights_temp_to_perm_present"), always),
    ]
    CLAIM_FIELD = {
        "care 70% restated as top-up": "leave_care_topup_present",
        "sick-pay 70% floor restated as top-up": "leave_sick_topup_present",
        "maternity 16 wks restated as above-statutory": "leave_has_above_statutory_maternity",
        "WIEG 1-wk partner leave restated as above-statutory": "leave_paternity_explicitly_above_statutory",
        "WIEG 5-wk/70% partner leave restated as above-statutory": "leave_paternity_explicitly_above_statutory",
        "UWV 9-wk/70% parental leave restated as CAO top-up": "leave_parental_topup_present",
        "ketenregeling 3/36 restated as deviation": "contract_ketenregeling_deviation_present",
        "ketenregeling 3/24 (WWZ era) restated as deviation": "contract_ketenregeling_deviation_present",
        "statutory chain rule coded as conversion right": "contract_conversion_rights_temp_to_perm_present",
    }
    rows = []
    for label, val_m, claim_m, win_m in checks:
        m = val_m.fillna(False) & claim_m & win_m.fillna(False)
        for i in df.index[m]:
            rows.append({"id": df.at[i, "id"], "cao_number": df.at[i, "cao_number"],
                         "date_used": str(dt.iloc[df.index.get_loc(i)].date()) if pd.notna(dt.iloc[df.index.get_loc(i)]) else "",
                         "fingerprint": label, "claim_field": CLAIM_FIELD[label]})
    out = pd.DataFrame(rows, columns=["id", "cao_number", "date_used", "fingerprint", "claim_field"])
    # annotate with the campaign adjudication ledger: a suspect whose claim boolean was
    # agent-adjudicated is settled; NEVER_CHECKED rows are the open review queue
    led_p = il.locate("jump_campaign_adjudications.csv")
    if os.path.exists(led_p) and len(out):
        led = pd.read_csv(led_p, sep=";", dtype=str)
        lk = {(r["record_id"], r["field"]): r["disposition"] for _, r in led.iterrows()}
        out["adjudication"] = [lk.get((str(r["id"]), r["claim_field"]), "NEVER_CHECKED")
                               for _, r in out.iterrows()]
    else:
        out["adjudication"] = "NEVER_CHECKED"
    path = os.path.join(il.OUT, "statutory_fingerprint_suspects.csv")
    out.to_csv(path, sep=";", index=False)
    n_by = out["fingerprint"].value_counts().to_dict() if len(out) else {}
    n_open = int((out["adjudication"] == "NEVER_CHECKED").sum()) if len(out) else 0
    return len(out), n_by, path, n_open

if __name__ == "__main__":
    n, by, path, n_open = run()
    print(f"statutory-fingerprint suspects: {n} ({n_open} never agent-checked) -> {path}")
    for k, v in by.items():
        print(f"    {v:4d}  {k}")
