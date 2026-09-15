# Audit checks (run by `qa/shared/audit_lib.run_audit`)

```
A1   Records missing record_id
A2   csv_value_new with leading/trailing whitespace
A3   high/medium confidence with placeholder evidence (starts with "(" or empty)
A4   csv_value_new=UNKNOWN with confidence != low (HARD RULE violation)
A5   Number value with unit_new=UNKNOWN
A6   Boolean field with non-boolean
A7   Pay-rate field with value > 100 (impossible %)
A8   Year-like values (1990-2030) in non-pay duration fields
A9   Pay-rate fields with duration unit
A10  Conflicting (record_id, field) pairs with different csv_value_new
A11  Topic-group / field-name mismatch
A12  Demoted rows where post-state is NOT (low + UNKNOWN)
A13  Deterministic-subagent disagreement rate per rule
     → if rule fired ≥10 times AND disagrees >20%, surface the rule.
       Indicates miscalibration — review before next topic.
       Below 10 firings: log rate but do not surface.
A14  High-confidence verdicts on truncated sources
     → any row where topic_section_was_truncated=true AND confidence ∈
       {high, medium} AND verdict ∈ {clear, correct_in_place, confirm}.
       Surfaces cases where the subagent was confident on a slice that
       had whole passages dropped — sanity-check before applying.
       Routes to needs_human_review automatically.
A15  Negative verdict on truncated source
     → any row where topic_section_was_truncated=true AND verdict ∈
       {clear, unable_to_verify} OR (verdict=set_boolean AND new_value=False)
       OR (csv_value_new=UNKNOWN with confidence=low).
       The "answer might be in a dropped passage" case. Routes to NHR
       with the dropped_passages_summary attached so the reviewer can
       quickly judge whether the missed content was actually relevant.
A16  Value present in CSV but not found in source (value-anchor miss)
     → any row where the slicer recorded value_not_in_source=true AND the
       subagent emitted verdict=confirm. Signals either a unit mismatch,
       a decimal-strip (FM_02), or that the source uses a paraphrase the
       value-variant generator missed. Routes to NHR.
A17  Evidence quote doesn't contain the proposed value
     → for verdict ∈ {correct_in_place, move}: the evidence_quote must
       literally contain the proposed csv_value_new (or its decimal-comma
       variant). Catches subagents that cite the source-block metadata
       header (`sgeving:`, `ingangsdatum:`) instead of the verbatim line
       supporting the value. Skipped for True/False (booleans) and
       UNKNOWN/empty values. Routes to NHR.
```
