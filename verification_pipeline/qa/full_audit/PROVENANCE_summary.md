# Provenance summary — raw extraction vs canonical (G6)

- Cells differing raw -> canonical: **9195** across 1455 records / 233 columns
- Change events logged across L1-L7: **9654** on 9334 distinct cells
- Diff cells WITHOUT a changelog entry (orphans): **0**
- Logged cells whose final value returned to the raw value (net-zero chains): **139**
- Chain-final vs canonical mismatches (should be ~0; >0 means a later layer isn't last in file order): **0**

Every change event carries: layer, old -> new, why, and the source quote where the applying
layer recorded one. Full per-cell chains: `PROVENANCE_all_layers.csv` (sorted record, field, layer).
Layer methodology: `docs/DATA_LINEAGE.md`. Reversal: apply each layer's changelog old_values in
reverse layer order, or restore the dated backups in `qa/_old/`.
