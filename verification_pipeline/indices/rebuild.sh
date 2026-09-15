#!/bin/zsh
set -e
set -o pipefail
cd "/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/indices"
echo "===== topic drivers ====="
for d in bonus pension term overtime training homeoffice contract safety childcare ai fringe absence parental_leave; do
  echo "--- $d ---"
  python3 ${d}_index.py 2>&1 | tail -3
done
echo "===== mw_indices ====="; python3 mw_indices.py 2>&1 | tail -4
echo "===== statutory_index ====="; python3 statutory_index.py 2>&1 | tail -4
echo "===== composite_index ====="; python3 composite_index.py 2>&1 | tail -5
echo "===== build_panel_monthly ====="; python3 build_panel_monthly.py 2>&1 | tail -6
echo "===== advanced_analysis ====="; python3 advanced_analysis.py 2>&1 | tail -8
echo "===== cao_level_export ====="; python3 build_cao_level_export.py 2>&1 | tail -2
echo "===== build_combined ====="; python3 build_combined.py 2>&1 | tail -4
echo "===== check_battery ====="; python3 check_battery.py 2>&1 | tail -12
echo "ALL DONE"
