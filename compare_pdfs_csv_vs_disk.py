import os
import pandas as pd
from pathlib import Path

# Paths
CSV1 = 'input_pdfs/extracted_cao_info.csv'
CSV2 = 'input_pdfs/main_links_log.csv'
PDF_ROOT = 'input_pdfs'
JSON_ROOT = 'output_json'

# Read CSVs and collect all referenced PDF names
pdfs_in_csv1 = set()
pdfs_in_csv2 = set()

# extracted_cao_info.csv
info_df = pd.read_csv(CSV1, sep=';')
if 'pdf_name' in info_df.columns:
    pdfs_in_csv1.update(info_df['pdf_name'].dropna().astype(str).str.strip())

# main_links_log.csv
log_df = pd.read_csv(CSV2, sep=';')
if 'pdf_name' in log_df.columns:
    pdfs_in_csv2.update(log_df['pdf_name'].dropna().astype(str).str.strip())

# Only show PDFs that are NOT mentioned in both CSVs
only_in_info = sorted(pdfs_in_csv1 - pdfs_in_csv2)
only_in_log = sorted(pdfs_in_csv2 - pdfs_in_csv1)
in_both = sorted(pdfs_in_csv1 & pdfs_in_csv2)

# Collect all PDFs on disk (recursively in subfolders, ignore .DS_Store and CSVs)
pdfs_on_disk = set()
for root, dirs, files in os.walk(PDF_ROOT):
    for file in files:
        if file.lower().endswith('.pdf'):
            pdfs_on_disk.add(file.strip())

# For each exclusive set, print which are missing on disk and which are present
print('PDFs only in extracted_cao_info.csv (NOT in main_links_log.csv):')
info_missing_on_disk = [pdf for pdf in only_in_info if pdf not in pdfs_on_disk]
info_present_on_disk = [pdf for pdf in only_in_info if pdf in pdfs_on_disk]
print('  Present on disk:')
for pdf in info_present_on_disk:
    print('    ', pdf)
print(f'  Total present: {len(info_present_on_disk)}')
print('  MISSING on disk:')
for pdf in info_missing_on_disk:
    print('    ', pdf)
print(f'  Total missing: {len(info_missing_on_disk)}')

print('\nPDFs only in main_links_log.csv (NOT in extracted_cao_info.csv):')
log_missing_on_disk = [pdf for pdf in only_in_log if pdf not in pdfs_on_disk]
log_present_on_disk = [pdf for pdf in only_in_log if pdf in pdfs_on_disk]
print('  Present on disk:')
for pdf in log_present_on_disk:
    print('    ', pdf)
print(f'  Total present: {len(log_present_on_disk)}')
print('  MISSING on disk:')
for pdf in log_missing_on_disk:
    print('    ', pdf)
print(f'  Total missing: {len(log_missing_on_disk)}')

# PDFs mentioned in BOTH CSVs but NOT on disk
in_both_missing_on_disk = [pdf for pdf in in_both if pdf not in pdfs_on_disk]
print('\nPDFs mentioned in BOTH CSVs but NOT found on disk:')
for pdf in in_both_missing_on_disk:
    print('  ', pdf)
print(f'Total: {len(in_both_missing_on_disk)}')

# PDFs on disk not mentioned in either CSV
pdfs_in_either_csv = pdfs_in_csv1 | pdfs_in_csv2
not_in_any_csv = sorted(pdfs_on_disk - pdfs_in_either_csv)
print('\nPDFs found on disk but NOT listed in either CSV:')
for pdf in not_in_any_csv:
    print('  ', pdf)
print(f'Total: {len(not_in_any_csv)}')

# ========== JSON COMPARISON ==========
# Convert all .pdf names from the CSVs to .json
def pdf_to_json_name(filename):
    filename = filename.strip()
    if filename.lower().endswith('.pdf'):
        return filename[:-4] + '.json'
    elif filename.lower().endswith('.json'):
        return filename
    else:
        return filename + '.json'

jsons_in_csv1 = set(pdf_to_json_name(f) for f in pdfs_in_csv1)
jsons_in_csv2 = set(pdf_to_json_name(f) for f in pdfs_in_csv2)

only_in_info_json = sorted(jsons_in_csv1 - jsons_in_csv2)
only_in_log_json = sorted(jsons_in_csv2 - jsons_in_csv1)
in_both_json = sorted(jsons_in_csv1 & jsons_in_csv2)

# Collect all JSONs on disk (recursively in subfolders)
jsons_on_disk = set()
for root, dirs, files in os.walk(JSON_ROOT):
    for file in files:
        if file.lower().endswith('.json'):
            jsons_on_disk.add(file.strip())

print('\n========== JSON COMPARISON =========')
print('JSONs only in extracted_cao_info.csv (NOT in main_links_log.csv):')
info_missing_on_disk_json = [f for f in only_in_info_json if f not in jsons_on_disk]
info_present_on_disk_json = [f for f in only_in_info_json if f in jsons_on_disk]
print('  Present on disk:')
for f in info_present_on_disk_json:
    print('    ', f)
print(f'  Total present: {len(info_present_on_disk_json)}')
print('  MISSING on disk:')
for f in info_missing_on_disk_json:
    print('    ', f)
print(f'  Total missing: {len(info_missing_on_disk_json)}')

print('\nJSONs only in main_links_log.csv (NOT in extracted_cao_info.csv):')
log_missing_on_disk_json = [f for f in only_in_log_json if f not in jsons_on_disk]
log_present_on_disk_json = [f for f in only_in_log_json if f in jsons_on_disk]
print('  Present on disk:')
for f in log_present_on_disk_json:
    print('    ', f)
print(f'  Total present: {len(log_present_on_disk_json)}')
print('  MISSING on disk:')
for f in log_missing_on_disk_json:
    print('    ', f)
print(f'  Total missing: {len(log_missing_on_disk_json)}')

# JSONs mentioned in BOTH CSVs but NOT on disk
in_both_missing_on_disk_json = [f for f in in_both_json if f not in jsons_on_disk]
print('\nJSONs mentioned in BOTH CSVs but NOT found on disk:')
for f in in_both_missing_on_disk_json:
    print('  ', f)
print(f'Total: {len(in_both_missing_on_disk_json)}')

# JSONs on disk not mentioned in either CSV
jsons_in_either_csv = jsons_in_csv1 | jsons_in_csv2
not_in_any_csv_json = sorted(jsons_on_disk - jsons_in_either_csv)
print('\nJSONs found on disk but NOT listed in either CSV:')
for f in not_in_any_csv_json:
    print('  ', f)
print(f'Total: {len(not_in_any_csv_json)}') 