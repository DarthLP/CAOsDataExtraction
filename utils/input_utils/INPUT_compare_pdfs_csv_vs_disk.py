import os
import pandas as pd
from pathlib import Path

# Paths
CSV1 = 'inputs/pdfs/input_pdfs/extracted_cao_info.csv'
CSV2 = 'inputs/pdfs/input_pdfs/main_links_log.csv'
PDF_ROOT = 'inputs/pdfs/input_pdfs'
JSON_ROOT = 'outputs/parsed_pdfs/parsed_pdfs_json'

# Read CSVs
info_df = pd.read_csv(CSV1, sep=';')
log_df = pd.read_csv(CSV2, sep=';')

# Filter out rows with empty or NaN pdf_name
info_df = info_df[info_df['pdf_name'].notna() & info_df['pdf_name'].astype(str).str.strip().ne('')]
log_df = log_df[log_df['pdf_name'].notna() & log_df['pdf_name'].astype(str).str.strip().ne('')]

# Group CSVs by cao_number
info_by_cao = info_df.groupby('cao_number')['pdf_name'].apply(list).to_dict()
log_by_cao = log_df.groupby('cao_number')['pdf_name'].apply(list).to_dict()

# Get all CAO folders in input_pdfs (ignore non-numeric folders)
cao_folders = [f for f in os.listdir(PDF_ROOT) if os.path.isdir(os.path.join(PDF_ROOT, f)) and f.isdigit()]
cao_folders = sorted(cao_folders, key=int)

print('==== MISSING PDFs (in CSVs but not in input_pdfs/CAO/) ====' )
for cao_number in cao_folders:
    folder_path = os.path.join(PDF_ROOT, cao_number)
    pdfs_in_folder = set(f for f in os.listdir(folder_path) if f.lower().endswith('.pdf'))
    # Check extracted_cao_info.csv
    missing_info = []
    for pdf_name in info_by_cao.get(int(cao_number), []):
        if pdf_name not in pdfs_in_folder:
            missing_info.append(pdf_name)
    # Check main_links_log.csv
    missing_log = []
    for pdf_name in log_by_cao.get(int(cao_number), []):
        if pdf_name not in pdfs_in_folder:
            missing_log.append(pdf_name)
    if missing_info or missing_log:
        print(f'CAO {cao_number}:')
        if missing_info:
            print('  Missing from extracted_cao_info.csv:')
            for pdf in missing_info:
                print(f'    {pdf} (CAO {cao_number})')
        if missing_log:
            print('  Missing from main_links_log.csv:')
            for pdf in missing_log:
                print(f'    {pdf} (CAO {cao_number})')

print('\n==== JSONs in outputs/parsed_pdfs/parsed_pdfs_json/CAO/ with missing PDF in input_pdfs/CAO/ ====' )
for cao_number in cao_folders:
    json_folder = os.path.join(JSON_ROOT, cao_number)
    pdf_folder = os.path.join(PDF_ROOT, cao_number)
    if not os.path.exists(json_folder):
        continue
    pdfs_in_folder = set(f for f in os.listdir(pdf_folder) if f.lower().endswith('.pdf'))
    jsons_in_folder = [f for f in os.listdir(json_folder) if f.lower().endswith('.json')]
    missing_jsons = []
    for json_name in jsons_in_folder:
        pdf_name = os.path.splitext(json_name)[0] + '.pdf'
        if pdf_name not in pdfs_in_folder:
            missing_jsons.append(json_name)
    if missing_jsons:
        print(f'CAO {cao_number}:')
        print('  JSONs with missing PDF:')
        for json_name in missing_jsons:
            print(f'    {json_name} (CAO {cao_number})')

# 1. PDFs in input_pdfs/CAO/ but not in either CSV
def get_csv_pdf_names(cao_number):
    return set(info_by_cao.get(int(cao_number), [])) | set(log_by_cao.get(int(cao_number), []))

print('\n==== PDFs in input_pdfs/CAO/ but NOT in either CSV ====' )
for cao_number in cao_folders:
    folder_path = os.path.join(PDF_ROOT, cao_number)
    pdfs_in_folder = set(f for f in os.listdir(folder_path) if f.lower().endswith('.pdf'))
    csv_pdf_names = get_csv_pdf_names(cao_number)
    missing_in_csv = [pdf for pdf in pdfs_in_folder if pdf not in csv_pdf_names]
    if missing_in_csv:
        print(f'CAO {cao_number}:')
        for pdf in missing_in_csv:
            print(f'  {pdf} (CAO {cao_number})')

# 2. JSONs in outputs/parsed_pdfs/parsed_pdfs_json/CAO/ with missing CSV entry
print('\n==== JSONs in outputs/parsed_pdfs/parsed_pdfs_json/CAO/ with missing CSV entry ====' )
for cao_number in cao_folders:
    json_folder = os.path.join(JSON_ROOT, cao_number)
    if not os.path.exists(json_folder):
        continue
    csv_pdf_names = get_csv_pdf_names(cao_number)
    jsons_in_folder = [f for f in os.listdir(json_folder) if f.lower().endswith('.json')]
    missing_csv = []
    for json_name in jsons_in_folder:
        pdf_name = os.path.splitext(json_name)[0] + '.pdf'
        if pdf_name not in csv_pdf_names:
            missing_csv.append(json_name)
    if missing_csv:
        print(f'CAO {cao_number}:')
        for json_name in missing_csv:
            print(f'  {json_name} (CAO {cao_number})')

# 3. CSVs with missing JSON
print('\n==== CSVs with missing JSON in outputs/parsed_pdfs/parsed_pdfs_json/CAO/ ====' )
for cao_number in cao_folders:
    json_folder = os.path.join(JSON_ROOT, cao_number)
    csv_pdf_names = get_csv_pdf_names(cao_number)
    jsons_in_folder = set(f for f in os.listdir(json_folder) if f.lower().endswith('.json')) if os.path.exists(json_folder) else set()
    missing_json = []
    for pdf_name in csv_pdf_names:
        json_name = os.path.splitext(pdf_name)[0] + '.json'
        if json_name not in jsons_in_folder:
            missing_json.append(pdf_name)
    if missing_json:
        print(f'CAO {cao_number}:')
        for pdf_name in missing_json:
            print(f'  {pdf_name} (CAO {cao_number})')

# CSV vs CSV comparison
print('\n==== CSV vs CSV comparison (per CAO) ====' )
for cao_number in cao_folders:
    info_set = set(info_by_cao.get(int(cao_number), []))
    log_set = set(log_by_cao.get(int(cao_number), []))
    only_in_info = sorted(info_set - log_set)
    only_in_log = sorted(log_set - info_set)
    if only_in_info or only_in_log:
        print(f'CAO {cao_number}:')
        if only_in_info:
            print('  In extracted_cao_info.csv but NOT in main_links_log.csv:')
            for pdf in only_in_info:
                print(f'    {pdf} (CAO {cao_number})')
        if only_in_log:
            print('  In main_links_log.csv but NOT in extracted_cao_info.csv:')
            for pdf in only_in_log:
                print(f'    {pdf} (CAO {cao_number})') 