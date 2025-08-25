import os
import pandas as pd
from pathlib import Path

# Paths
CSV_PATH = 'inputs/pdfs/input_pdfs/extracted_cao_info.csv'
PDF_ROOT = Path('inputs/pdfs/input_pdfs')
JSON_ROOTS = [Path('outputs/parsed_pdfs/parsed_pdfs_json'), Path('outputs/llm_extracted')]

# Load mapping
df = pd.read_csv(CSV_PATH, sep=';')

for idx, row in df.iterrows():
    cao_number = str(row['cao_number'])
    original_pdf_name = row['pdf_name']
    original_json_name = Path(original_pdf_name).with_suffix('.json').name

    # --- PDF ---
    pdf_folder = PDF_ROOT / cao_number
    if pdf_folder.exists():
        # Find any PDF in the folder
        for file in pdf_folder.glob('*.pdf'):
            if file.name != original_pdf_name:
                target = pdf_folder / original_pdf_name
                if not target.exists():
                    print(f"Renaming PDF: {file} -> {target}")
                    file.rename(target)
                else:
                    print(f"Target PDF already exists: {target}")

    # --- JSON (in all possible roots) ---
    for json_root in JSON_ROOTS:
        json_folder = json_root / cao_number
        if json_folder.exists():
            for file in json_folder.glob('*.json'):
                if file.name != original_json_name:
                    target = json_folder / original_json_name
                    if not target.exists():
                        print(f"Renaming JSON: {file} -> {target}")
                        file.rename(target)
                    else:
                        print(f"Target JSON already exists: {target}") 