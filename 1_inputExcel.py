import pandas as pd

# =========================
# Load and Process Excel Files
# =========================

# === Load Excel ===
excel_path = "inputExcel/250702 AI information matrix.xlsx"
# Load all sheets
excel_sheets = pd.read_excel(excel_path, header=None, sheet_name=None)

# Process first sheet (default behavior)
df = list(excel_sheets.values())[0]
df.columns = df.iloc[0]
df = df[1:].reset_index(drop=True)

# Build markdown table as string for sheet 1
markdown = "| " + " | ".join([str(col).strip() for col in df.columns]) + " |\n"
markdown += "| " + " | ".join(["---"] * len(df.columns)) + " |\n"
for _, row in df.iterrows():
    line = "| " + " | ".join([str(row[col]).strip() if bool(pd.notna(row[col])) else "" for col in df.columns]) + " |"
    markdown += line + "\n"

with open("fields_prompt.md", "w", encoding="utf-8") as f:
    f.write(markdown)
print("Markdown-style prompt structure written to fields_prompt.md")

# === Process worksheet 2 and 3 if present ===
sheet_filenames = ["fields_prompt.md", "fields_prompt_salary.md", "fields_prompt_rest.md"]
for idx, sheet_name in enumerate(list(excel_sheets.keys())[:3]):
    df_sheet = excel_sheets[sheet_name]
    df_sheet.columns = df_sheet.iloc[0]
    df_sheet = df_sheet[1:].reset_index(drop=True)
    markdown_sheet = "| " + " | ".join([str(col).strip() for col in df_sheet.columns]) + " |\n"
    markdown_sheet += "| " + " | ".join(["---"] * len(df_sheet.columns)) + " |\n"
    for _, row in df_sheet.iterrows():
        line = "| " + " | ".join([str(row[col]).strip() if bool(pd.notna(row[col])) else "" for col in df_sheet.columns]) + " |"
        markdown_sheet += line + "\n"
    out_filename = sheet_filenames[idx]
    with open(out_filename, "w", encoding="utf-8") as f:
        f.write(markdown_sheet)
    print(f"Markdown-style prompt structure written to {out_filename}")

# === Load collapsed Excel ===
collapsed_excel_path = "inputExcel/250702 AI information matrix collapsed.xlsx"
df_collapsed = pd.read_excel(collapsed_excel_path, header=None)

# First row = column names, second row = only data row
df_collapsed.columns = df_collapsed.iloc[0]
df_collapsed = df_collapsed[1:2].reset_index(drop=True)

# Build markdown table as string for collapsed
markdown_collapsed = "| " + " | ".join([str(col).strip() for col in df_collapsed.columns]) + " |\n"
markdown_collapsed += "| " + " | ".join(["---"] * len(df_collapsed.columns)) + " |\n"

for _, row in df_collapsed.iterrows():
    line = "| " + " | ".join([str(row[col]).strip() if bool(pd.notna(row[col])) else "" for col in df_collapsed.columns]) + " |"
    markdown_collapsed += line + "\n"

# Save collapsed markdown to file
with open("fields_prompt_collapsed.md", "w", encoding="utf-8") as f:
    f.write(markdown_collapsed)

print("Markdown-style prompt structure written to fields_prompt_collapsed.md")
