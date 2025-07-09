import pandas as pd

# === Load Excel ===
excel_path = "inputExcel/250702 AI information matrix.xlsx"
df = pd.read_excel(excel_path, header=None)

# First row = column names, second row = descriptions
df.columns = df.iloc[0]
df = df[1:].reset_index(drop=True)

# Build markdown table as string
markdown = "| " + " | ".join([str(col).strip() for col in df.columns]) + " |\n"
markdown += "| " + " | ".join(["---"] * len(df.columns)) + " |\n"

for _, row in df.iterrows():
    line = "| " + " | ".join([str(row[col]).strip() if pd.notna(row[col]) else "" for col in df.columns]) + " |"
    markdown += line + "\n"

# Save to file outside the inputExcel folder
with open("fields_prompt.md", "w", encoding="utf-8") as f:
    f.write(markdown)

print("Markdown-style prompt structure written to fields_prompt.md")

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
    line = "| " + " | ".join([str(row[col]).strip() if pd.notna(row[col]) else "" for col in df_collapsed.columns]) + " |"
    markdown_collapsed += line + "\n"

# Save collapsed markdown to file
with open("fields_prompt_collapsed.md", "w", encoding="utf-8") as f:
    f.write(markdown_collapsed)

print("Markdown-style prompt structure written to fields_prompt_collapsed.md")
