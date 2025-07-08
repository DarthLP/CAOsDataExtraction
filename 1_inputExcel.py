import pandas as pd

# Load Excel
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
