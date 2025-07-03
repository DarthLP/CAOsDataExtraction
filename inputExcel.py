import pandas as pd
import yaml

# Load Excel
excel_path = "inputExcel/250702 AI information matrix collapsed.xlsx"
df = pd.read_excel(excel_path, header=None)

# First row = column names, second row = descriptions
df.columns = df.iloc[0]
descriptions = df.iloc[1]
df = df[2:].reset_index(drop=True)

# Write extended YAML schema
with open("fields.yaml", "w") as f:
    yaml.dump(
        [{'name': str(col), 'description': str(descriptions[col])} for col in df.columns],
        f, sort_keys=False, allow_unicode=True
    )

print("Detailed fields.yaml written successfully.")

