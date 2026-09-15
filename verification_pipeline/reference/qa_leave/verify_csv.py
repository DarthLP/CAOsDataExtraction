import pandas as pd

csv_file = '/Users/lorenzpiazolo/Documents/Claude/Projects/Dutch Bargaining Agreements/qa_leave/outputs/subagent_worksheets/chunks/chunk_083_corrections.csv'

df = pd.read_csv(csv_file, sep=';', dtype=str)
print(f'Rows: {len(df)}')
print(f'Columns: {list(df.columns)}')
print(f'Shape: {df.shape}')
print(f'\nConfidence value counts:\n{df["confidence"].value_counts().to_dict()}')
print(f'\nFirst row:\n{df.iloc[0].to_dict()}')
